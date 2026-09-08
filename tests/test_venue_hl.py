"""Managed Hyperliquid limit-order lifecycle without network access."""
import asyncio
import os
import sys
import time
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.venue_hl import HLVenue, HLOrderUpdatesFeed  # noqa: E402


class FakeSigning:
    @staticmethod
    def order_request_to_order_wire(req, asset):
        return {"req": req, "asset": asset}

    @staticmethod
    def order_wires_to_order_action(wires):
        return {"type": "order", "orders": wires}

    @staticmethod
    def sign_l1_action(*_args):
        return {"r": "0x1", "s": "0x2", "v": 27}


def test_managed_limit_cancels_then_reports_partial_fill():
    async def go():
        venue = object.__new__(HLVenue)
        venue.account = object()
        venue.asset_id = 110000
        venue.coin = "io:SNDK"
        venue.name = "ENTROPY"
        venue.settle_timeout = 0.2
        venue._signing = FakeSigning()
        venue._signed_payload = lambda action: {"action": action}

        async def post(_payload):
            return ({"status": "ok", "response": {"data": {"statuses": [
                {"resting": {"oid": 7}}
            ]}}}, None, False)

        canceled = False

        async def status(_cloid):
            if canceled:
                return {"status": "canceled", "filled_base": 0.004,
                        "avg_px": None, "err": None, "unresolved": False}
            return {"status": "open", "filled_base": 0.004,
                    "avg_px": None, "err": None, "unresolved": False}

        async def cancel(_cloid):
            nonlocal canceled
            canceled = True
            return None

        venue._post_exchange = post
        venue._order_status = status
        venue._cancel_by_oid = cancel
        result = await venue.send_managed_limit(
            is_buy=True, qty=0.01, limit_px=100.0, ttl_sec=0.01,
            keep_open=lambda: True)
        assert result["status"] == "canceled"
        assert result["filled_base"] == 0.004
        assert not result["unresolved"]

    asyncio.run(go())


def test_order_updates_feed_resolves_final_ioc_by_cloid():
    async def go():
        feed = HLOrderUpdatesFeed(
            "ENTROPY", "ws://unused", "0x" + "1" * 40, "io:SNDK")
        fut = feed.watch("0xabc")
        feed._handle_message({
            "channel": "subscriptionResponse",
            "data": {"subscription": {
                "type": "orderUpdates", "user": "0x" + "1" * 40,
            }},
        })
        assert feed.ready.is_set()
        feed._handle_message({
            "channel": "orderUpdates",
            "data": [{
                "order": {
                    "coin": "io:SNDK", "cloid": "0xAbC", "oid": 7,
                    "origSz": "0.0123", "sz": "0.0123",
                },
                "status": "open",
            }],
        })
        assert not fut.done()
        feed._handle_message({
            "channel": "orderUpdates",
            "data": [{
                "order": {
                    "coin": "io:SNDK", "cloid": "0xAbC", "oid": 7,
                    "origSz": "0.0123", "sz": "0",
                },
                "status": "filled",
            }],
        })
        result = await fut
        assert result["status"] == "filled"
        assert result["filled_base"] == 0.0123
        assert result["confirm_source"] == "ws"

    asyncio.run(go())


def test_taker_uses_ws_fill_before_rest_detail():
    async def go():
        venue = object.__new__(HLVenue)
        venue.account = SimpleNamespace(
            nonces=SimpleNamespace(next=lambda: 1), wallet=object(),
            is_mainnet=True, query_address="0x" + "1" * 40)
        venue.asset_id = 110000
        venue.coin = "io:SNDK"
        venue.name = "ENTROPY"
        venue.settle_timeout = 0.5
        venue._signing = FakeSigning()
        venue._next_cloid = lambda: SimpleNamespace(to_raw=lambda: "0xabc")
        feed = HLOrderUpdatesFeed(
            "ENTROPY", "ws://unused", venue.account.query_address,
            venue.coin)
        venue.order_feed = feed

        async def post(_payload):
            await asyncio.sleep(0.15)
            return ({"status": "ok", "response": {"data": {"statuses": [
                {"filled": {"totalSz": "0.0123", "avgPx": "100.25"}}
            ]}}}, None, False)

        venue._post_exchange = post
        loop = asyncio.get_running_loop()
        loop.call_later(0.01, feed._resolve, "0xabc", {
            "status": "filled", "filled_base": 0.0123, "avg_px": None,
            "err": None, "unresolved": False, "confirm_source": "ws",
        })
        started = time.perf_counter()
        result = await venue.send_taker(
            is_buy=True, qty=0.0123, limit_px=101.0)
        assert time.perf_counter() - started < 0.10
        assert result["filled_base"] == 0.0123
        assert result["avg_px"] is None
        assert "_rest_detail_task" in result
        result = await venue.finalize_order_info(result)
        assert result["avg_px"] == 100.25
        assert "_rest_detail_task" not in result

    asyncio.run(go())


def test_finalize_can_drop_slow_rest_detail_without_waiting():
    async def go():
        venue = object.__new__(HLVenue)
        venue.name = "ENTROPY"
        venue.settle_timeout = 1.0

        async def slow_detail():
            await asyncio.sleep(1.0)
            return None, None, True

        task = asyncio.create_task(slow_detail())
        info = {
            "status": "filled", "filled_base": 0.0123, "avg_px": None,
            "err": None, "unresolved": False, "confirm_source": "ws",
            "_rest_detail_task": task,
        }
        started = time.perf_counter()
        result = await venue.finalize_order_info(info, wait=False)
        assert time.perf_counter() - started < 0.10
        assert task.cancelled()
        assert "_rest_detail_task" not in result

    asyncio.run(go())
