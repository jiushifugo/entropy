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


class FakeWs:
    def __init__(self):
        self.sent = []

    async def send(self, payload):
        import json
        self.sent.append(json.loads(payload))


def partial_submit_venue(final_status, final_qty, missing_oid=False, status_before_cancel=None):
    venue = object.__new__(HLVenue)
    venue.account, venue.asset_id = object(), 110000
    venue.coin, venue.name = 'io:SNDK', 'ENTROPY'
    venue.settle_timeout = .001
    venue._signing = FakeSigning()
    venue._signed_payload = lambda action: {'action': action}
    venue.order_feed = None
    calls = []
    async def post(payload):
        row = {'totalSz': '.0005', 'avgPx': '1373.5'}
        if not missing_oid:
            row['oid'] = 77
        return ({'status': 'ok', 'response': {'data': {'statuses': [{'filled': row}]}}}, None, False)
    async def status(oid):
        calls.append(('status', oid))
        if not any(c[0] == 'cancel' for c in calls):
            return status_before_cancel
        return {'status': final_status, 'filled_base': final_qty,
                'avg_px': None, 'err': None, 'unresolved': False}
    async def cancel(oid):
        calls.append(('cancel', oid))
        return None
    async def find(**kwargs):
        calls.append(('find', None))
        return None
    venue._post_exchange, venue._order_status = post, status
    venue._cancel_by_oid, venue._find_matching_open_order = cancel, find
    return venue, calls


def test_partial_submit_cancel_captures_late_fills():
    async def go():
        for final_status, final_qty in [('filled', .0163), ('canceled', .002)]:
            venue, calls = partial_submit_venue(final_status, final_qty)
            result = await venue.send_managed_limit(is_buy=False, qty=.0163,
                limit_px=1373.5, ttl_sec=1, keep_open=lambda: True)
            assert ('cancel', 77) in calls
            assert result['filled_base'] == final_qty
            assert not result['unresolved']
            assert result['diagnostics']['cancel_reason'] == 'partial-submit'
    asyncio.run(go())


def test_partial_submit_without_identity_stays_unresolved():
    async def go():
        venue, calls = partial_submit_venue('canceled', .0005, missing_oid=True)
        result = await venue.send_managed_limit(is_buy=False, qty=.0163,
            limit_px=1373.5, ttl_sec=1, keep_open=lambda: True)
        assert result['unresolved'] and result['status'] == 'submit-unresolved'
        assert result['filled_base'] == .0005
        assert not any(c[0] == 'cancel' for c in calls)
    asyncio.run(go())


def test_partial_submit_rejects_stale_final_cumulative_size():
    async def go():
        venue, calls = partial_submit_venue('canceled', 0)
        result = await venue.send_managed_limit(is_buy=False, qty=.0163,
            limit_px=1373.5, ttl_sec=1, keep_open=lambda: True)
        assert result['unresolved'] and result['pending_oid'] == 77
        assert result['filled_base'] == .0005
    asyncio.run(go())


def test_partial_submit_already_closed_does_not_cancel_again():
    async def go():
        final = {'status': 'canceled', 'filled_base': .0005, 'avg_px': None,
                 'err': None, 'unresolved': False}
        venue, calls = partial_submit_venue('canceled', .0005, status_before_cancel=final)
        result = await venue.send_managed_limit(is_buy=False, qty=.0163,
            limit_px=1373.5, ttl_sec=1, keep_open=lambda: True)
        assert not result['unresolved']
        assert not any(c[0] == 'cancel' for c in calls)
    asyncio.run(go())


def test_full_submit_keeps_immediate_completion_path():
    async def go():
        venue, calls = partial_submit_venue('filled', .0163)
        async def full(payload):
            return ({'status': 'ok', 'response': {'data': {'statuses': [
                {'filled': {'totalSz': '.0163', 'avgPx': '1373.5', 'oid': 77}}
            ]}}}, None, False)
        venue._post_exchange = full
        result = await venue.send_managed_limit(is_buy=True, qty=.0163,
            limit_px=1373.5, ttl_sec=1, keep_open=lambda: True)
        assert not result['unresolved'] and result['filled_base'] == .0163
        assert result['avg_px'] == 1373.5
        assert not calls
    asyncio.run(go())


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
        venue.order_feed = None

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
        assert result["confirm_source"] == "rest"
        diag = result["diagnostics"]
        assert diag["cancel_reason"] == "partial-fill"
        assert diag["submit_ms"] >= 0
        assert diag["sign_ms"] >= 0
        assert diag["request_ms"] >= 0
        assert abs(diag["sign_ms"] + diag["request_ms"] - diag["submit_ms"]) < .001
        assert diag["resting_ms"] >= 0
        assert diag["cancel_confirm_ms"] >= 0
        assert diag["total_ms"] >= diag["submit_ms"]

    asyncio.run(go())


def test_managed_limit_prefers_private_websocket_submission():
    async def go():
        venue = object.__new__(HLVenue)
        venue.account = object()
        venue.asset_id = 110000
        venue.coin = "io:SNDK"
        venue.name = "ENTROPY"
        venue.settle_timeout = 0.2
        venue._signing = FakeSigning()
        venue._signed_payload = lambda action: {"action": action}
        ready = asyncio.Event()
        ready.set()

        async def ws_post(_payload, _timeout):
            return ({"status": "ok", "response": {"data": {"statuses": [
                {"resting": {"oid": 7}}
            ]}}}, None, False)

        venue.order_feed = SimpleNamespace(ready=ready, post_action=ws_post)
        venue._post_exchange = lambda _payload: (_ for _ in ()).throw(
            AssertionError("HTTP should not be used"))
        venue._order_status = lambda _oid: asyncio.sleep(
            0, result={"status": "canceled", "filled_base": 0.0,
                       "avg_px": None, "err": None, "unresolved": False})
        venue._cancel_by_oid = lambda _oid: asyncio.sleep(0, result=None)
        result = await venue.send_managed_limit(
            is_buy=True, qty=0.01, limit_px=100.0, ttl_sec=0.01,
            keep_open=lambda: True)
        assert result["confirm_source"] == "ws-post"

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


def test_order_updates_feed_posts_signed_action_on_same_websocket():
    async def go():
        feed = HLOrderUpdatesFeed(
            "ENTROPY", "ws://unused", "0x" + "1" * 40, "io:SNDK")
        ws = FakeWs()
        feed._ws = ws
        feed.ready.set()
        task = asyncio.create_task(feed.post_action({"action": {"type": "order"}}, 0.2))
        await asyncio.sleep(0)
        request = ws.sent[0]
        assert request["method"] == "post"
        assert request["request"]["type"] == "action"
        feed._handle_message({
            "channel": "post",
            "data": {"id": request["id"], "response": {
                "type": "action", "payload": {"status": "ok"},
            }},
        })
        assert await task == ({"status": "ok"}, None, False)

    asyncio.run(go())


def test_taker_uses_ws_fill_before_ws_post_detail():
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
        feed.ready.set()
        venue.order_feed = feed

        async def post(_payload, _timeout):
            await asyncio.sleep(0.15)
            return ({"status": "ok", "response": {"data": {"statuses": [
                {"filled": {"totalSz": "0.0123", "avgPx": "100.25"}}
            ]}}}, None, False)

        feed.post_action = post
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
        assert "_detail_task" in result
        result = await venue.finalize_order_info(result)
        assert result["avg_px"] == 100.25
        assert "_detail_task" not in result

    asyncio.run(go())


def test_finalize_can_drop_slow_detail_without_waiting():
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
            "_detail_task": task,
        }
        started = time.perf_counter()
        result = await venue.finalize_order_info(info, wait=False)
        assert time.perf_counter() - started < 0.10
        assert task.cancelled()
        assert "_detail_task" not in result

    asyncio.run(go())
