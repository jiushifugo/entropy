"""Managed Hyperliquid limit-order lifecycle without network access."""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.venue_hl import HLVenue  # noqa: E402


class FakeSigning:
    @staticmethod
    def order_request_to_order_wire(req, asset):
        return {"req": req, "asset": asset}

    @staticmethod
    def order_wires_to_order_action(wires):
        return {"type": "order", "orders": wires}


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
