import asyncio
import time
from types import SimpleNamespace

import pytest

from entropy_arb.venue_hl import HLVenue
from test_engine import make_engine


def hl_equity(core=True):
    v = object.__new__(HLVenue)
    v.account = SimpleNamespace(query_address="0x123")
    v.conf = SimpleNamespace(hl_dex="io")
    v.include_core_equity = core
    return v


@pytest.mark.parametrize("response", [[], [["day", {}]], [["day", {"accountValueHistory": [[1, None]]}]],
                                      [["day", {"accountValueHistory": [[1, "nan"]]}]]])
def test_unified_equity_never_falls_back_to_a_different_scope(response):
    async def go():
        v = hl_equity()
        calls = []
        async def query(payload):
            calls.append(payload["type"])
            return response
        v._info = query
        with pytest.raises((ValueError, TypeError)):
            await v.fetch_equity()
        assert calls == ["portfolio"]
    asyncio.run(go())


@pytest.mark.parametrize("value", [None, "", True, "nan", "inf"])
def test_missing_bucket_equity_is_not_zero(value):
    async def go():
        v = hl_equity(False)
        async def query(_):
            return {"marginSummary": {"accountValue": value}, "withdrawable": "0"}
        v._info = query
        with pytest.raises(ValueError):
            await v.fetch_equity()
    asyncio.run(go())


def test_explicit_zero_equity_remains_a_valid_real_loss():
    async def go():
        v = hl_equity()
        async def query(_):
            return [["day", {"accountValueHistory": [[time.time() * 1000, "0"]]}]]
        v._info = query
        assert await v.fetch_equity() == (0, None)
    asyncio.run(go())


def equity_engine(tmp_path):
    e = make_engine()
    e.cfg.daily_risk_state_file = str(tmp_path / "risk.json")
    e.cfg.daily_loss_limit_usd = 20
    e.cfg.daily_volume_limit_usd = 0
    e.daily_risk_day = e._utc_day()
    e.daily_equity_baseline = 1400
    for v in e.venues.values():
        v.start_equity, v.equity, v.free = None, None, None
    return e


def test_invalid_snapshots_pause_additions_without_manufacturing_loss(tmp_path):
    async def go():
        e = equity_engine(tmp_path)
        async def missing(): return None
        async def hedge(): return (600, 500)
        e.entropy.fetch_equity, e.hedge.fetch_equity = missing, hedge
        for _ in range(3):
            await e._refresh_daily_risk()
        assert e._equity_unverified and e.daily_pnl_usd is None
        assert not e.risk_limited and e.daily_equity_baseline == 1400
        assert e._position_risk_block(True, .04) == "equity-unverified"
        e.entropy.position = -.1
        e.hedge.position = .1
        e.cfg.position_risk_guard_enabled = False
        reduction = SimpleNamespace(qty=.04)
        assert e._position_risk_plan_block(e.entropy, e.hedge, reduction) is None
        assert e._position_risk_plan_block(e.hedge, e.entropy, reduction) == "equity-unverified"
        async def recovered(): return (800, None)
        e.entropy.fetch_equity = recovered
        await e._refresh_daily_risk()
        assert not e._equity_unverified and e.daily_pnl_usd == 0
        assert not e.risk_limited
    asyncio.run(go())


def test_real_loss_requires_two_consecutive_valid_samples(tmp_path):
    async def go():
        e = equity_engine(tmp_path)
        async def loss(): return (770, None)
        async def hedge(): return (600, 500)
        async def bad(): return (float("nan"), None)
        e.entropy.fetch_equity, e.hedge.fetch_equity = loss, hedge
        await e._refresh_daily_risk()
        assert not e.risk_limited
        e.entropy.fetch_equity = bad
        await e._refresh_daily_risk()
        assert e._daily_loss_sample_streak == 0
        e.entropy.fetch_equity = loss
        await e._refresh_daily_risk()
        assert not e.risk_limited
        await e._refresh_daily_risk()
        assert e.risk_limited and e.daily_pnl_usd == -30
    asyncio.run(go())


def test_incomplete_first_poll_of_new_day_does_not_reuse_old_baseline(tmp_path):
    async def go():
        e = equity_engine(tmp_path)
        e.daily_risk_day = "2000-01-01"
        async def missing(): return None
        async def hedge(): return (600, 500)
        e.entropy.fetch_equity, e.hedge.fetch_equity = missing, hedge
        await e._refresh_daily_risk()
        assert e.daily_equity_baseline is None and e.daily_pnl_usd is None
        async def recovered(): return (801, None)
        e.entropy.fetch_equity = recovered
        await e._refresh_daily_risk()
        assert e.daily_equity_baseline == 1401 and e.daily_pnl_usd == 0
    asyncio.run(go())


def test_equity_recovery_does_not_clear_existing_loss_lock(tmp_path):
    async def go():
        e = equity_engine(tmp_path)
        e.risk_limited = True
        async def entropy(): return (801, None)
        async def hedge(): return (600, 500)
        e.entropy.fetch_equity, e.hedge.fetch_equity = entropy, hedge
        await e._refresh_daily_risk()
        assert e.risk_limited and not e._equity_unverified
    asyncio.run(go())
