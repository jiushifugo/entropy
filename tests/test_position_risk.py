import asyncio
import pytest

from entropy_arb.position_risk import parse_position_risk, entry_block_reason
from entropy_arb.venue_hl import HLVenue
from test_engine import make_cfg
from test_config import load, MINIMAL


def sample(size=1, liq=90):
    state = {"time": 100000, "assetPositions": [{"position": {
        "coin": "io:SNDK", "szi": str(size), "liquidationPx": str(liq),
        "marginUsed": "10", "leverage": {"type": "isolated", "value": 15}}}]}
    ctx = [{"universe": [{"name": "io:SNDK"}]}, [{"markPx": "100"}]]
    return state, ctx


def test_signed_distance_long_short_and_beyond_liquidation():
    for size, liq, expected in [(1, 90, 10), (-1, 110, 10), (1, 101, -1), (-1, 99, -1)]:
        state, ctx = sample(size, liq)
        snap = parse_position_risk(state, "io:SNDK", ctx, now=100)
        assert snap["distance_pct"] == expected
        assert snap["margin_used"] == 10


def test_flat_is_not_assumed_margin_safe():
    snap = parse_position_risk({"time": 100000, "assetPositions": []}, "io:SNDK", None, now=100)
    assert snap["liquidation_px"] is None
    assert entry_block_reason(snap, None, 0, True, .1, 3, 15, now=100) == "position-risk-flat-unverified"


@pytest.mark.parametrize("value", [None, "nan", "inf", "-1", "0", True])
def test_invalid_liquidation_is_not_safe(value):
    state, ctx = sample()
    state["assetPositions"][0]["position"]["liquidationPx"] = value
    with pytest.raises((ValueError, TypeError)):
        parse_position_risk(state, "io:SNDK", ctx, now=100)


def test_missing_stale_or_wrong_identity_data():
    state, ctx = sample()
    with pytest.raises(ValueError):
        parse_position_risk({}, "io:SNDK", ctx, now=100)
    with pytest.raises(ValueError):
        parse_position_risk(state, "io:SNDK", ctx, now=116)
    ctx[0]["universe"][0]["name"] = "BTC"
    with pytest.raises(ValueError):
        parse_position_risk(state, "io:SNDK", ctx, now=100)


def test_guard_closing_allowed_but_no_crossing_or_stale_assumptions():
    state, ctx = sample(1, 99)
    snap = parse_position_risk(state, "io:SNDK", ctx, now=100)
    def gate(buy=True, qty=.1, local=1, now=100, error=None):
        return entry_block_reason(snap, error, local, buy, qty, 3, 15, now=now)
    assert gate() == "position-risk-distance"
    assert gate(False, 1) is None
    assert gate(False, 1.01) == "position-risk-distance"
    assert gate(now=116) == "position-risk-stale"
    assert gate(local=2) == "position-risk-position-changed"
    assert gate(error="request failed") == "position-risk-unavailable"


def test_config_default_off_and_threshold_validation():
    assert make_cfg().position_risk_guard_enabled is False
    from entropy_arb.config import ConfigError
    for settings in ["position_risk_guard_enabled: true", "position_risk_min_distance_pct: .nan",
                     "position_risk_max_age_sec: 0", "position_risk_max_age_sec: 16"]:
        with pytest.raises(ConfigError):
            load(MINIMAL + "\nrisk:\n  " + settings + "\n")


def test_adapter_uses_actual_account_and_dex_for_risk():
    async def go():
        from types import SimpleNamespace
        import time
        venue = object.__new__(HLVenue)
        venue.account = SimpleNamespace(query_address="0x123")
        venue.conf = SimpleNamespace(hl_dex="io")
        venue.coin = "io:SNDK"
        state, ctx = sample()
        state["time"] = time.time() * 1000
        calls = []
        async def info(payload):
            calls.append(payload)
            return state if payload["type"] == "clearinghouseState" else ctx
        venue._info = info
        assert (await venue.fetch_position_risk())["position"] == 1
        assert calls[0]["user"] == "0x123"
        assert all(c["dex"] == "io" for c in calls)
    asyncio.run(go())


def test_engine_guard_default_off_and_enabled_prevents_first_leg():
    from test_engine import make_engine
    from types import SimpleNamespace
    eng = make_engine()
    assert eng._position_risk_block(True, 1) is None
    eng.cfg.position_risk_guard_enabled = True
    eng.cfg.position_risk_min_distance_pct = 3
    assert eng._position_risk_block(True, 1) == "position-risk-unavailable"
    async def forbidden(**kwargs):
        raise AssertionError("must not submit any leg")
    eng.entropy.send_taker = eng.hedge.send_taker = forbidden
    assert asyncio.run(eng._execute(eng.entropy, eng.hedge, SimpleNamespace(qty=1))) is False


def test_guard_disabled_does_not_change_record_only_or_recovery_locks():
    from test_engine import make_engine
    eng = make_engine()
    eng.position_risk_error = "offline"
    eng._position_review_required = True
    eng.halted = True
    assert eng._position_risk_block(True, 1) is None
    assert eng._position_review_required and eng.halted
