import time

import pytest

from entropy_arb.position_risk import parse_entry_evidence, project_entry
from test_engine import make_engine

ACCOUNT = "0x" + "1" * 40


def inputs():
    active = {"user": ACCOUNT, "coin": "io:SNDK", "leverage": {
        "type": "isolated", "value": 15, "rawUsd": "0"},
        "maxTradeSzs": ["10", "10"], "availableToTrade": ["803", "803"], "markPx": "100"}
    meta = {"universe": [{"name": "io:SNDK", "maxLeverage": 15, "marginTableId": 15}],
            "collateralToken": 0, "marginTables": []}
    snapshot = {"position": 0, "observed_at": time.time()}
    return active, meta, snapshot


def evidence():
    a, m, s = inputs()
    return s, parse_entry_evidence(a, m, ACCOUNT, "io:SNDK", time.time())


def test_official_builtin_table_and_flat_long_short_three_percent():
    snapshot, entry = evidence()
    assert entry["maintenance_rate"] == 1 / 30
    for buy in (True, False):
        px = 100.04 if buy else 99.96
        result = project_entry(snapshot, entry, buy, .4, px, .9, 3, 15)
        assert result["reason"] is None
        assert 3 < result["estimated_distance_pct"] < 3.5
        assert project_entry(snapshot, entry, buy, .4, px, .9, 5, 15)["reason"] == "position-risk-projected-distance"


def test_adverse_execution_cannot_pass_just_because_leverage_is_fifteen():
    snapshot, entry = evidence()
    assert project_entry(snapshot, entry, False, .4, 99, .9, 3, 15)["reason"] == "position-risk-projected-distance"
    assert project_entry(snapshot, entry, True, .4, 101, .9, 3, 15)["reason"] == "position-risk-projected-distance"


@pytest.mark.parametrize("case", ["identity", "cross", "collateral", "tiers", "missing-table", "bad-capacity", "nan"])
def test_unmodeled_evidence_is_not_guessed(case):
    active, meta, _ = inputs()
    if case == "identity": active["user"] = "0xother"
    if case == "cross": active["leverage"]["type"] = "cross"
    if case == "collateral": meta["collateralToken"] = 1
    if case == "tiers":
        meta["marginTables"] = [[15, {"marginTiers": [{"lowerBound": "0", "maxLeverage": 15},
                                                     {"lowerBound": "100", "maxLeverage": 10}]}]]
    if case == "missing-table": meta["universe"][0]["marginTableId"] = 99
    if case == "bad-capacity": active["maxTradeSzs"] = ["1"]
    if case == "nan": active["markPx"] = "nan"
    with pytest.raises((ValueError, KeyError, TypeError)):
        parse_entry_evidence(active, meta, ACCOUNT, "io:SNDK", time.time())


def test_stale_capacity_funds_and_flat_cash_fail_closed():
    snapshot, entry = evidence()
    assert project_entry(snapshot, None, True, .1, 100, .9, 3, 15)["reason"] == "position-risk-entry-unavailable"
    entry["observed_at"] -= 20
    assert project_entry(snapshot, entry, True, .1, 100, .9, 3, 15)["reason"] == "position-risk-entry-unavailable"
    snapshot, entry = evidence()
    assert project_entry(snapshot, entry, True, 11, 100, .9, 3, 15)["reason"] == "position-risk-order-capacity"
    entry["available_margin"][0] = 0
    assert project_entry(snapshot, entry, True, .1, 100, .9, 3, 15)["reason"] == "position-risk-insufficient-margin"
    snapshot, entry = evidence()
    entry["raw_usd"] = 10
    assert project_entry(snapshot, entry, True, .1, 100, .9, 3, 15)["reason"] == "position-risk-entry-unavailable"


def test_addition_gets_no_new_margin_credit_and_flip_is_blocked():
    snapshot, entry = evidence()
    snapshot.update(position=1, mark_px=100, liquidation_px=96, leverage=15)
    small = project_entry(snapshot, entry, True, .01, 100, .9, 3, 15)
    large = project_entry(snapshot, entry, True, 1, 100, .9, 3, 15)
    assert small["reason"] is None
    assert large["reason"] == "position-risk-projected-distance"
    assert large["estimated_distance_pct"] < small["estimated_distance_pct"]
    assert project_entry(snapshot, entry, False, 2, 100, .9, 3, 15)["reason"] == "position-risk-flip-unmodeled"


def test_engine_flat_entry_now_needs_and_uses_evidence():
    eng = make_engine()
    eng.cfg.position_risk_guard_enabled = True
    eng.cfg.position_risk_min_distance_pct = 3
    snapshot, entry = evidence()
    eng.position_risk_snapshot = snapshot
    assert eng._position_risk_block(True, .1, 100.04) == "position-risk-entry-unavailable"
    snapshot["entry"] = entry
    assert eng._position_risk_block(True, .1, 100.04) is None
    assert eng._position_risk_block(False, .1, 99) == "position-risk-projected-distance"
    eng.position_risk_error = "offline"
    assert eng._position_risk_block(True, .1, 100) == "position-risk-unavailable"


def test_model_also_checks_old_position_snapshot_and_adverse_new_mark():
    snapshot, entry = evidence()
    snapshot["observed_at"] -= 60
    assert project_entry(snapshot, entry, True, .1, 100, .9, 3, 15)["reason"] == "position-risk-entry-unavailable"
    snapshot, entry = evidence()
    snapshot.update(position=1, mark_px=100, liquidation_px=96, leverage=15)
    entry["mark_px"] = 98
    assert project_entry(snapshot, entry, True, .01, 98, .9, 3, 15)["reason"] == "position-risk-projected-distance"


def test_plan_gate_blocks_unsafe_order_before_any_first_leg():
    import asyncio
    from types import SimpleNamespace
    eng = make_engine()
    eng.cfg.position_risk_guard_enabled = True
    eng.cfg.position_risk_min_distance_pct = 3
    snapshot, entry = evidence()
    snapshot["entry"] = entry
    eng.position_risk_snapshot = snapshot
    async def forbidden(**kwargs):
        raise AssertionError("unsafe entry must not send")
    eng.entropy.send_taker = eng.hedge.send_taker = forbidden
    plan = SimpleNamespace(qty=.1, buy_limit=101, sell_limit=102)
    assert asyncio.run(eng._execute(eng.entropy, eng.hedge, plan)) is False
