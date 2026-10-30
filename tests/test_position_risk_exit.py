import asyncio
import json
import time
from pathlib import Path

import pytest

from test_halt_exit import setup_engine
from test_engine import make_engine
from test_config import load, MINIMAL
from entropy_arb.config import ConfigError


def prepared(tmp_path):
    eng, calls = setup_engine(tmp_path)
    eng.cfg.flatten_on_execution_halt = False  # Independent opt-in exit feature.
    eng.cfg.position_risk_guard_enabled = True
    eng.cfg.position_risk_min_distance_pct = 5
    eng.cfg.position_risk_exit_enabled = True
    eng.cfg.position_risk_exit_distance_pct = 2
    eng.halted = eng._execution_halted = False
    eng.position_risk_snapshot = {"observed_at": time.time(), "position": .2,
                                  "mode": "isolated", "distance_pct": 1}
    eng.position_risk_error = None
    return eng, calls


def test_risk_exit_latches_persists_and_reduces_both_sides(tmp_path):
    async def go():
        eng, calls = prepared(tmp_path)
        eng._consider_position_risk_exit()
        assert eng.halted and eng._position_risk_exit_active
        marker = Path(eng._position_review_path() + ".risk-exit")
        assert json.loads(marker.read_text())["reason"] == "position-risk-exit"
        await eng._position_risk_exit_once()
        assert len(calls) == 2 and all(k["reduce_only"] for _, k in calls)
        assert abs(eng.entropy.position) < 1e-12 and abs(eng.hedge.position) < 1e-12
        assert eng.halted and marker.exists()
        await eng._position_risk_exit_once()
        assert len(calls) == 2  # no order when flat
        restarted = make_engine()
        restarted.cfg.daily_risk_state_file = eng.cfg.daily_risk_state_file
        restarted._load_position_review()
        assert restarted.halted and restarted._position_review_required
    asyncio.run(go())


@pytest.mark.parametrize("condition", ["off", "stale", "future", "unknown", "review", "pending", "flat", "cross", "safe", "mismatch", "locked", "grace", "stop"])
def test_unverified_or_disabled_does_not_trigger_exit(tmp_path, condition):
    async def go():
        eng, calls = prepared(tmp_path)
        s = eng.position_risk_snapshot
        if condition == "off": eng.cfg.position_risk_exit_enabled = False
        if condition == "stale": s["observed_at"] -= 60
        if condition == "future": s["observed_at"] += 60
        if condition == "unknown": eng.position_risk_error = "timeout"
        if condition == "review": eng._position_review_required = True
        if condition == "pending": eng._pending_entropy_oid = 7
        if condition == "flat": s["position"] = 0
        if condition == "cross": s["mode"] = "cross"
        if condition == "safe": s["distance_pct"] = 3
        if condition == "mismatch": s["position"] = .3
        if condition == "locked": await eng._vlock("entropy").acquire()
        if condition == "grace": eng.entropy.last_traded_ts = time.time()
        if condition == "stop": eng.stop.set()
        eng._consider_position_risk_exit()
        assert not eng._position_risk_exit_active and not calls
        assert not Path(eng._position_review_path() + ".risk-exit").exists()
    asyncio.run(go())


def test_risk_exit_partial_first_leg_only_hedges_confirmed_fill(tmp_path):
    async def go():
        eng, calls = prepared(tmp_path)
        async def partial(**kwargs):
            calls.append(("entropy", kwargs))
            return {"status": "canceled", "filled_base": .1, "avg_px": 100}
        eng.entropy.send_taker = partial
        eng._consider_position_risk_exit()
        await eng._position_risk_exit_once()
        assert calls[1][1]["qty"] == .1
        assert eng.entropy.position == .1 and eng.hedge.position == -.1
    asyncio.run(go())


@pytest.mark.parametrize("outcome", ["unknown", "missing", "overfill", "zero", "partial"])
def test_failed_or_unknown_hedge_never_resumes_or_blind_retries(tmp_path, outcome):
    async def go():
        eng, calls = prepared(tmp_path)
        async def failed(**kwargs):
            calls.append(("hedge", kwargs))
            return {"status": "filled", "filled_base": {"unknown": 0, "overfill": 1,
                    "zero": 0, "partial": .1, "missing": None}[outcome],
                    "unresolved": outcome == "unknown"}
        eng.hedge.send_taker = failed
        eng._consider_position_risk_exit()
        await eng._position_risk_exit_once()
        assert eng._position_review_required and eng.halted
        await eng._position_risk_exit_once()
        assert len(calls) == 2
    asyncio.run(go())


@pytest.mark.parametrize("condition", ["venue", "mismatch", "dust", "fetch"])
def test_no_first_order_without_both_verified_routes(tmp_path, condition):
    async def go():
        eng, calls = prepared(tmp_path)
        eng._consider_position_risk_exit()
        if condition == "venue": eng._venue_down["hedge"] = 1
        if condition == "dust": eng.hedge.min_base = 1
        if condition in ("mismatch", "fetch"):
            async def position():
                if condition == "fetch": raise RuntimeError("offline")
                return -.3
            eng.hedge.fetch_position = position
        await eng._position_risk_exit_once()
        assert not calls and eng.halted
    asyncio.run(go())


def test_late_liquidation_lock_takes_precedence(tmp_path):
    async def go():
        eng, calls = prepared(tmp_path)
        eng._consider_position_risk_exit()
        eng._require_position_review("entropy:liquidation-fill", {})
        await eng._position_risk_exit_once()
        assert not calls
    asyncio.run(go())


def test_restart_preserves_other_lock_and_never_auto_recovers_pending(tmp_path):
    eng, _ = prepared(tmp_path)
    eng._consider_position_risk_exit()
    path = Path(eng._position_review_path())
    path.write_text('{"reason": "managed-cancel-pending", "details": {}}')
    before = path.read_text()
    restarted = make_engine()
    restarted.cfg.daily_risk_state_file = eng.cfg.daily_risk_state_file
    restarted._load_position_review()
    assert restarted._position_review_reason == "persisted-position-risk-exit"
    assert restarted._pending_cancel_context is None
    assert path.read_text() == before


def test_persistence_failure_locks_without_orders(tmp_path):
    eng, calls = prepared(tmp_path)
    Path(eng._position_review_path() + ".risk-exit").mkdir()
    eng._consider_position_risk_exit()
    assert not eng._position_risk_exit_active
    assert eng._position_review_required and not calls


def test_known_no_fill_first_leg_does_not_close_hedge(tmp_path):
    async def go():
        eng, calls = prepared(tmp_path)
        async def nofill(**kwargs):
            calls.append(("entropy", kwargs))
            return {"status": "canceled", "filled_base": 0}
        eng.entropy.send_taker = nofill
        eng._consider_position_risk_exit()
        await eng._position_risk_exit_once()
        assert len(calls) == 1
        assert eng.entropy.position == .2 and eng.hedge.position == -.2
        assert eng.halted
    asyncio.run(go())


def test_short_position_and_exact_threshold_trigger(tmp_path):
    async def go():
        eng, calls = prepared(tmp_path)
        eng.entropy.position, eng.hedge.position = -.2, .2
        eng.position_risk_snapshot.update(position=-.2, distance_pct=2)
        eng._consider_position_risk_exit()
        await eng._position_risk_exit_once()
        assert calls[0][1]["is_buy"] and not calls[1][1]["is_buy"]
        assert eng.halted and all(abs(v.position) < 1e-12 for v in eng.venues.values())
    asyncio.run(go())


def test_unknown_first_leg_never_sends_hedge(tmp_path):
    async def go():
        eng, calls = prepared(tmp_path)
        async def unknown(**kwargs):
            calls.append(("entropy", kwargs))
            return {"status": "timeout", "filled_base": 0, "unresolved": True}
        eng.entropy.send_taker = unknown
        eng._consider_position_risk_exit()
        await eng._position_risk_exit_once()
        assert len(calls) == 1 and eng._position_review_required
    asyncio.run(go())


def test_partial_below_hedge_minimum_escalates(tmp_path):
    async def go():
        eng, calls = prepared(tmp_path)
        async def partial(**kwargs):
            calls.append(("entropy", kwargs))
            return {"status": "canceled", "filled_base": .0001, "avg_px": 100}
        eng.entropy.send_taker = partial
        eng._consider_position_risk_exit()
        await eng._position_risk_exit_once()
        assert len(calls) == 1 and eng._position_review_required
        assert eng.entropy.position < .2 and eng.hedge.position == -.2
    asyncio.run(go())


def test_cannot_activate_with_unset_thresholds_or_without_guard():
    for text in ["position_risk_exit_enabled: true", "position_risk_exit_distance_pct: .nan",
                 "position_risk_guard_enabled: true\n  position_risk_min_distance_pct: 3\n  position_risk_exit_enabled: true\n  position_risk_exit_distance_pct: 4"]:
        with pytest.raises(ConfigError):
            load(MINIMAL + "\nrisk:\n  " + text + "\n")
    cfg = load(MINIMAL + "\nrisk:\n  position_risk_guard_enabled: true\n  position_risk_min_distance_pct: 5\n  position_risk_exit_enabled: true\n  position_risk_exit_distance_pct: 2\n")
    assert cfg.position_risk_exit_enabled


def test_equal_five_percent_thresholds_load_and_trigger_together(tmp_path):
    cfg = load(MINIMAL + "\nrisk:\n  position_risk_guard_enabled: true\n  position_risk_min_distance_pct: 5\n  position_risk_exit_enabled: true\n  position_risk_exit_distance_pct: 5\n")
    eng, calls = prepared(tmp_path)
    eng.cfg.position_risk_min_distance_pct = cfg.position_risk_min_distance_pct
    eng.cfg.position_risk_exit_distance_pct = cfg.position_risk_exit_distance_pct
    eng.position_risk_snapshot["distance_pct"] = 5.01
    assert eng._position_risk_block(True, .1) is None
    eng._consider_position_risk_exit()
    assert not eng._position_risk_exit_active
    eng.position_risk_snapshot["distance_pct"] = 5.0
    assert eng._position_risk_block(True, .1) == "position-risk-distance"
    eng._consider_position_risk_exit()
    assert eng._position_risk_exit_active and eng.halted
    assert not calls
