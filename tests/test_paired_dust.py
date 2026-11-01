import asyncio
from pathlib import Path
import time

import pytest

from test_position_risk_exit import prepared
from test_config import load, MINIMAL
from entropy_arb.config import ConfigError


def dust_engine(tmp_path):
    eng, calls = prepared(tmp_path)
    eng.cfg.position_risk_paired_dust_max_usd = .2
    eng.entropy.position, eng.hedge.position = -.0001, .0001
    eng.position_risk_snapshot.update(position=-.0001, distance_pct=1, observed_at=time.time())
    eng.position_risk_snapshot["mark_px"] = 100
    return eng, calls


def test_verified_paired_dust_does_not_latch_but_entries_still_blocked(tmp_path):
    async def go():
        eng, calls = dust_engine(tmp_path)
        await eng._refresh_paired_dust()
        assert eng._paired_dust is not None
        eng._consider_position_risk_exit()
        assert not eng.halted and not eng._position_risk_exit_active
        assert eng._position_risk_block(False, .1, 100) == "position-risk-distance"
        assert not calls and not Path(eng._position_review_path() + ".risk-exit").exists()
    asyncio.run(go())


@pytest.mark.parametrize("case", ["off", "over-cap", "one-sided", "same-sign", "unequal",
                                  "one-tradeable", "stale-book", "pending", "review", "halt-exit",
                                  "stale-risk", "unknown-risk"])
def test_not_all_small_positions_are_exempted(tmp_path, case):
    async def go():
        eng, calls = dust_engine(tmp_path)
        if case == "off": eng.cfg.position_risk_paired_dust_max_usd = 0
        if case == "over-cap": eng.cfg.position_risk_paired_dust_max_usd = .001
        if case == "one-sided": eng.hedge.position = 0
        if case == "same-sign": eng.hedge.position = -.0001
        if case == "unequal": eng.hedge.position = .0002
        if case == "one-tradeable": eng.hedge.min_base, eng.hedge.min_quote = .00001, .001
        if case == "stale-book": eng.hedge.book.alive_ts = 0
        if case == "pending": eng._pending_entropy_oid = 7
        if case == "review": eng._position_review_required = True
        if case == "halt-exit": eng._position_risk_exit_active = True
        if case == "stale-risk": eng.position_risk_snapshot["observed_at"] -= 60
        if case == "unknown-risk": eng.position_risk_error = "offline"
        await eng._refresh_paired_dust()
        assert eng._paired_dust is None and not calls
    asyncio.run(go())


def test_two_account_reads_required_and_failures_remove_old_observation(tmp_path):
    async def go():
        eng, calls = dust_engine(tmp_path)
        await eng._refresh_paired_dust()
        count = 0
        async def changed():
            nonlocal count
            count += 1
            return .0001 if count == 1 else .02
        eng.hedge.fetch_position = changed
        await eng._refresh_paired_dust()
        assert count == 2 and eng._paired_dust is None
        assert eng._position_review_required and not calls
    asyncio.run(go())


def test_fetch_failure_never_leaves_cached_exemption(tmp_path):
    async def go():
        eng, _ = dust_engine(tmp_path)
        await eng._refresh_paired_dust()
        async def fail():
            raise RuntimeError("offline")
        eng.hedge.fetch_position = fail
        await eng._refresh_paired_dust()
        assert eng._paired_dust is None
        eng._consider_position_risk_exit()
        assert eng._dust_verification_pending and not eng._position_risk_exit_active
        eng._dust_verification_since -= 60
        await eng._refresh_paired_dust()
        assert eng._position_review_reason == "paired-dust-verification-timeout"
    asyncio.run(go())


def test_changed_or_expired_cache_cannot_suppress_normal_exit(tmp_path):
    async def go():
        eng, _ = dust_engine(tmp_path)
        await eng._refresh_paired_dust()
        eng._paired_dust["verified_at"] -= 60
        eng._consider_position_risk_exit()
        assert eng._position_risk_exit_active
    asyncio.run(go())


def test_liquidation_incident_still_locks_verified_dust(tmp_path):
    async def go():
        eng, calls = dust_engine(tmp_path)
        await eng._refresh_paired_dust()
        eng._require_position_review("entropy:liquidation-fill", {})
        await eng._refresh_paired_dust()
        assert eng._paired_dust is None and eng.halted and eng._position_review_required
        assert not calls
    asyncio.run(go())


def test_paired_dust_config_default_off_and_rejects_invalid_cap():
    assert load(MINIMAL).position_risk_paired_dust_max_usd == 0
    assert load(MINIMAL + "\nrisk:\n  position_risk_paired_dust_max_usd: 0.5\n").position_risk_paired_dust_max_usd == .5
    for cap in ("-1", ".nan", ".inf"):
        with pytest.raises(ConfigError):
            load(MINIMAL + "\nrisk:\n  position_risk_paired_dust_max_usd: " + cap + "\n")


def test_startup_unready_book_waits_then_verifies_without_exit_marker(tmp_path):
    async def go():
        eng, calls = dust_engine(tmp_path)
        eng.hedge.book.ready = False
        await eng._refresh_paired_dust()
        eng._consider_position_risk_exit()
        assert eng._dust_verification_pending
        assert not eng._position_risk_exit_active and not eng._position_review_required
        assert eng._scan(time.time()) is None
        assert eng._position_risk_block(True, .1, 100) == "paired-dust-verification-pending"
        assert not Path(eng._position_review_path() + ".risk-exit").exists()
        eng.hedge.set_book(100, 100.1)
        await eng._refresh_paired_dust()
        eng._consider_position_risk_exit()
        assert not eng._dust_verification_pending and eng._paired_dust
        assert not eng.halted and not calls
    asyncio.run(go())


def test_unready_tiny_candidate_timeout_is_manual_not_blind_exit(tmp_path):
    async def go():
        eng, calls = dust_engine(tmp_path)
        eng.hedge.book.ready = False
        await eng._refresh_paired_dust()
        eng._dust_verification_since -= 60
        eng._consider_position_risk_exit()
        assert eng._position_review_reason == "paired-dust-verification-timeout"
        assert not eng._position_risk_exit_active and not calls
    asyncio.run(go())


def test_large_inventory_is_not_deferred_while_book_unready(tmp_path):
    async def go():
        eng, calls = dust_engine(tmp_path)
        eng.entropy.position, eng.hedge.position = -.2, .2
        eng.position_risk_snapshot["position"] = -.2
        eng.hedge.book.ready = False
        await eng._refresh_paired_dust()
        eng._consider_position_risk_exit()
        assert not eng._dust_verification_pending and eng._position_risk_exit_active
        assert not calls
    asyncio.run(go())


def test_old_incident_lock_is_not_cleared_by_startup_wait(tmp_path):
    async def go():
        eng, calls = dust_engine(tmp_path)
        eng._require_position_review("risk-exit-dust", {})
        before = Path(eng._position_review_path()).read_text()
        eng.hedge.book.ready = False
        await eng._refresh_paired_dust()
        eng._consider_position_risk_exit()
        assert eng._position_review_required and eng.halted
        assert Path(eng._position_review_path()).read_text() == before
        assert not calls
    asyncio.run(go())
