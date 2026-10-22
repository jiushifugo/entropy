import asyncio
import json
from pathlib import Path
from test_engine import make_engine


def pending_engine(tmp_path):
    eng = make_engine()
    eng.cfg.daily_risk_state_file = str(tmp_path / 'risk.json')
    eng.entropy.position, eng.hedge.position = -.6109, .6109
    ctx = {'oid': 77, 'known_fill': 0.0, 'requested': .0181,
           'identity': [eng.cfg.entropy.symbol, eng.cfg.hedge_venue],
           'positions': {'entropy': -.6109, 'hedge': .6109}}
    eng._pending_entropy_oid = 77
    eng._pending_cancel_context = ctx
    eng._require_position_review('managed-cancel-pending', ctx)
    async def resolved(oid):
        assert oid == 77
        return {'status': 'canceled', 'filled_base': 0, 'err': None, 'unresolved': False}
    eng.entropy.resolve_managed_order = resolved
    for venue in eng.venues.values():
        async def position(v=venue):
            return ctx['positions'][v.key]
        venue.fetch_position = position
    return eng


def test_timeout_only_verified_recovery_archives_marker(tmp_path):
    async def go():
        eng = pending_engine(tmp_path)
        await eng._resolve_pending_entropy_order()
        assert not eng.halted and not eng._position_review_required
        assert eng._pending_entropy_oid is None
        assert not Path(eng._position_review_path()).exists()
        assert list(tmp_path.glob('*.verified-*'))
    asyncio.run(go())


def test_timeout_recovery_does_not_override_execution_halt(tmp_path):
    async def go():
        eng = pending_engine(tmp_path)
        eng._execution_halted = True
        await eng._resolve_pending_entropy_order()
        assert eng.halted and not eng._position_review_required
    asyncio.run(go())


def test_changed_cumulative_fill_escalates_to_manual_review(tmp_path):
    async def go():
        eng = pending_engine(tmp_path)
        async def late_fill(oid):
            return {'status': 'canceled', 'filled_base': .001}
        eng.entropy.resolve_managed_order = late_fill
        await eng._resolve_pending_entropy_order()
        assert eng._position_review_required
        assert eng._position_review_reason == 'pending-order-fill-changed'
        assert json.loads(Path(eng._position_review_path()).read_text())['reason'] == 'pending-order-fill-changed'
        await eng._resolve_pending_entropy_order()
        assert eng.halted
    asyncio.run(go())


def test_position_mismatch_never_adopted(tmp_path):
    async def go():
        eng = pending_engine(tmp_path)
        async def wrong_position():
            return -.7
        eng.entropy.fetch_position = wrong_position
        await eng._resolve_pending_entropy_order()
        assert eng._position_review_reason == 'pending-order-position-mismatch'
        assert eng.entropy.position == -.6109
    asyncio.run(go())


def test_second_snapshot_must_also_agree(tmp_path):
    async def go():
        eng = pending_engine(tmp_path)
        calls = 0
        async def changes():
            nonlocal calls
            calls += 1
            return -.6109 if calls == 1 else -.611
        eng.entropy.fetch_position = changes
        await eng._resolve_pending_entropy_order()
        assert eng._position_review_reason == 'pending-order-position-mismatch'
        assert eng.halted
    asyncio.run(go())


def test_missing_fill_and_open_order_cannot_resume(tmp_path):
    async def go():
        eng = pending_engine(tmp_path)
        for result in (None, {'status': 'canceled'}, {'status': 'open', 'filled_base': 0}):
            async def unresolved(oid):
                return result
            eng.entropy.resolve_managed_order = unresolved
            await eng._resolve_pending_entropy_order()
            assert eng.halted and eng._pending_entropy_oid == 77
    asyncio.run(go())


def test_new_execution_timeout_creates_recoverable_context(tmp_path):
    async def go():
        eng = make_engine(midline=0, upper=1, lower=1)
        eng.cfg.daily_risk_state_file = str(tmp_path / 'risk.json')
        eng.cfg.entropy_resting_ttl_sec = 1
        eng.cfg.flatten_on_execution_halt = True
        e, h = eng.entropy, eng.hedge
        e.set_book(100, 100.1)
        h.set_book(100.3, 100.4)
        plan, _ = eng._plan(e, h, 22)
        async def timeout(**kwargs):
            return {'status': 'cancel-timeout', 'filled_base': 0,
                    'avg_px': None, 'err': None, 'unresolved': True, 'pending_oid': 77}
        e.send_managed_limit = timeout
        await eng._execute(e, h, plan)
        assert eng._position_review_reason == 'managed-cancel-pending'
        ctx = eng._pending_cancel_context
        assert ctx['oid'] == 77 and ctx['requested'] == plan.qty
        assert ctx['positions'] == {'entropy': 0, 'hedge': 0}
        assert json.loads(Path(eng._position_review_path()).read_text())['details'] == ctx
    asyncio.run(go())


def test_liquidation_during_verification_supersedes_timeout(tmp_path):
    async def go():
        eng = pending_engine(tmp_path)
        async def liquidated():
            eng._require_position_review('entropy:liquidation-fill', {'sz': .6})
            return -.6109
        eng.entropy.fetch_position = liquidated
        await eng._resolve_pending_entropy_order()
        assert eng._position_review_reason == 'entropy:liquidation-fill'
        assert eng._position_review_required and eng.halted
    asyncio.run(go())


def test_recoverable_marker_survives_restart_but_legacy_lock_does_not_auto_clear(tmp_path):
    async def go():
        eng = pending_engine(tmp_path)
        restarted = pending_engine(tmp_path)
        restarted._pending_entropy_oid = restarted._pending_cancel_context = None
        restarted._load_position_review()
        assert restarted._position_review_reason == 'managed-cancel-pending'
        restarted.entropy.position = restarted.hedge.position = 0
        await restarted._reconcile_positions(hedge=False, strict=True)
        assert not restarted.halted
        assert restarted.entropy.position == -.6109
        Path(eng._position_review_path()).write_text(json.dumps({
            'reason': 'execution-outcome-unknown', 'details': {'direction': 'sell_entropy'}}))
        legacy = make_engine()
        legacy.cfg.daily_risk_state_file = eng.cfg.daily_risk_state_file
        legacy._load_position_review()
        assert legacy._pending_cancel_context is None and legacy.halted
    asyncio.run(go())
