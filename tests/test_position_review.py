import asyncio
import json
import time

from test_engine import make_engine
from entropy_arb.venue_hl import HLOrderUpdatesFeed


def test_liquidation_stream_filters_history_and_other_accounts():
    feed = HLOrderUpdatesFeed('ENTROPY', 'ws://unused', '0x123', 'io:SNDK')
    events = []
    feed.on_account_incident = lambda reason, details: events.append(reason)
    def fill(**kw):
        item = {'coin': 'io:SNDK', 'time': feed.started_ms,
                'liquidation': {'liquidatedUser': '0x123'}}
        item.update(kw)
        feed._handle_message({'channel': 'userFills', 'data': {'fills': [item]}})
    fill(time=feed.started_ms - 1)
    fill(coin='BTC')
    fill(liquidation={'liquidatedUser': '0x999'})
    assert not events
    fill()
    assert events == ['liquidation-fill']
    feed._handle_message({'channel': 'user', 'data': {
        'liquidation': {'liquidated_user': '0x123'}}})
    assert len(events) == 2


def test_unexpected_position_locks_without_adopting_or_hedging(tmp_path):
    async def go():
        eng = make_engine()
        eng.cfg.daily_risk_state_file = str(tmp_path / 'risk.json')
        eng.entropy.position = -.0241
        eng.hedge.position = .0241
        async def fetch():
            return .2173
        eng.entropy.fetch_position = fetch
        await eng._reconcile_venue(eng.entropy, strict=False)
        assert eng._position_review_required and eng.halted
        assert eng.entropy.position == -.0241
        assert eng._review_snapshots['entropy'] == .2173
        assert json.loads((tmp_path / 'risk.json.position-review').read_text())['reason'] == 'unexpected-position-change'
        await eng._maybe_hedge()
        await eng._evaluate()
        assert not eng._exec_tasks and eng.hedges == 0
        restarted = make_engine()
        restarted.cfg.daily_risk_state_file = eng.cfg.daily_risk_state_file
        restarted._load_position_review()
        assert restarted.halted and restarted._position_review_required
    asyncio.run(go())


def test_startup_baseline_and_recent_trade_grace_still_work(tmp_path):
    async def go():
        eng = make_engine()
        eng.cfg.daily_risk_state_file = str(tmp_path / 'risk.json')
        async def fetch():
            return .3
        eng.entropy.fetch_position = fetch
        await eng._reconcile_venue(eng.entropy, strict=True)
        assert eng.entropy.position == .3 and not eng.halted
        eng.entropy.last_traded_ts = time.time()
        eng.entropy.position = .2
        await eng._reconcile_venue(eng.entropy, strict=False)
        assert eng.entropy.position == .2 and not eng.halted
    asyncio.run(go())


def test_incident_during_first_leg_does_not_send_stale_inventory_hedge(tmp_path):
    async def go():
        eng = make_engine(midline=0, upper=1, lower=1)
        eng.cfg.daily_risk_state_file = str(tmp_path / 'risk.json')
        e, h = eng.entropy, eng.hedge
        e.set_book(100, 100.1)
        h.set_book(100.3, 100.4)
        plan, _ = eng._plan(e, h, 22)
        assert plan is not None
        async def first_leg(**kwargs):
            eng._require_position_review('liquidation-fill', {'sz': '.2414'})
            return {'status': 'filled', 'filled_base': plan.qty,
                    'avg_px': 100.1, 'err': None, 'unresolved': False}
        async def forbidden(**kwargs):
            raise AssertionError('must not send hedge from unverified positions')
        e.send_taker = first_leg
        h.send_taker = forbidden
        await eng._execute(e, h, plan)
        assert eng._position_review_required
        assert eng._sends.get('hedge') is None
        assert h.position == 0
        await eng._maybe_hedge()
        assert eng.hedges == 0
    asyncio.run(go())


def test_unidentified_managed_order_sets_durable_review_lock(tmp_path):
    async def go():
        eng = make_engine(midline=0, upper=1, lower=1)
        eng.cfg.daily_risk_state_file = str(tmp_path / 'risk.json')
        eng.cfg.entropy_resting_ttl_sec = 1
        e, h = eng.entropy, eng.hedge
        e.set_book(100, 100.1)
        h.set_book(100.3, 100.4)
        plan, _ = eng._plan(e, h, 22)
        async def unresolved(**kwargs):
            return {'status': 'submit-unresolved', 'filled_base': .0005,
                    'avg_px': None, 'err': None, 'unresolved': True}
        e.send_managed_limit = unresolved
        await eng._execute(e, h, plan)
        assert eng._position_review_required and eng.halted
        assert eng._position_review_reason == 'unresolved-managed-order'
        assert (tmp_path / 'risk.json.position-review').exists()
        assert eng._sends.get('hedge') is None
    asyncio.run(go())
