import asyncio
from test_engine import make_engine


def setup_engine(tmp_path):
    eng = make_engine()
    eng.cfg.daily_risk_state_file = str(tmp_path / 'risk.json')
    eng.cfg.flatten_on_execution_halt = True
    eng.cfg.max_order_notional = 50
    eng.halted = eng._execution_halted = True
    eng.entropy.position, eng.hedge.position = .2, -.2
    calls = []
    for v in (eng.entropy, eng.hedge):
        v.set_book(100, 100.1)
        async def position(v=v):
            return v.position
        async def send(v=v, **kwargs):
            calls.append((v.key, kwargs))
            return {'status': 'filled', 'filled_base': kwargs['qty'],
                    'avg_px': 100, 'err': None, 'unresolved': False}
        v.fetch_position, v.send_taker = position, send
    return eng, calls


def test_opt_in_exit_verified_positions_reduce_only(tmp_path):
    async def go():
        eng, calls = setup_engine(tmp_path)
        await eng._halt_exit_once()
        assert len(calls) == 2
        assert all(kw['reduce_only'] for _, kw in calls)
        assert calls[0][1]['is_buy'] is False
        assert calls[1][1]['is_buy'] is True
        assert abs(eng.entropy.position) < 1e-12
        assert abs(eng.hedge.position) < 1e-12
        assert eng.halted  # never resume strategy after flattening
    asyncio.run(go())


def test_review_unknown_order_and_default_off_do_not_send(tmp_path):
    async def go():
        eng, calls = setup_engine(tmp_path)
        eng._position_review_required = True
        await eng._halt_exit_once()
        eng._position_review_required = False
        eng._pending_entropy_oid = 7
        await eng._halt_exit_once()
        eng._pending_entropy_oid = None
        eng.cfg.flatten_on_execution_halt = False
        await eng._halt_exit_once()
        assert not calls
    asyncio.run(go())


def test_exit_mismatch_locks_instead_of_adopting(tmp_path):
    async def go():
        eng, calls = setup_engine(tmp_path)
        async def mismatch():
            return .4
        eng.entropy.fetch_position = mismatch
        await eng._halt_exit_once()
        assert not calls
        assert eng._position_review_required
        assert eng.entropy.position == .2
    asyncio.run(go())


def test_unknown_exit_response_never_sends_second_leg(tmp_path):
    async def go():
        eng, calls = setup_engine(tmp_path)
        async def unknown(**kwargs):
            calls.append(('entropy', kwargs))
            return {'unresolved': True, 'filled_base': 0}
        eng.entropy.send_taker = unknown
        await eng._halt_exit_once()
        assert len(calls) == 1
        assert eng._position_review_required
    asyncio.run(go())


def test_unavailable_second_venue_prevents_first_exit(tmp_path):
    async def go():
        eng, calls = setup_engine(tmp_path)
        eng._venue_down['hedge'] = 1
        await eng._halt_exit_once()
        assert not calls
    asyncio.run(go())
