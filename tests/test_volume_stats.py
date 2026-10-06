from entropy_arb.volume_stats import VolumeStats
from test_engine import make_engine
import time


def test_hour_rotation_and_separate_denominators():
    stats = VolumeStats()
    assert stats.rotate(3600) is None
    stats.samples.update(no_edge=3, inventory=1, eligible=100)
    stats.executions.update(no_fill=2)
    assert 'no_edge=3 (75.0%)' in stats.summary()
    assert stats.rotate(7199) is None
    previous = stats.rotate(7200)
    assert 'blocked=4' in previous
    assert not stats.executions and not stats.samples


def test_execution_outcomes_are_exclusive():
    stats = VolumeStats()
    for matched, failed in ((1, False), (.5, False), (0, False), (0, True), (.5, True)):
        stats.record_execution(matched, 1, .0001, failed)
    assert stats.executions == {'full': 1, 'partial': 1, 'no_fill': 1, 'error': 2}


def test_diagnostics_read_only_and_pause_categories():
    eng = make_engine()
    now = time.time()
    buy, sell = eng.entropy, eng.hedge
    before = dict(eng._armed)
    assert eng._volume_reason(now, buy, sell, 'buy_entropy') in ('no_edge', 'venue', 'waiting', 'eligible')
    assert eng._armed == before
    assert not eng._sends and not eng._venue_locks
    eng.halted = True
    assert eng._volume_reason(now, buy, sell, 'buy_entropy') == 'risk'
    eng.halted = False
    eng._pending_entropy_oid = 123
    assert eng._volume_reason(now, buy, sell, 'buy_entropy') == 'venue'


def test_edge_inventory_and_cooldown():
    eng = make_engine(midline=0, upper=1, lower=1)
    eng.entropy.set_book(100, 100.01)
    eng.hedge.set_book(100, 100.01)
    now = time.time()
    assert eng._volume_reason(now, eng.hedge, eng.entropy, 'sell_entropy') == 'no_edge'
    eng.entropy.set_book(101, 101.01)
    eng.entropy.position = -10
    eng.hedge.position = 10
    eng.cfg.max_paired_layers = 1
    assert eng._volume_reason(now, eng.hedge, eng.entropy, 'sell_entropy') == 'inventory'
    eng.last_trade_ts = now
    eng.cfg.cooldown_sec = 5
    assert eng._volume_reason(now, eng.hedge, eng.entropy, 'sell_entropy') == 'waiting'
