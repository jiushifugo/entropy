"""Engine signal math: midline band directions, inventory ladder, scan.

Run:  python3 -m pytest tests/  (or  python3 tests/test_engine.py)
"""
import asyncio
import os
import sys
import tempfile
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.book import OrderBook  # noqa: E402
from entropy_arb.config import load_config  # noqa: E402
from entropy_arb.engine import Engine  # noqa: E402

NO_ENV = os.path.join(tempfile.gettempdir(), "entropy-arb-no-such.env")


def make_cfg(midline=5.0, upper=4.0, lower=3.0):
    f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    f.write(f"""
thresholds:
  midline_bps: {midline}
  upper_bps: {upper}
  lower_bps: {lower}
execution:
  premium_persist_sec: 0.0
""")
    f.close()
    return load_config(f.name, NO_ENV,
                       symbol="SNDK", hedge_venue="lighter-rh")


class StubVenue:
    def __init__(self, key, label, cap=10000.0, fee=0.0):
        self.key, self.name = key, label
        self.cap_usd, self.fee_bps = cap, fee
        self.size_decimals, self.min_base, self.min_quote = 4, 1e-4, 10.0
        self.position, self.cash, self.volume_usd = 0.0, 0.0, 0.0
        self.orders_per_min = 30
        self.last_traded_ts = 0.0
        self.book = OrderBook()

    def ready_to_trade(self):
        return True

    def px_round(self, px, round_up):
        return px

    async def send_taker(self, *, is_buy, qty, limit_px, reduce_only=False):
        return {"status": "filled", "filled_base": qty, "avg_px": limit_px,
                "err": None, "unresolved": False}

    def set_book(self, bid, ask, sz=50.0):
        self.book.apply_hl([[{"px": str(bid), "sz": str(sz)}],
                            [{"px": str(ask), "sz": str(sz)}]])


def make_engine(**thr):
    cfg = make_cfg(**thr)
    eng = Engine(cfg)
    eng.entropy = StubVenue("entropy", "ENTROPY")
    eng.hedge = StubVenue("hedge", "RH")
    eng.venues = {"entropy": eng.entropy, "hedge": eng.hedge}
    eng._step, eng._min_base, eng._min_notional = 1e-4, 1e-4, 10.0
    return eng


def approx(a, b, tol=1e-9):
    assert abs(a - b) <= tol, f"{a} != {b}"


def test_eff_threshold_directions():
    eng = make_engine(midline=5.0, upper=4.0, lower=3.0)
    e, h = eng.entropy, eng.hedge
    # sell entropy: hurdle = midline + upper = 9
    approx(eng._eff_threshold(buy=h, sell=e), 9.0)
    # buy entropy: hurdle = lower - midline = -2 (unwind side of a positive
    # midline is deliberately cheap — that's what completes the round trip)
    approx(eng._eff_threshold(buy=e, sell=h), -2.0)
    # round trip nets upper + lower regardless of midline sign
    for m in (-7.0, 0.0, 12.5):
        eng.cfg.midline_bps = m
        total = eng._eff_threshold(buy=h, sell=e) + eng._eff_threshold(buy=e, sell=h)
        approx(total, 7.0)


def test_entropy_resting_edge_guard_tracks_hedge_book():
    eng = make_engine(midline=0.0, upper=2.0, lower=2.0)
    eng.cfg.hedge_leg_slippage_bps = 0.0
    e, h = eng.entropy, eng.hedge
    e.set_book(99.99, 100.01)
    h.set_book(100.03, 100.04)
    assert eng._entropy_resting_edge_ok(
        entropy_is_buy=True, entropy=e, hedge=h, entropy_limit=100.0)
    h.set_book(100.01, 100.02)
    assert not eng._entropy_resting_edge_ok(
        entropy_is_buy=True, entropy=e, hedge=h, entropy_limit=100.0)

    h.set_book(99.99, 100.0)
    assert eng._entropy_resting_edge_ok(
        entropy_is_buy=False, entropy=e, hedge=h, entropy_limit=100.03)
    eng.profit_only = True
    eng.cfg.profit_only_min_bps = 5.0
    assert not eng._entropy_resting_edge_ok(
        entropy_is_buy=False, entropy=e, hedge=h, entropy_limit=100.03)


def test_inventory_ladder():
    eng = make_engine()
    eng.cfg.inventory_scale_bps, eng.cfg.inventory_floor_frac = 10.0, 0.5
    e, h = eng.entropy, eng.hedge
    e.set_book(99.9, 100.1)   # mid 100
    h.set_book(99.9, 100.1)
    approx(eng._inv_add_bps(e, h), 0.0)          # flat: dead zone
    e.position = 90.0                             # long $9k of $10k cap
    v = eng._inv_add_bps(e, h)                    # buying entropy adds long
    assert 7.5 < v < 8.5, v                       # u=0.9 -> ~+8
    approx(eng._inv_add_bps(h, e), 0.0)           # selling entropy reduces
    h.position = -90.0                            # hedge short $9k too
    v2 = eng._inv_add_bps(e, h)                   # both legs add -> max()
    assert abs(v2 - v) < 0.6, (v, v2)             # max, not sum


def test_balanced_short_dust_allows_topup_and_favorable_flip():
    eng = make_engine()
    eng.cfg.max_order_notional = 20.0
    eng._min_base, eng._min_notional = 0.01, 10.0
    eng.entropy.position = -0.0081
    eng.hedge.position = 0.0081

    approx(eng._state_cap_notional("sell_entropy", 1500.0), 20.0)
    approx(eng._state_cap_notional("buy_entropy", 1500.0), 20.0)
    assert eng._is_dust_flip("buy_entropy", 1500.0)
    assert not eng._is_dust_flip("sell_entropy", 1500.0)


def test_balanced_long_dust_allows_topup_and_favorable_flip():
    eng = make_engine()
    eng.cfg.max_order_notional = 20.0
    eng._min_base, eng._min_notional = 0.01, 10.0
    eng.entropy.position = 0.0081
    eng.hedge.position = -0.0081

    approx(eng._state_cap_notional("buy_entropy", 1500.0), 20.0)
    approx(eng._state_cap_notional("sell_entropy", 1500.0), 20.0)
    assert eng._is_dust_flip("sell_entropy", 1500.0)
    assert not eng._is_dust_flip("buy_entropy", 1500.0)


def test_dust_flip_requires_absolute_budgeted_net_floor():
    eng = make_engine(midline=0.0, upper=0.1, lower=0.1)
    eng._min_base, eng._min_notional = 0.01, 10.0
    eng.entropy.position = -0.0081
    eng.hedge.position = 0.0081
    plan = SimpleNamespace()

    eng.cfg.dust_flip_min_net_bps = 0.5
    eng._budgeted_net_bps = lambda *_args: 0.49
    assert not eng._dust_flip_plan_ok(
        "buy_entropy", eng.entropy, eng.hedge, plan, 100.0)
    eng._budgeted_net_bps = lambda *_args: 0.50
    assert eng._dust_flip_plan_ok(
        "buy_entropy", eng.entropy, eng.hedge, plan, 100.0)

    eng.profit_only = True
    eng.cfg.profit_only_min_bps = 1.0
    assert not eng._dust_flip_plan_ok(
        "buy_entropy", eng.entropy, eng.hedge, plan, 100.0)


def test_entropy_resting_dust_flip_keeps_absolute_net_floor():
    eng = make_engine(midline=0.0, upper=0.1, lower=0.1)
    eng._min_base, eng._min_notional = 0.01, 10.0
    eng.cfg.hedge_leg_slippage_bps = 0.0
    eng.cfg.dust_flip_min_net_bps = 0.5
    eng.entropy.position = -0.0081
    eng.hedge.position = 0.0081
    eng.entropy.set_book(99.99, 100.0)

    eng.hedge.set_book(100.004, 100.014)
    assert not eng._entropy_resting_edge_ok(
        entropy_is_buy=True, entropy=eng.entropy, hedge=eng.hedge,
        entropy_limit=100.0)
    eng.hedge.set_book(100.006, 100.016)
    assert eng._entropy_resting_edge_ok(
        entropy_is_buy=True, entropy=eng.entropy, hedge=eng.hedge,
        entropy_limit=100.0)


def run_scan(eng):
    async def go():
        # first pass arms the direction, second passes the persistence gate
        # (premium_persist_sec is 0 in the test config)
        eng._scan(__import__("time").time())
        return eng._scan(__import__("time").time())
    return asyncio.run(go())


def test_scan_fires_sell_entropy_above_band():
    eng = make_engine(midline=5.0, upper=4.0, lower=3.0)
    # entropy 15 bps rich vs hedge: above midline+upper=9 -> sell entropy
    eng.entropy.set_book(100.14, 100.16)
    eng.hedge.set_book(99.99, 100.01)
    best = run_scan(eng)
    assert best is not None
    buy, sell, plan = best
    assert sell.key == "entropy" and buy.key == "hedge"
    assert plan.exp_edge_usd > 0


def test_scan_quiet_inside_band():
    eng = make_engine(midline=5.0, upper=4.0, lower=3.0)
    # entropy 5 bps rich = exactly on the midline: inside the band, no trade
    eng.entropy.set_book(100.04, 100.06)
    eng.hedge.set_book(99.99, 100.01)
    assert run_scan(eng) is None


def test_scan_fires_buy_entropy_below_band():
    eng = make_engine(midline=5.0, upper=4.0, lower=3.0)
    # entropy 5 bps CHEAP (premium -5): below midline-lower=+2 -> buy entropy
    eng.entropy.set_book(99.94, 99.96)
    eng.hedge.set_book(99.99, 100.01)
    best = run_scan(eng)
    assert best is not None
    buy, sell, plan = best
    assert buy.key == "entropy" and sell.key == "hedge"


def test_scan_respects_position_caps():
    eng = make_engine(midline=0.0, upper=1.0, lower=1.0)
    eng.entropy.set_book(100.14, 100.16)
    eng.hedge.set_book(99.99, 100.01)
    eng.entropy.position = -100.0   # entropy already short at its cap
    eng.entropy.cap_usd = 10000.0
    eng.hedge.position = 100.0
    eng.hedge.cap_usd = 10000.0
    assert run_scan(eng) is None


def test_entropy_first_cancel_is_safe_and_does_not_halt():
    eng = make_engine(midline=0.0, upper=1.0, lower=1.0)
    eng.cfg.max_consecutive_errors = 1
    eng.cfg.net_tolerance_base = 0.001
    eng.entropy.set_book(100.14, 100.16)
    eng.hedge.set_book(99.99, 100.01)
    _buy, _sell, plan = run_scan(eng)

    async def canceled(**kwargs):
        return {"status": "canceled", "filled_base": 0.0, "avg_px": None,
                "err": None, "unresolved": False}

    eng.entropy.send_taker = canceled
    asyncio.run(eng._execute(eng.hedge, eng.entropy, plan))
    assert eng.consec_errors == 0
    assert eng.halted is False
    assert eng.trades == 0


def test_per_venue_slippage_is_capped_by_expected_edge():
    eng = make_engine(midline=0.0, upper=1.0, lower=1.0)
    eng.cfg.entropy_leg_slippage_bps = 4.0
    eng.cfg.entropy_max_slippage_bps = 4.0
    eng.cfg.hedge_leg_slippage_bps = 1.0
    eng.cfg.slippage_reserve_bps = 2.0
    eng.entropy.set_book(100.14, 100.16)
    eng.hedge.set_book(99.99, 100.01)
    buy, sell, plan = run_scan(eng)
    b, s = eng._arb_leg_slippage_bps(buy, sell, plan)
    assert abs(b - 1.0) < 1e-9
    assert abs(s - 4.0) < 1e-9

    # With only 2 bps of surplus above the hurdle, restore the hedge's
    # requested 1 bps first and give the remaining 1 bps to Entropy.
    plan.marginal_premium_bps = 3.0
    b, s = eng._arb_leg_slippage_bps(buy, sell, plan)
    assert abs((b + s) - 2.0) < 1e-9
    assert abs(b - 1.0) < 1e-9
    assert abs(s - 1.0) < 1e-9


def test_surplus_edge_widens_only_entropy_first_ioc():
    eng = make_engine(midline=0.0, upper=1.3, lower=1.3)
    eng.cfg.entropy_leg_slippage_bps = 0.8
    eng.cfg.entropy_max_slippage_bps = 3.0
    eng.cfg.hedge_leg_slippage_bps = 0.5
    eng.cfg.slippage_reserve_bps = 0.2
    buy, sell = eng.entropy, eng.hedge

    # At the hurdle the 1.1 bps budget keeps the legacy 0.8:0.5 split.
    plan = SimpleNamespace(marginal_premium_bps=1.3)
    b, s = eng._arb_leg_slippage_bps(buy, sell, plan)
    approx(b, 0.8 * 1.1 / 1.3)
    approx(s, 0.5 * 1.1 / 1.3)

    # Extra edge first restores RH's 0.5 bps reserve, then goes only to the
    # Entropy IOC.  A large edge is capped at 3 bps on Entropy.
    plan.marginal_premium_bps = 2.04
    b, s = eng._arb_leg_slippage_bps(buy, sell, plan)
    approx(b, 1.34)
    approx(s, 0.5)
    plan.marginal_premium_bps = 6.99
    b, s = eng._arb_leg_slippage_bps(buy, sell, plan)
    approx(b, 3.0)
    approx(s, 0.5)


def test_entropy_dynamic_cap_defaults_to_legacy_limit():
    cfg = make_cfg(midline=0.0, upper=1.3, lower=1.3)
    assert cfg.entropy_max_slippage_bps == cfg.entropy_leg_slippage_bps


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"{name:40s} OK")
