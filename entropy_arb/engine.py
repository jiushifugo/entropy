"""Two-venue arbitrage engine: Entropy vs one hedge venue.

The signal is a fixed band around a configured midline (config.yaml):

    SELL entropy / BUY hedge  when executable premium >= midline + upper (+fees)
    BUY entropy / SELL hedge  when executable premium <= midline - lower (+fees)

Around the signal: per-direction persistence arming,
per-venue inventory ladder + position caps, per-venue order budgets and
reactive rate-limit exclusion, net-delta hedging, venue-outage pausing with
probing, and periodic on-chain reconciliation. There is no paper mode: the
bot either trades live or runs --record-only (data collection, no strategy).
Both venues' books are recorded to 1-minute CSV bars throughout.
"""
from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Dict, List, Optional

import aiohttp

from .book import ArbPlan, floor_step, plan_arb
from .config import Config
from .recorder import MinuteRecorder
from .venue_hl import HLVenue
from .venue_lighter import LighterVenue

log = logging.getLogger("engine")

CSV_HEADER = ["ts", "direction", "buy_venue", "sell_venue", "qty",
              "buy_limit", "sell_limit", "buy_notional", "sell_notional",
              "exp_edge_usd", "gross_edge_usd", "marginal_premium_bps",
              "midline_bps", "inv_add_bps", "ok", "buy_fill", "sell_fill",
              "buy_status", "sell_status", "fill_edge_usd"]
BALANCE_POLL_SEC = 30.0


class Engine:
    def __init__(self, cfg: Config, record_only: bool = False) -> None:
        self.cfg = cfg
        self.record_only = record_only
        self.session: Optional[aiohttp.ClientSession] = None
        self.entropy = None
        self.hedge = None
        self.venues: Dict[str, object] = {}
        self.recorder: Optional[MinuteRecorder] = None
        self.markets_ready = False
        self.stop = asyncio.Event()
        self._update_evt = asyncio.Event()
        self._reconcile_evt = asyncio.Event()
        # per-venue locks: an execution holds both; a reconcile holds one, so
        # a chain read can never race an in-flight order on that venue
        self._venue_locks: Dict[str, asyncio.Lock] = {}
        self._exec_tasks: set = set()
        self.halted = False
        self.consec_errors = 0
        self.last_trade_ts = 0.0
        self.trades = 0
        self.hedges = 0
        self.total_exp_edge = 0.0
        self.total_fill_edge = 0.0
        self.start_ts = time.time()
        self._last_skiplog = 0.0
        self._poke_due: Optional[float] = None
        # per-direction persistence arming: direction key -> first-seen ts
        self._armed: Dict[str, Optional[float]] = {"sell_entropy": None,
                                                   "buy_entropy": None}
        self._step = 1e-4
        self._min_base = 0.0
        self._min_notional = 10.0
        self._mtm_baseline: Optional[float] = None
        # proactive per-venue send budget: timestamps of recent order sends
        self._sends: Dict[str, deque] = {}
        # reactive per-venue throttle: venue key -> excluded until
        self._venue_limited_until: Dict[str, float] = {}
        # venue outage tracking: key -> down-since ts; a down venue pauses
        # trading and is probed every venue_probe_sec until it answers
        self._venue_down: Dict[str, float] = {}
        self._venue_probe_at: Dict[str, float] = {}
        self._venue_fetch_fails: Dict[str, int] = {}
        # per-execution records for the dashboard (newest last)
        self.recent_trades: deque = deque(maxlen=50)
        self.daily_risk_day = ""
        self.daily_equity_baseline: Optional[float] = None
        self.daily_pnl_usd: Optional[float] = None
        self.daily_volume_usd = 0.0
        self.profit_only = False
        self.risk_limited = False
        self._risk_halted = False

    # ------------------------------------------------------------- utilities

    def _vlock(self, key: str) -> asyncio.Lock:
        lock = self._venue_locks.get(key)
        if lock is None:
            lock = self._venue_locks[key] = asyncio.Lock()
        return lock

    def _venue_rate_ok(self, v) -> bool:
        """True while the venue is under its max_orders_per_min (sliding 60s)."""
        dq = self._sends.setdefault(v.key, deque())
        now = time.time()
        while dq and now - dq[0] > 60.0:
            dq.popleft()
        return len(dq) < v.orders_per_min

    def _venue_limited(self, v) -> bool:
        return time.time() < self._venue_limited_until.get(v.key, 0.0)

    def _mark_limited(self, v) -> None:
        self._venue_limited_until[v.key] = time.time() + self.cfg.rate_limit_pause_sec
        log.warning("[%s] rate limited — trading paused for %.0fs",
                    v.name, self.cfg.rate_limit_pause_sec)

    def _record_send(self, v) -> None:
        self._sends.setdefault(v.key, deque()).append(time.time())

    def request_stop(self) -> None:
        self.stop.set()
        self._update_evt.set()
        self._reconcile_evt.set()

    # ------------------------------------------------------------- lifecycle

    async def run(self) -> None:
        # Long keepalive so order-path connections survive quiet spells; the
        # keepalive loop pings inside this window to hold them open.
        self.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(
            keepalive_timeout=75.0, ttl_dns_cache=300))
        try:
            await self._run_inner()
        finally:
            await self.session.close()

    def _make_venue(self, vc):
        if vc.kind == "lighter":
            return LighterVenue(vc, self.session, self.cfg.settle_timeout_sec)
        return HLVenue(vc, self.cfg.hl_api_url, self.cfg.hl_ws_url,
                       self.session, self.cfg.settle_timeout_sec)

    async def _run_inner(self) -> None:
        cfg = self.cfg
        self.entropy = self._make_venue(cfg.entropy)
        self.hedge = self._make_venue(cfg.hedge)
        self.venues = {"entropy": self.entropy, "hedge": self.hedge}
        await asyncio.gather(self.entropy.load_market(), self.hedge.load_market())
        self.markets_ready = True

        live = not self.record_only
        if live:
            if not cfg.creds_complete:
                raise RuntimeError(
                    "live trading needs credentials for both venues in .env "
                    "(see .env.example); use --record-only to run without "
                    "them / 实盘需要在 .env 中配置两个交易所的密钥，仅采集数据"
                    "请用 --record-only")
            self.entropy.init_signer()
            self.hedge.init_signer()
            if self.hedge.kind == "hl":
                self.entropy.share_nonces_with(self.hedge)
        if (self.hedge.kind == "hl"
                and self.entropy._query_address()
                and self.entropy._query_address() == self.hedge._query_address()):
            self.hedge.include_core_equity = False  # shared account: count once

        self._step = 10 ** -min(self.entropy.size_decimals,
                                self.hedge.size_decimals)
        self._min_base = max(self.entropy.min_base, self.hedge.min_base,
                             self._step)
        self._min_notional = max(cfg.min_order_notional,
                                 self.entropy.min_quote, self.hedge.min_quote)
        log.info("pair ENTROPY(%s)-%s(%s): midline=%+.2fbps band=[-%.2f, +%.2f] "
                 "fees=%.2f+%.2f step=%g min_ntl=$%g",
                 self.entropy.conf.symbol, self.hedge.name,
                 self.hedge.conf.symbol, cfg.midline_bps, cfg.lower_bps,
                 cfg.upper_bps, self.entropy.fee_bps, self.hedge.fee_bps,
                 self._step, self._min_notional)

        if self.record_only:
            log.warning("RECORD-ONLY — collecting minute data, no strategy, "
                        "no orders")
        else:
            log.warning("LIVE — real orders will be sent (use --record-only "
                        "for credential-less data collection)")
            await self._reconcile_positions(hedge=False, strict=True)
            await self._refresh_daily_risk(startup=True)
            log.info("starting positions: %s (net %+.6g)",
                     " ".join(f"{v.name}={v.position:+.6g}"
                              for v in self.venues.values()),
                     sum(v.position for v in self.venues.values()))

        tasks: List[asyncio.Task] = []
        for v in self.venues.values():
            tasks += v.start_tasks(self.stop, self._update_evt.set, live)
        if cfg.recorder_enabled or self.record_only:
            self.recorder = MinuteRecorder(cfg.recorder_csv, self.entropy.book,
                                           self.hedge.book, cfg.staleness_sec)
            tasks.append(asyncio.create_task(self.recorder.run(self.stop),
                                             name="recorder"))
        if not self.record_only:
            tasks.append(asyncio.create_task(self._strategy_loop(),
                                             name="strategy"))
            tasks.append(asyncio.create_task(self._balance_loop(),
                                             name="balances"))
            tasks.append(asyncio.create_task(self._http_keepalive_loop(),
                                             name="keepalive"))
        tasks.append(asyncio.create_task(self._status_loop(), name="status"))
        if live:
            tasks.append(asyncio.create_task(self._reconcile_loop(),
                                             name="reconcile"))

        await self.stop.wait()
        if self._exec_tasks:  # let in-flight executions settle, never cancel
            log.info("waiting for %d in-flight execution(s) to settle",
                     len(self._exec_tasks))
            await asyncio.wait(self._exec_tasks,
                               timeout=cfg.settle_timeout_sec + 2.0)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for v in self.venues.values():
            await v.close()
        log.info("shutdown — %d trades, %d hedges, exp edge $%.4f, "
                 "fill edge $%.4f", self.trades, self.hedges,
                 self.total_exp_edge, self.total_fill_edge)

    # --------------------------------------------------------------- signals

    def _inv_add_bps(self, buy, sell) -> float:
        """Inventory ladder: a surcharge that grows once a venue's position
        passes floor_frac of its cap in the direction the trade would add to
        (buying adds when that venue is >= flat long; selling adds when the
        venue is <= flat short). Max of the two venues' ramps."""
        scale = self.cfg.inventory_scale_bps
        if scale <= 0:
            return 0.0
        floor = min(max(self.cfg.inventory_floor_frac, 0.0), 0.99)

        def ramp(v, adding: bool) -> float:
            if not adding:
                return 0.0
            ref = v.book.mid()
            if ref is None:
                return 0.0
            u = min(abs(v.position) * ref / v.cap_usd, 1.0)
            if u <= floor:
                return 0.0
            return scale * (u - floor) / (1.0 - floor)

        return max(ramp(buy, buy.position >= 0), ramp(sell, sell.position <= 0))

    def _eff_threshold(self, buy, sell) -> float:
        """Net hurdle (bps, on top of fees) for the direction buy->sell.

        selling entropy: executable premium must clear midline + upper;
        buying entropy: the reverse premium must clear lower - midline."""
        tol = self.cfg.net_tolerance_base
        epos, hpos = self.entropy.position, self.hedge.position
        closing_long = (sell.key == "entropy" and epos > tol and hpos < -tol)
        closing_short = (buy.key == "entropy" and epos < -tol and hpos > tol)
        if closing_long:
            base = self.cfg.midline_bps - self.cfg.exit_bps
        elif closing_short:
            # plan_arb sees hedge/entropy (the inverse premium).
            base = -self.cfg.midline_bps - self.cfg.exit_bps
        elif sell.key == "entropy":
            base = self.cfg.midline_bps + self.cfg.upper_bps
        else:
            base = self.cfg.lower_bps - self.cfg.midline_bps
        return base + self._inv_add_bps(buy, sell)

    def _state_cap_notional(self, dkey: str, ref_px: float) -> float:
        """Allow entries only while flat; once paired inventory exists, allow
        only the direction that reduces it, capped at flat.  An execution can
        therefore never cross zero and silently become a reverse entry.

        A partial close can leave a balanced pair below either venue's minimum
        size.  Such dust cannot be reduced directly.  While daily risk is open,
        admit either direction at one normal slice: adding makes the old pair
        hedgeable, while the opposite direction may cross zero only after the
        dedicated positive-edge gate in ``_dust_flip_plan_ok``."""
        tol = self.cfg.net_tolerance_base
        epos, hpos = self.entropy.position, self.hedge.position
        if abs(epos) <= tol and abs(hpos) <= tol:
            return self.cfg.max_order_notional
        if epos > tol and hpos < -tol:
            paired_base = min(epos, -hpos)
            paired_notional = paired_base * ref_px
            if (paired_base < self._min_base
                    or paired_notional < self._min_notional):
                if self.risk_limited:
                    return 0.0
                return self.cfg.max_order_notional
            return min(epos, -hpos) * ref_px if dkey == "sell_entropy" else 0.0
        if epos < -tol and hpos > tol:
            paired_base = min(-epos, hpos)
            paired_notional = paired_base * ref_px
            if (paired_base < self._min_base
                    or paired_notional < self._min_notional):
                if self.risk_limited:
                    return 0.0
                return self.cfg.max_order_notional
            return min(-epos, hpos) * ref_px if dkey == "buy_entropy" else 0.0
        # Reconcile/hedge inconsistent inventory before admitting strategy flow.
        return 0.0

    def _is_dust_flip(self, dkey: str, ref_px: float) -> bool:
        """True when ``dkey`` crosses a balanced sub-minimum pair through 0."""
        tol = self.cfg.net_tolerance_base
        epos, hpos = self.entropy.position, self.hedge.position
        if epos > tol and hpos < -tol:
            paired = min(epos, -hpos)
            dust = (paired < self._min_base
                    or paired * ref_px < self._min_notional)
            return dust and dkey == "sell_entropy"
        if epos < -tol and hpos > tol:
            paired = min(-epos, hpos)
            dust = (paired < self._min_base
                    or paired * ref_px < self._min_notional)
            return dust and dkey == "buy_entropy"
        return False

    def _dust_flip_plan_ok(self, dkey: str, buy, sell, plan: ArbPlan,
                           ref_px: float) -> bool:
        if not self._is_dust_flip(dkey, ref_px):
            return True
        net_bps = self._budgeted_net_bps(buy, sell, plan)
        floor_bps = self.cfg.dust_flip_min_net_bps
        if self.profit_only:
            floor_bps = max(floor_bps, self.cfg.profit_only_min_bps)
        if net_bps + 1e-9 < floor_bps:
            self._skiplog("[DUST FLIP] %s skipped: budgeted net %.2fbps "
                          "< %.2fbps", dkey, net_bps, floor_bps)
            return False
        return True

    def _headroom(self, buy, sell, ref_px: float) -> float:
        hb = buy.cap_usd - buy.position * ref_px
        hs = sell.cap_usd + sell.position * ref_px
        return min(hb, hs)

    def _plan(self, buy, sell, cap_notional: float):
        return plan_arb(
            buy.book, sell.book,
            threshold_bps=self._eff_threshold(buy, sell),
            buy_fee_bps=buy.fee_bps, sell_fee_bps=sell.fee_bps,
            take_fraction=self.cfg.take_fraction,
            cap_notional=cap_notional,
            min_base=self._min_base,
            min_notional=self._min_notional,
            size_step=self._step,
        )

    # -------------------------------------------------------------- strategy

    async def _strategy_loop(self) -> None:
        while not self.stop.is_set():
            await self._update_evt.wait()
            self._update_evt.clear()
            if self.stop.is_set():
                break
            try:
                await self._evaluate()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("evaluate failed")

    def _schedule_poke(self, delay: float) -> None:
        loop = asyncio.get_running_loop()
        due = loop.time() + max(delay, 0.01)
        if self._poke_due is not None and self._poke_due <= due + 0.02:
            return

        def _fire() -> None:
            self._poke_due = None
            self._update_evt.set()

        self._poke_due = due
        loop.call_at(due, _fire)

    def _skiplog(self, fmt: str, *args) -> None:
        now = time.time()
        if now - self._last_skiplog >= 2.0:
            self._last_skiplog = now
            log.info(fmt, *args)

    async def _evaluate(self) -> None:
        cfg = self.cfg
        if self.halted:
            return
        now = time.time()
        if now - self.last_trade_ts < cfg.cooldown_sec:
            self._schedule_poke(cfg.cooldown_sec - (now - self.last_trade_ts))
            return
        best = self._scan(now)
        if best is None:
            return
        buy, sell, plan = best
        # _scan verified both locks free and nothing ran since (no awaits),
        # so these acquires take the no-suspension fast path
        await self._vlock(buy.key).acquire()
        await self._vlock(sell.key).acquire()
        # run as a task so a shutdown cancels the strategy loop's await, never
        # the in-flight execution itself (both legs must settle)
        t = asyncio.create_task(self._execute_locked(buy, sell, plan))
        self._exec_tasks.add(t)
        t.add_done_callback(self._exec_tasks.discard)
        await asyncio.shield(t)

    async def _execute_locked(self, buy, sell, plan: ArbPlan) -> None:
        """Run one execution while holding both venue locks (acquired by the
        caller), then release them and settle the aftermath: unresolved
        outcomes escalate to reconcile, everything else gets a net-delta
        check."""
        unresolved = False
        try:
            unresolved = await self._execute(buy, sell, plan)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("execute failed")
        finally:
            self._vlock(buy.key).release()
            self._vlock(sell.key).release()
        if unresolved:
            self._reconcile_evt.set()
        else:
            await self._maybe_hedge()
        self._update_evt.set()  # freed venues may have a queued opportunity

    def _scan(self, now: float):
        """Evaluate both directions; returns the best executable
        (buy, sell, plan), or None."""
        cfg = self.cfg
        best = None
        for buy, sell, dkey in ((self.hedge, self.entropy, "sell_entropy"),
                                (self.entropy, self.hedge, "buy_entropy")):
            if not (buy.book.is_fresh(cfg.staleness_sec)
                    and sell.book.is_fresh(cfg.staleness_sec)):
                continue
            if not (buy.ready_to_trade() and sell.ready_to_trade()):
                continue
            if self._venue_down:
                continue  # a venue in outage pauses the (only) pair
            if self._vlock(buy.key).locked() or self._vlock(sell.key).locked():
                continue  # mid-execution or mid-reconcile
            if self._venue_limited(buy) or self._venue_limited(sell):
                continue  # reactive 429 exclusion
            if not (self._venue_rate_ok(buy) and self._venue_rate_ok(sell)):
                self._skiplog("%s deferred: venue order budget exhausted", dkey)
                continue
            # never refire into books that predate the venue's own last trade
            if (buy.book.last_update_ts <= buy.last_traded_ts
                    or sell.book.last_update_ts <= sell.last_traded_ts):
                continue
            ref_px = buy.book.best_ask()
            state_cap = self._state_cap_notional(dkey, ref_px or 0.0)
            if self.risk_limited and abs(self.entropy.position) \
                    <= cfg.net_tolerance_base and abs(self.hedge.position) \
                    <= cfg.net_tolerance_base:
                state_cap = 0.0
            if state_cap < self._min_notional:
                self._armed[dkey] = None
                continue
            plan, reason = self._plan(
                buy, sell, min(cfg.max_order_notional, state_cap))
            edge_present = reason not in ("no_edge", "empty_book")
            if not edge_present:
                self._armed[dkey] = None
                continue
            if (plan is not None
                    and not self._dust_flip_plan_ok(
                        dkey, buy, sell, plan, ref_px or plan.buy_limit)):
                self._armed[dkey] = None
                continue
            armed = self._armed.get(dkey)
            if armed is None:
                # premium persistence: only fire if the edge survives
                # premium_persist_sec (filters one-tick phantoms)
                self._armed[dkey] = now
                self._schedule_poke(cfg.premium_persist_sec)
                continue
            if now - armed < cfg.premium_persist_sec:
                self._schedule_poke(cfg.premium_persist_sec - (now - armed))
                continue
            if plan is None:
                continue
            if not self._profit_only_plan_ok(dkey, buy, sell, plan):
                continue
            headroom = self._headroom(buy, sell, plan.buy_limit)
            if headroom < plan.buy_notional:
                plan, _ = self._plan(buy, sell,
                                     min(cfg.max_order_notional, state_cap,
                                         headroom))
                if plan is None:
                    self._skiplog("%s blocked by position caps (headroom $%.0f)",
                                  dkey, max(headroom, 0.0))
                    continue
                if not self._profit_only_plan_ok(dkey, buy, sell, plan):
                    continue
                if not self._dust_flip_plan_ok(
                        dkey, buy, sell, plan, ref_px or plan.buy_limit):
                    continue
            if best is None or plan.exp_edge_usd > best[2].exp_edge_usd:
                best = (buy, sell, plan)
        return best

    # ------------------------------------------------------------- execution

    def _arb_leg_slippage_bps(self, buy, sell, plan: ArbPlan):
        """Per-venue slippage limits, dynamically capped by executable edge.

        At the configured hurdle this preserves the original proportional
        split between Entropy and the hedge venue.  Once executable premium
        exceeds the effective hurdle, the surplus first restores the hedge
        venue's desired allowance and then widens only the difficult,
        Entropy-first IOC up to ``entropy_max_slippage_bps``.  Thus a large
        live edge can improve Entropy fill probability without weakening the
        threshold for marginal opportunities.
        """
        cfg = self.cfg
        desired_entropy = max(cfg.entropy_leg_slippage_bps, 0.0)
        desired_hedge = max(cfg.hedge_leg_slippage_bps, 0.0)
        # Budget against the excursion from the configured premium centre,
        # not the absolute cross-venue premium.  With a non-zero midline the
        # latter can be small or negative on one unwind side even though the
        # configured full round trip remains profitable.
        band_bps = (cfg.upper_bps if sell.key == "entropy"
                    else cfg.lower_bps)
        budget = max(band_bps - cfg.slippage_reserve_bps, 0.0)
        desired_total = desired_entropy + desired_hedge
        scale = min(1.0, budget / desired_total) if desired_total > 0 else 0.0
        entropy_allow = desired_entropy * scale
        hedge_allow = desired_hedge * scale

        # plan.marginal_premium_bps is gross.  The effective hurdle is net of
        # taker fees, so remove both venue fees before measuring surplus.
        surplus = max(
            plan.marginal_premium_bps - buy.fee_bps - sell.fee_bps
            - self._eff_threshold(buy, sell),
            0.0,
        )
        hedge_topup = min(surplus, max(desired_hedge - hedge_allow, 0.0))
        hedge_allow += hedge_topup
        surplus -= hedge_topup
        entropy_allow = min(
            max(cfg.entropy_max_slippage_bps, desired_entropy),
            entropy_allow + surplus,
        )

        if buy.key == "entropy":
            return entropy_allow, hedge_allow
        return hedge_allow, entropy_allow

    def _profit_only_plan_ok(self, dkey: str, buy, sell,
                             plan: ArbPlan) -> bool:
        """Gate strategy flow after the daily loss limit is reached.

        The estimate is deliberately pessimistic: both legs are repriced to
        the normal/dynamic execution bounds and taker fees are then removed.
        Emergency delta hedges do not pass through ``_scan`` and therefore
        always remain allowed.
        """
        if not self.profit_only:
            return True
        net_bps = self._budgeted_net_bps(buy, sell, plan)
        if net_bps + 1e-9 < self.cfg.profit_only_min_bps:
            self._skiplog("[PROFIT-ONLY] %s skipped: budgeted net %.2fbps "
                          "< %.2fbps", dkey, net_bps,
                          self.cfg.profit_only_min_bps)
            return False
        return True

    def _budgeted_net_bps(self, buy, sell, plan: ArbPlan) -> float:
        """Net edge after fees and the entry leg slippage allowances."""
        buy_slip_bps, sell_slip_bps = self._arb_leg_slippage_bps(
            buy, sell, plan)
        worst_buy = plan.buy_limit * (1.0 + buy_slip_bps / 1e4)
        worst_sell = plan.sell_limit * (1.0 - sell_slip_bps / 1e4)
        net_usd = plan.qty * (
            worst_sell * (1.0 - plan.sell_fee)
            - worst_buy * (1.0 + plan.buy_fee)
        )
        notional = max(plan.qty * worst_buy, 1e-12)
        return net_usd / notional * 1e4

    def _entropy_resting_edge_ok(self, *, entropy_is_buy: bool, entropy,
                                 hedge, entropy_limit: float) -> bool:
        """Whether a resting Entropy fill is still safe to hedge now.

        The Entropy price is fixed by the live order.  Revalue only the RH leg
        from its current executable top, reserve its normal entry slippage,
        remove both taker fees, and require the same state-aware hurdle that
        admitted the original plan.  Profit-only mode adds its absolute net
        floor.  Stale/down books and a hard risk limit cancel immediately.
        """
        cfg = self.cfg
        if (self.halted or self.stop.is_set() or self.risk_limited
                or entropy.key in self._venue_down
                or hedge.key in self._venue_down
                or not entropy.book.is_fresh(cfg.staleness_sec)
                or not hedge.book.is_fresh(cfg.staleness_sec)):
            return False
        hedge_slip = max(cfg.hedge_leg_slippage_bps, 0.0) / 1e4
        if entropy_is_buy:
            buy, sell = entropy, hedge
            buy_px = entropy_limit
            top = hedge.book.best_bid()
            sell_px = top * (1.0 - hedge_slip) if top is not None else None
        else:
            buy, sell = hedge, entropy
            top = hedge.book.best_ask()
            buy_px = top * (1.0 + hedge_slip) if top is not None else None
            sell_px = entropy_limit
        if buy_px is None or sell_px is None or buy_px <= 0 or sell_px <= 0:
            return False
        net_bps = (
            sell_px * (1.0 - sell.fee_bps / 1e4)
            / (buy_px * (1.0 + buy.fee_bps / 1e4)) - 1.0
        ) * 1e4
        floor_bps = self._eff_threshold(buy, sell)
        direction = "buy_entropy" if entropy_is_buy else "sell_entropy"
        if self._is_dust_flip(direction, buy_px):
            floor_bps = max(floor_bps, cfg.dust_flip_min_net_bps)
        if self.profit_only:
            floor_bps = max(floor_bps, cfg.profit_only_min_bps)
        return net_bps + 1e-9 >= floor_bps

    async def _execute(self, buy, sell, plan: ArbPlan) -> bool:
        """Send both legs and settle the fills. Both venue locks are held by
        the caller. Returns True when an outcome is unresolved and the caller
        must escalate to reconcile."""
        if self.halted:
            return False
        cfg = self.cfg
        inv_bps = self._inv_add_bps(buy, sell)
        direction = "sell_entropy" if sell.key == "entropy" else "buy_entropy"
        self.last_trade_ts = time.time()
        log.info("[ARB] %s: BUY %s %.6g @<=%.6g | SELL %s @>=%.6g | "
                 "take $%.0f of $%.0f | prem %.2fbps | exp $%.4f",
                 direction, buy.name, plan.qty, plan.buy_limit, sell.name,
                 plan.sell_limit, plan.buy_notional, plan.q_max_notional,
                 plan.marginal_premium_bps, plan.exp_edge_usd)
        buy_slip_bps, sell_slip_bps = self._arb_leg_slippage_bps(
            buy, sell, plan)
        buy_bound = buy.px_round(
            plan.buy_limit * (1 + buy_slip_bps / 1e4), round_up=False)
        sell_bound = sell.px_round(
            plan.sell_limit * (1 - sell_slip_bps / 1e4), round_up=True)
        log.info("[LIMITS] %s: %s %.2fbps | %s %.2fbps | reserve %.2fbps",
                 direction, buy.name, buy_slip_bps, sell.name, sell_slip_bps,
                 cfg.slippage_reserve_bps)
        def failed_info(err=None, status="not-sent"):
            return {"status": status, "filled_base": 0.0, "avg_px": None,
                    "err": err, "unresolved": False}

        async def send(v, *, is_buy, qty, limit_px):
            self._record_send(v)
            try:
                return await v.send_taker(is_buy=is_buy, qty=qty,
                                          limit_px=limit_px)
            except Exception as exc:
                return failed_info(repr(exc), "send-failed")

        # Entropy-first execution.  RH is never touched unless the difficult
        # Entropy IOC has a confirmed fill.  This removes the dominant live
        # failure mode observed in production: Entropy canceled / RH filled.
        entropy_is_buy = buy.key == "entropy"
        entropy = buy if entropy_is_buy else sell
        hedge = sell if entropy_is_buy else buy
        entropy_bound = buy_bound if entropy_is_buy else sell_bound
        first_entropy_book_ts = entropy.book.last_update_ts
        managed_entropy = (
            cfg.entropy_resting_ttl_sec > 0
            and hasattr(entropy, "send_managed_limit")
        )
        entropy_started = time.perf_counter()
        if managed_entropy:
            self._record_send(entropy)
            try:
                entropy_info = await entropy.send_managed_limit(
                    is_buy=entropy_is_buy, qty=plan.qty,
                    limit_px=entropy_bound,
                    ttl_sec=cfg.entropy_resting_ttl_sec,
                    keep_open=lambda: self._entropy_resting_edge_ok(
                        entropy_is_buy=entropy_is_buy, entropy=entropy,
                        hedge=hedge, entropy_limit=entropy_bound),
                )
            except Exception as exc:
                entropy_info = failed_info(repr(exc), "send-failed")
        else:
            entropy_info = await send(
                entropy, is_buy=entropy_is_buy, qty=plan.qty,
                limit_px=entropy_bound)
        entropy_elapsed_ms = (time.perf_counter() - entropy_started) * 1000.0
        entropy_log_tag = "ENTROPY LIMIT" if managed_entropy else "ENTROPY IOC"
        log.info("[%s] %s attempt 1: %s %.6g @%s%.6g | %s fill %.6g | "
                 "%.0fms", entropy_log_tag, direction,
                 "BUY" if entropy_is_buy else "SELL", plan.qty,
                 "<=" if entropy_is_buy else ">=", entropy_bound,
                 entropy_info.get("status", "unknown"),
                 float(entropy_info.get("filled_base") or 0.0),
                 entropy_elapsed_ms)
        entropy_fill = float(entropy_info.get("filled_base") or 0.0)

        # A canceled Entropy IOC is safe to retry because RH has not been
        # touched.  Retry at most once, only after a fresh Entropy book update
        # and a full re-plan against both current books.  The retry must retain
        # a configured net edge after fees and entry slippage budgets.
        retryable_cancel = (
            cfg.entropy_retry_once
            and not managed_entropy
            and entropy_fill <= cfg.net_tolerance_base
            and str(entropy_info.get("status", "")).startswith("canceled")
            and not entropy_info.get("unresolved")
            and entropy_info.get("err") is None
        )
        if retryable_cancel:
            loop = asyncio.get_running_loop()
            deadline = loop.time() + 0.25
            while (entropy.book.last_update_ts <= first_entropy_book_ts
                   and loop.time() < deadline):
                await asyncio.sleep(0.02)
            books_fresh = (entropy.book.last_update_ts > first_entropy_book_ts
                           and buy.book.is_fresh(cfg.staleness_sec)
                           and sell.book.is_fresh(cfg.staleness_sec))
            if not books_fresh:
                log.info("[ENTROPY RETRY] %s skipped: no fresh book", direction)
            elif not self._venue_rate_ok(entropy):
                log.info("[ENTROPY RETRY] %s skipped: order budget exhausted",
                         direction)
            else:
                ref_px = buy.book.best_ask()
                state_cap = self._state_cap_notional(direction, ref_px or 0.0)
                retry_cap = min(cfg.max_order_notional,
                                plan.buy_notional, state_cap)
                retry_plan, retry_reason = self._plan(buy, sell, retry_cap)
                if retry_plan is None:
                    log.info("[ENTROPY RETRY] %s skipped: %s", direction,
                             retry_reason)
                else:
                    retry_net_bps = self._budgeted_net_bps(
                        buy, sell, retry_plan)
                    retry_floor_bps = max(
                        cfg.entropy_retry_min_net_bps,
                        cfg.profit_only_min_bps if self.profit_only else 0.0,
                    )
                    if self._is_dust_flip(
                            direction, ref_px or retry_plan.buy_limit):
                        retry_floor_bps = max(
                            retry_floor_bps, cfg.dust_flip_min_net_bps)
                    if retry_net_bps + 1e-9 < retry_floor_bps:
                        log.info("[ENTROPY RETRY] %s skipped: budgeted net "
                                 "%.2fbps < %.2fbps", direction,
                                 retry_net_bps, retry_floor_bps)
                    else:
                        retry_buy_slip, retry_sell_slip = \
                            self._arb_leg_slippage_bps(buy, sell, retry_plan)
                        retry_buy_bound = buy.px_round(
                            retry_plan.buy_limit
                            * (1 + retry_buy_slip / 1e4), round_up=False)
                        retry_sell_bound = sell.px_round(
                            retry_plan.sell_limit
                            * (1 - retry_sell_slip / 1e4), round_up=True)
                        retry_bound = (retry_buy_bound if entropy_is_buy
                                       else retry_sell_bound)
                        log.info("[ENTROPY RETRY] %s fresh prem %.2fbps | "
                                 "budgeted net %.2fbps | limit %s%.6g",
                                 direction,
                                 retry_plan.marginal_premium_bps,
                                 retry_net_bps,
                                 "<=" if entropy_is_buy else ">=",
                                 retry_bound)
                        retry_started = time.perf_counter()
                        retry_info = await send(
                            entropy, is_buy=entropy_is_buy,
                            qty=retry_plan.qty, limit_px=retry_bound)
                        retry_elapsed_ms = (
                            time.perf_counter() - retry_started) * 1000.0
                        log.info("[ENTROPY IOC] %s attempt 2: %s %.6g "
                                 "@%s%.6g | %s fill %.6g | %.0fms",
                                 direction,
                                 "BUY" if entropy_is_buy else "SELL",
                                 retry_plan.qty,
                                 "<=" if entropy_is_buy else ">=",
                                 retry_bound,
                                 retry_info.get("status", "unknown"),
                                 float(retry_info.get("filled_base") or 0.0),
                                 retry_elapsed_ms)
                        plan = retry_plan
                        entropy_info = retry_info
                        entropy_fill = float(
                            entropy_info.get("filled_base") or 0.0)

        hedge_info = failed_info(status="skipped-no-entropy-fill")
        if (entropy_fill > cfg.net_tolerance_base
                and not entropy_info.get("unresolved")
                and entropy_info.get("err") is None):
            hedge_qty = floor_step(entropy_fill, self._step)
            hedge_is_buy = not entropy_is_buy
            ref = hedge.book.best_ask() if hedge_is_buy else hedge.book.best_bid()
            # Once Entropy has filled this is no longer an optional arb leg:
            # completing the hedge is mandatory.  Do not reuse the tiny
            # entry-edge budget (which can be fractions of a bp in volume
            # mode); use the dedicated delta-hedge protection instead.
            hedge_slip_bps = cfg.hedge_slippage_bps
            if (ref is not None and hedge_qty >= hedge.min_base
                    and hedge_qty * ref >= max(cfg.min_order_notional,
                                               hedge.min_quote)):
                hedge_bound = hedge.px_round(
                    ref * (1 + hedge_slip_bps / 1e4), round_up=False) \
                    if hedge_is_buy else hedge.px_round(
                        ref * (1 - hedge_slip_bps / 1e4), round_up=True)
                log.info("[HEDGE LEG] %s: %s %.6g on %s @%.6g after "
                         "Entropy fill %.6g | protection %.2fbps", direction,
                         "BUY" if hedge_is_buy else "SELL", hedge_qty,
                         hedge.name, hedge_bound, entropy_fill,
                         hedge_slip_bps)
                hedge_info = await send(hedge, is_buy=hedge_is_buy,
                                        qty=hedge_qty, limit_px=hedge_bound)
            else:
                hedge_info = failed_info(
                    "confirmed Entropy fill is below hedgeable minimum",
                    "unhedgeable")

        if entropy_is_buy:
            binfo, sinfo = entropy_info, hedge_info
        else:
            binfo, sinfo = hedge_info, entropy_info
        for v, info, side in ((buy, binfo, "buy"), (sell, sinfo, "sell")):
            if info.get("err"):
                log.error("[%s] %s leg: %s", v.name, side, info["err"])
        bfill = binfo["filled_base"]
        sfill = sinfo["filled_base"]
        buy.position += bfill
        sell.position -= sfill
        if bfill:
            bpx = binfo.get("avg_px") or plan.buy_limit
            buy.cash -= bfill * bpx * (1 + plan.buy_fee)
            buy.volume_usd += bfill * bpx
        if sfill:
            spx = sinfo.get("avg_px") or plan.sell_limit
            sell.cash += sfill * spx * (1 - plan.sell_fee)
            sell.volume_usd += sfill * spx

        matched = min(bfill, sfill)
        fill_edge = 0.0
        if matched > 0 and binfo.get("avg_px") and sinfo.get("avg_px"):
            fill_edge = matched * (sinfo["avg_px"] * (1 - plan.sell_fee)
                                   - binfo["avg_px"] * (1 + plan.buy_fee))
            self.total_fill_edge += fill_edge
        log.info("[SETTLED] %s: buy %s %s %.6g/%.6g | sell %s %s %.6g/%.6g | "
                 "matched %.6g | fill edge $%.4f", direction,
                 buy.name, binfo["status"], bfill, plan.qty,
                 sell.name, sinfo["status"], sfill, plan.qty, matched, fill_edge)
        buy.last_traded_ts = sell.last_traded_ts = time.time()

        unresolved = binfo.get("unresolved") or sinfo.get("unresolved")
        hard_err = (binfo.get("err") is not None
                    or sinfo.get("err") is not None)
        rate_limited = False
        for v, info in ((buy, binfo), (sell, sinfo)):
            if str(info.get("err", "")).startswith("RATE_LIMITED"):
                rate_limited = True
                self._mark_limited(v)
            elif "margin" in str(info.get("status", "")).lower():
                log.warning("[%s] margin rejection — collateral exhausted, "
                            "pausing venue", v.name)
                self._mark_limited(v)
        # A one-leg fill is an execution failure even when both venue calls
        # returned normally.  Treating filled/canceled as success resets the
        # error counter and lets the strategy churn emergency hedges forever.
        # _execute_locked() still calls _maybe_hedge() after this method, so
        # the exposure is reduced before a configured halt takes effect.
        asymmetric_fill = abs(bfill - sfill) > cfg.net_tolerance_base
        sent_ok = not hard_err and not unresolved and not asymmetric_fill
        if asymmetric_fill:
            log.error("[ASYMMETRIC FILL] buy %.6g / sell %.6g — "
                      "counting as execution failure", bfill, sfill)
        completed_trade = sent_ok and matched > cfg.net_tolerance_base
        if sent_ok:
            self.consec_errors = 0
        elif not rate_limited:
            self.consec_errors += 1
            if self.consec_errors >= cfg.max_consecutive_errors:
                self.halted = True
                log.critical("HALTED after %d consecutive execution problems "
                             "— flatten manually and restart / 连续执行异常，"
                             "引擎已停止，请手动平仓后重启", self.consec_errors)
        if completed_trade:
            self.trades += 1
            self.total_exp_edge += plan.exp_edge_usd * matched / plan.qty
        self._add_daily_volume(
            (bfill * (binfo.get("avg_px") or plan.buy_limit))
            + (sfill * (sinfo.get("avg_px") or plan.sell_limit)))
        self._record_trade(direction, plan,
                           None if unresolved else fill_edge,
                           f"{binfo['status']}/{sinfo['status']}",
                           completed_trade)
        self._log_csv(direction, buy, sell, plan, completed_trade, bfill, sfill,
                      binfo["status"], sinfo["status"], fill_edge, inv_bps)
        self.last_trade_ts = time.time()
        return bool(unresolved)

    def _record_trade(self, direction: str, plan: ArbPlan, fill_edge,
                      status: str, ok: bool) -> None:
        self.recent_trades.append({
            "ts": time.time(), "direction": direction, "qty": plan.qty,
            "notional": plan.buy_notional,
            "prem_bps": plan.marginal_premium_bps,
            "exp": plan.exp_edge_usd, "fill": fill_edge, "status": status,
            "ok": ok})

    async def _maybe_hedge(self) -> None:
        net = sum(v.position for v in self.venues.values())
        if abs(net) > self.cfg.net_tolerance_base:
            await self._hedge(net)

    async def _hedge(self, net: float) -> None:
        """Reduce the venue that carries the imbalance back toward net zero
        (reduce-only taker with hedge_slippage_bps price protection)."""
        cfg = self.cfg
        is_sell = net > 0
        sgn = 1.0 if net > 0 else -1.0
        slip = cfg.hedge_slippage_bps / 1e4
        for v in sorted(self.venues.values(),
                        key=lambda x: (self._venue_limited(x), -x.position * sgn)):
            if v.position * sgn <= 0:
                continue
            if v.key in self._venue_down \
                    or not v.book.is_fresh(cfg.staleness_sec):
                continue  # unreachable or blind: cannot hedge here
            lk = self._vlock(v.key)
            if lk.locked():
                continue
            qty = floor_step(min(abs(net), abs(v.position)), self._step)
            if qty < v.min_base:
                continue
            ref = v.book.best_bid() if is_sell else v.book.best_ask()
            if ref is None:
                continue
            limit = v.px_round(ref * (1 - slip), False) if is_sell \
                else v.px_round(ref * (1 + slip), True)
            if qty * limit < max(cfg.min_order_notional, v.min_quote):
                continue
            await lk.acquire()  # verified free, no awaits since: fast path
            try:
                log.warning("[HEDGE] net %+.6g — %s %.6g on %s @%.6g",
                            net, "SELL" if is_sell else "BUY", qty, v.name, limit)
                self.hedges += 1
                self._record_send(v)  # counts toward the budget, never blocked
                info = await v.send_taker(is_buy=not is_sell, qty=qty,
                                          limit_px=limit, reduce_only=True)
                if info.get("err") or info.get("unresolved"):
                    log.error("[HEDGE] %s: %s", v.name,
                              info.get("err") or "unresolved")
                    if str(info.get("err", "")).startswith("RATE_LIMITED"):
                        self._mark_limited(v)
                    self._reconcile_evt.set()
                else:
                    fill = info["filled_base"]
                    v.position += -fill if is_sell else fill
                    if fill:
                        px = info.get("avg_px") or limit
                        fee = v.fee_bps / 1e4
                        v.cash += fill * px * (1 - fee) if is_sell \
                            else -fill * px * (1 + fee)
                        v.volume_usd += fill * px
                        self._add_daily_volume(fill * px)
                    log.info("[HEDGE SETTLED] %s %s %.6g/%.6g",
                             v.name, info["status"], fill, qty)
                v.last_traded_ts = time.time()
            finally:
                lk.release()
            return
        log.warning("[HEDGE] net %+.6g below hedgeable minimum — carrying "
                    "(next reconcile retries)", net)

    # --------------------------------------------------- reconcile / status

    # Lighter's REST account state lags its ws settlements; overwriting a
    # venue that traded seconds ago "restores" stale positions and triggers
    # phantom hedge oscillations. Grace-guard + venue lock prevent that.
    RECONCILE_GRACE_SEC = 5.0

    async def _reconcile_positions(self, hedge: bool,
                                   strict: bool = False) -> None:
        now = time.time()
        vs = []
        for v in self.venues.values():
            if now - v.last_traded_ts <= self.RECONCILE_GRACE_SEC:
                continue  # just traded: chain read would be stale
            if v.key in self._venue_down \
                    and now < self._venue_probe_at.get(v.key, 0.0):
                continue  # down venue: probe only every venue_probe_sec
            vs.append(v)
        if not vs:
            return
        got = await asyncio.gather(
            *(self._reconcile_venue(v, strict) for v in vs),
            return_exceptions=True)
        for r in got:
            if isinstance(r, BaseException):
                raise r  # strict startup: fail loudly
        if hedge:
            await self._maybe_hedge()

    async def _reconcile_venue(self, v, strict: bool) -> None:
        async with self._vlock(v.key):
            now = time.time()
            if now - v.last_traded_ts <= self.RECONCILE_GRACE_SEC:
                return  # traded while waiting for the lock
            try:
                r = await v.fetch_position()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if strict:
                    raise RuntimeError(
                        f"[{v.name}] cannot fetch starting position: {e!r}")
                # exchange unreachable (e.g. scheduled maintenance): pause
                # trading and keep probing until it answers again
                n = self._venue_fetch_fails.get(v.key, 0) + 1
                self._venue_fetch_fails[v.key] = n
                self._venue_probe_at[v.key] = now + self.cfg.venue_probe_sec
                if n >= 3 and v.key not in self._venue_down:
                    self._venue_down[v.key] = now
                    log.critical("[%s] API unreachable (%d attempts) — "
                                 "trading PAUSED; probing every %.0fs until "
                                 "it recovers", v.name, n,
                                 self.cfg.venue_probe_sec)
                elif v.key not in self._venue_down:
                    log.warning("[%s] position fetch failed (%d): %r",
                                v.name, n, e)
                return
            if v.key in self._venue_down:
                log.warning("[%s] API recovered after %.0fs outage — "
                            "trading RESUMED", v.name,
                            now - self._venue_down.pop(v.key))
                self._update_evt.set()
            self._venue_fetch_fails[v.key] = 0
            delta = r - v.position
            if abs(delta) > 1e-12:
                if abs(delta) > self.cfg.net_tolerance_base:
                    log.warning("[%s] reconcile: chain %+.6g vs local %+.6g "
                                "— adopting chain", v.name, r, v.position)
                mid = v.book.mid()
                if mid is not None:
                    v.cash -= delta * mid
                v.position = r

    async def _reconcile_loop(self) -> None:
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self._reconcile_evt.wait(),
                                       timeout=self.cfg.reconcile_sec)
                self._reconcile_evt.clear()
                await asyncio.sleep(1.0)
            except asyncio.TimeoutError:
                pass
            if self.stop.is_set():
                break
            try:
                await self._reconcile_positions(hedge=True)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("reconcile failed")

    async def _balance_loop(self) -> None:
        while not self.stop.is_set():
            await self._refresh_daily_risk()
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=BALANCE_POLL_SEC)
            except asyncio.TimeoutError:
                pass

    @staticmethod
    def _utc_day() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _save_daily_risk(self) -> None:
        path = self.cfg.daily_risk_state_file
        if not path:
            return
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w") as fh:
            json.dump({"utc_day": self.daily_risk_day,
                       "equity_baseline": self.daily_equity_baseline,
                       "volume_usd": self.daily_volume_usd,
                       "profit_only": self.profit_only}, fh)
        os.replace(tmp, path)

    def _apply_risk_gate(self) -> None:
        loss_hit = (self.cfg.daily_loss_limit_usd > 0
                    and self.daily_pnl_usd is not None
                    and self.daily_pnl_usd <= -self.cfg.daily_loss_limit_usd)
        volume_hit = (self.cfg.daily_volume_limit_usd > 0
                      and self.daily_volume_usd
                      >= self.cfg.daily_volume_limit_usd)
        profit_only_hit = (loss_hit
                           and self.cfg.daily_loss_action == "profit_only")
        hard_hit = volume_hit or (loss_hit and not profit_only_hit)
        if profit_only_hit and not self.profit_only:
            self.profit_only = True
            self._save_daily_risk()
            log.critical("[DAILY RISK] PROFIT-ONLY latched: pnl %s / -$%.2f; "
                         "new strategy trades require >= %.2fbps after "
                         "fees and execution budget",
                         (f"${self.daily_pnl_usd:+.4f}"
                          if self.daily_pnl_usd is not None else "—"),
                         self.cfg.daily_loss_limit_usd,
                         self.cfg.profit_only_min_bps)
        if hard_hit and not self.risk_limited:
            self.risk_limited = True
            log.critical("[DAILY RISK] close-only: pnl %s / -$%.2f, "
                         "volume $%.2f / $%.2f",
                         (f"${self.daily_pnl_usd:+.4f}"
                          if self.daily_pnl_usd is not None else "—"),
                         self.cfg.daily_loss_limit_usd,
                         self.daily_volume_usd,
                         self.cfg.daily_volume_limit_usd)
        flat = (abs(self.entropy.position) <= self.cfg.net_tolerance_base
                and abs(self.hedge.position) <= self.cfg.net_tolerance_base)
        if hard_hit and flat and not self._risk_halted:
            self.halted = True
            self._risk_halted = True
            log.critical("[DAILY RISK] HALTED flat; resumes on next UTC day "
                         "or process restart after state reset")

    def _add_daily_volume(self, usd: float) -> None:
        if usd <= 0:
            return
        self.daily_volume_usd += usd
        self._save_daily_risk()
        self._apply_risk_gate()

    async def _refresh_daily_risk(self, startup: bool = False) -> None:
        day = self._utc_day()
        vals = await asyncio.gather(
            *(v.fetch_equity() for v in self.venues.values()),
            return_exceptions=True)
        total = 0.0
        valid = True
        for v, got in zip(self.venues.values(), vals):
            if isinstance(got, BaseException) or got is None:
                valid = False
                if isinstance(got, BaseException):
                    log.debug("[%s] equity poll failed: %r", v.name, got)
                continue
            v.equity, v.free = got
            if v.start_equity is None:
                v.start_equity = v.equity
            total += v.equity
        if startup:
            try:
                with open(self.cfg.daily_risk_state_file) as fh:
                    saved = json.load(fh)
            except (OSError, ValueError, TypeError):
                saved = {}
            if saved.get("utc_day") == day:
                self.daily_risk_day = day
                self.daily_equity_baseline = float(saved["equity_baseline"])
                self.daily_volume_usd = float(saved.get("volume_usd", 0.0))
                self.profit_only = (
                    self.cfg.daily_loss_action == "profit_only"
                    and bool(saved.get("profit_only", False)))
        if self.daily_risk_day != day:
            self.daily_risk_day = day
            if valid:
                self.daily_equity_baseline = total
            self.daily_volume_usd = 0.0
            self.daily_pnl_usd = 0.0 if valid else None
            self.profit_only = False
            self.risk_limited = False
            if self._risk_halted:
                self.halted = False
                self._risk_halted = False
            self._save_daily_risk()
            log.warning("[DAILY RISK] new UTC day %s — counters reset", day)
        if valid and self.daily_equity_baseline is not None:
            self.daily_pnl_usd = total - self.daily_equity_baseline
            self._save_daily_risk()
        self._apply_risk_gate()

    async def _http_keepalive_loop(self) -> None:
        if self.cfg.http_keepalive_sec <= 0:
            return
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self.stop.wait(),
                                       timeout=self.cfg.http_keepalive_sec)
                return
            except asyncio.TimeoutError:
                pass
            await asyncio.gather(*(v.warm_http() for v in self.venues.values()),
                                 return_exceptions=True)

    def account_delta(self) -> Optional[float]:
        """Change in real account equity since start (both venues)."""
        total = 0.0
        for v in self.venues.values():
            if v.equity is None or v.start_equity is None:
                return None
            total += v.equity - v.start_equity
        return total

    def session_pnl(self) -> Optional[float]:
        total = 0.0
        for v in self.venues.values():
            m = v.book.mid()
            if m is None:
                return None
            total += v.cash + v.position * m
        if self._mtm_baseline is None:
            self._mtm_baseline = total
        return total - self._mtm_baseline

    def premium_bps(self) -> Optional[float]:
        em, hm = self.entropy.book.mid(), self.hedge.book.mid()
        if not (em and hm):
            return None
        return (em / hm - 1.0) * 1e4

    async def _status_loop(self) -> None:
        cfg = self.cfg
        while not self.stop.is_set():
            try:
                await asyncio.sleep(cfg.status_interval_sec)
            except asyncio.CancelledError:
                raise
            books = " | ".join(
                f"{v.name} {v.book.best_bid() or '—'}/{v.book.best_ask() or '—'}"
                + ("" if v.book.is_fresh(cfg.staleness_sec) else " STALE")
                + (" RATE-LTD" if self._venue_limited(v) else "")
                + (" DOWN" if v.key in self._venue_down else "")
                for v in self.venues.values())
            prem = self.premium_bps()
            prem_s = f"{prem:+.2f}" if prem is not None else "—"
            pos = " ".join(f"{v.name} {v.position:+.6g}"
                           for v in self.venues.values())
            net = sum(v.position for v in self.venues.values())
            pnl = self.session_pnl()
            rec = (f" | rec {self.recorder.rows_written} rows"
                   if self.recorder else "")
            mode = (" *** HALTED ***" if self.halted else
                    " *** CLOSE-ONLY ***" if self.risk_limited else
                    " *** PROFIT-ONLY ***" if self.profit_only else "")
            log.info("[status] %s | prem %s bps (band %+.2f..%+.2f) | pos %s "
                     "net %+.6g | trades %d hedges %d | MTM %s expEdge $%.4f "
                     "fillEdge $%.4f%s%s",
                     books, prem_s, cfg.midline_bps - cfg.lower_bps,
                     cfg.midline_bps + cfg.upper_bps, pos, net, self.trades,
                     self.hedges,
                     f"${pnl:+.4f}" if pnl is not None else "—",
                     self.total_exp_edge, self.total_fill_edge, rec, mode)

    def _log_csv(self, direction, buy, sell, plan: ArbPlan, ok: bool, bfill,
                 sfill, bstatus, sstatus, fill_edge, inv_bps) -> None:
        try:
            path = self.cfg.trades_csv
            d = os.path.dirname(path)
            if d:
                os.makedirs(d, exist_ok=True)
            if os.path.exists(path):
                with open(path) as fh0:
                    if fh0.readline().strip() != ",".join(CSV_HEADER):
                        os.replace(path, path + ".old")
            new = not os.path.exists(path)
            with open(path, "a", newline="") as fh:
                w = csv.writer(fh)
                if new:
                    w.writerow(CSV_HEADER)
                w.writerow([f"{time.time():.3f}",
                            direction, buy.name, sell.name, f"{plan.qty:.8g}",
                            plan.buy_limit, plan.sell_limit,
                            f"{plan.buy_notional:.2f}", f"{plan.sell_notional:.2f}",
                            f"{plan.exp_edge_usd:.4f}", f"{plan.gross_edge_usd:.4f}",
                            f"{plan.marginal_premium_bps:.3f}",
                            f"{self.cfg.midline_bps:.3f}",
                            f"{inv_bps:.3f}", int(ok), f"{bfill:.8g}",
                            f"{sfill:.8g}", bstatus, sstatus, f"{fill_edge:.4f}"])
        except Exception:
            log.exception("csv write failed")
