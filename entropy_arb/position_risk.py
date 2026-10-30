"""Read-only position telemetry and an opt-in entry gate, never order repair."""
import math
import time


def number(value, *, positive=False):
    if value is None or isinstance(value, bool):
        raise ValueError("missing numeric risk field")
    value = float(value)
    if not math.isfinite(value) or (positive and value <= 0):
        raise ValueError("invalid numeric risk field")
    return value


def parse_position_risk(state, coin, contexts, now=None):
    now = time.time() if now is None else now
    rows = state.get("assetPositions")
    if not isinstance(rows, list):
        raise ValueError("missing assetPositions")
    source_time = number(state.get("time"), positive=True) / 1000
    if not 0 <= now - source_time <= 15:
        raise ValueError("stale account risk snapshot")
    matches = [r["position"] for r in rows
               if isinstance(r, dict) and isinstance(r.get("position"), dict)
               and r["position"].get("coin") == coin]
    if len(matches) > 1:
        raise ValueError("duplicate position")
    result = {"observed_at": source_time, "position": 0.0,
              "mode": None, "leverage": None, "margin_used": None,
              "liquidation_px": None, "mark_px": None, "distance_pct": None}
    if not matches:
        return result  # flat is not evidence of adequate margin for a new trade
    pos = matches[0]
    size = number(pos.get("szi"))
    result["position"] = size
    if size == 0:
        return result
    lev = pos.get("leverage") or {}
    result.update(mode=lev.get("type"), leverage=number(lev.get("value"), positive=True),
                  margin_used=number(pos.get("marginUsed")),
                  liquidation_px=number(pos.get("liquidationPx"), positive=True))
    if result["mode"] not in ("isolated", "cross") or result["margin_used"] < 0:
        raise ValueError("invalid margin metadata")
    meta, ctxs = contexts
    indices = [i for i, row in enumerate(meta["universe"]) if row.get("name") == coin]
    if len(indices) != 1:
        raise ValueError("mark price identity missing")
    mark = number(ctxs[indices[0]].get("markPx"), positive=True)
    result["mark_px"] = mark
    # Signed: negative means already at/beyond the reported liquidation level.
    result["distance_pct"] = ((mark - result["liquidation_px"]) if size > 0
                              else (result["liquidation_px"] - mark)) / mark * 100
    return result


def entry_block_reason(snapshot, error, local_position, is_buy, qty,
                       min_distance_pct, max_age_sec, now=None):
    now = time.time() if now is None else now
    if error or not snapshot:
        return "position-risk-unavailable"
    if not 0 <= now - snapshot["observed_at"] <= max_age_sec:
        return "position-risk-stale"
    position = snapshot["position"]
    if abs(position - local_position) > 1e-10:
        return "position-risk-position-changed"
    delta = qty if is_buy else -qty
    # Never allow a nominal reduction to cross through zero into a new position.
    if position * delta < 0 and qty <= abs(position) + 1e-10:
        return None
    if position == 0:
        return "position-risk-flat-unverified"  # no hypothetical margin model yet
    if snapshot["mode"] != "isolated":
        return "position-risk-mode-unverified"
    if snapshot["distance_pct"] <= min_distance_pct:
        return "position-risk-distance"
    return None


def parse_entry_evidence(active, meta, account, coin, started_at):
    """Only isolated, single-tier markets are modeled. Never guess a tier table."""
    if active.get("coin") != coin or str(active.get("user", "")).lower() != account.lower():
        raise ValueError("active asset identity mismatch")
    lev = active.get("leverage") or {}
    if lev.get("type") != "isolated":
        raise ValueError("only isolated entry projection supported")
    leverage = number(lev.get("value"), positive=True)
    if leverage != int(leverage):
        raise ValueError("non-integer leverage")
    markets = [m for m in meta["universe"] if m.get("name") == coin]
    if len(markets) != 1 or meta.get("collateralToken") != 0:
        raise ValueError("market identity or USDC collateral unverified")
    market = markets[0]
    table_id = market.get("marginTableId")
    tables = [table for ident, table in meta.get("marginTables", []) if ident == table_id]
    if len(tables) > 1:
        raise ValueError("duplicate margin table")
    if tables:
        tiers = tables[0].get("marginTiers")
        if not isinstance(tiers, list) or len(tiers) != 1 or number(tiers[0].get("lowerBound")) != 0:
            raise ValueError("tiered margin projection unsupported")
        maximum = number(tiers[0].get("maxLeverage"), positive=True)
    elif type(table_id) is int and 0 < table_id < 50:
        # Official margin-tiers specification: IDs below 50 encode a single
        # tier whose max leverage equals its ID (not necessarily in the array).
        maximum = float(table_id)
    else:
        raise ValueError("margin table missing")
    if maximum != number(market.get("maxLeverage"), positive=True) or leverage > maximum:
        raise ValueError("leverage/tier mismatch")
    limits = [number(x) for x in active["maxTradeSzs"]]
    funds = [number(x) for x in active["availableToTrade"]]
    if len(limits) != 2 or len(funds) != 2 or min(limits + funds) < 0:
        raise ValueError("invalid directional capacity")
    return {"observed_at": number(started_at, positive=True), "leverage": leverage,
            "mark_px": number(active.get("markPx"), positive=True),
            "raw_usd": number(lev.get("rawUsd")), "maintenance_rate": 1 / (2 * maximum),
            "max_sizes": limits, "available_margin": funds}


def project_entry(snapshot, evidence, is_buy, qty, worst_px, fee_bps,
                  min_distance_pct, max_age_sec, now=None):
    """Conservative single-tier estimate, NOT a venue fill or margin guarantee.

    Flat: credit only minimum initial collateral, never the whole wallet.
    Addition: reconstruct existing equity from the reported liquidation price;
    credit ZERO new collateral (a conservative lower bound), subtract adverse
    fill-vs-mark PnL and reserve entry/exit fees. Never models position flips.
    """
    now = time.time() if now is None else now
    try:
        if not evidence or not 0 <= now - evidence["observed_at"] <= max_age_sec:
            raise ValueError("entry evidence missing/stale")
        if not snapshot or not 0 <= now - snapshot["observed_at"] <= max_age_sec:
            raise ValueError("position evidence missing/stale")
        qty, px = number(qty, positive=True), number(worst_px, positive=True)
        fee = number(fee_bps) / 1e4
        if fee < 0:
            raise ValueError("negative fee reserve")
        position = number(snapshot["position"])
        side = 1 if is_buy else -1
        if position * side < 0:
            return {"reason": "position-risk-flip-unmodeled"}
        idx = 0 if is_buy else 1
        mark = evidence["mark_px"] if position == 0 else (
            min(snapshot["mark_px"], evidence["mark_px"]) if is_buy else
            max(snapshot["mark_px"], evidence["mark_px"]))
        mark = number(mark, positive=True)
        rate = number(evidence["maintenance_rate"], positive=True)
        leverage = number(evidence["leverage"], positive=True)
        if rate >= 1:
            raise ValueError("invalid maintenance rate")
        if qty > evidence["max_sizes"][idx] + 1e-10:
            return {"reason": "position-risk-order-capacity"}
        fees = qty * px * fee
        required = qty * max(mark, px) / leverage + fees
        if required > evidence["available_margin"][idx]:
            return {"reason": "position-risk-insufficient-margin"}
        if position == 0:
            if abs(evidence["raw_usd"]) > 1e-10:
                raise ValueError("unexpected flat isolated cash")
            equity = qty * min(mark, px) / leverage
        else:
            if snapshot["leverage"] != leverage:
                raise ValueError("leverage changed")
            liquidation = number(snapshot["liquidation_px"], positive=True)
            equity = abs(position) * (side * (mark - liquidation) + rate * liquidation)
        total_size = abs(position) + qty
        # Do not credit favorable execution PnL; reserve both entry and exit.
        equity -= max(0.0, qty * side * (px - mark)) + fees + total_size * max(mark, px) * fee
        liq = (side * total_size * mark - equity) / (total_size * (side - rate))
        distance = side * (mark - liq) / mark * 100
        if not all(math.isfinite(v) for v in (equity, liq, distance)) or liq <= 0:
            raise ValueError("unmodeled liquidation price")
        return {"reason": "position-risk-projected-distance" if distance <= min_distance_pct else None,
                "estimated_liquidation_px": liq, "estimated_distance_pct": distance,
                "initial_margin_required": required, "equity_lower_bound": equity}
    except (ValueError, TypeError, KeyError, ZeroDivisionError, OverflowError) as exc:
        return {"reason": "position-risk-entry-unavailable", "error": str(exc)}
