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
