#!/usr/bin/env python3
"""Read-only public-account risk monitor. Never starts Engine or loads secrets."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from entropy_arb.position_risk import parse_position_risk, parse_entry_evidence, project_entry


API_URL = "https://api.hyperliquid.xyz/info"


def query(payload):
    # Fixed endpoint and allowlist: signing/exchange/order calls are not exposed.
    if payload.get("type") not in ("clearinghouseState", "metaAndAssetCtxs", "activeAssetData"):
        raise ValueError("read-only info requests only")
    request = urllib.request.Request(
        API_URL, data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def collect(account, coin, dex, request=query, include_entry=False):
    started = time.time()
    state = request({"type": "clearinghouseState", "user": account, "dex": dex})
    contexts = None
    positions = state.get("assetPositions")
    if not isinstance(positions, list):
        raise ValueError("missing assetPositions")
    if include_entry or any((row.get("position") or {}).get("coin") == coin for row in positions):
        contexts = request({"type": "metaAndAssetCtxs", "dex": dex})
    snapshot = parse_position_risk(state, coin, contexts)
    if include_entry:
        try:
            active = request({"type": "activeAssetData", "user": account, "coin": coin})
            snapshot["entry"] = parse_entry_evidence(active, contexts[0], account, coin, started)
        except Exception as exc:
            snapshot["entry_error"] = str(exc)
    snapshot["status"] = "FLAT" if snapshot["position"] == 0 else "OBSERVED"
    return snapshot


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account", required=True, help="public trading account, NOT agent signer")
    parser.add_argument("--coin", default="io:SNDK")
    parser.add_argument("--dex", default="io")
    parser.add_argument("--interval", type=float, default=15.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--preflight", action="store_true", help="hypothetical buy/sell estimates only; NEVER places orders")
    parser.add_argument("--quantity", type=float)
    parser.add_argument("--fee-bps", type=float)
    parser.add_argument("--slippage-bps", type=float)
    parser.add_argument("--min-distance-pct", type=float)
    args = parser.parse_args(argv)
    if (len(args.account) != 42 or not args.account.startswith("0x")
            or any(c not in "0123456789abcdefABCDEF" for c in args.account[2:])):
        parser.error("--account must be a public 0x address (42 characters)")
    if not 5 <= args.interval <= 3600:
        parser.error("--interval must be between 5 and 3600 seconds")
    if not args.dex or not args.coin.startswith(args.dex + ":"):
        parser.error("--coin must belong to --dex, e.g. io:SNDK / io")
    if args.preflight:
        import math
        if any(v is None or not math.isfinite(v) for v in (
                args.quantity, args.fee_bps, args.slippage_bps, args.min_distance_pct)):
            parser.error("preflight requires finite --quantity --fee-bps --slippage-bps --min-distance-pct")
        if not (args.quantity > 0 and args.fee_bps >= 0 and 0 <= args.slippage_bps < 10000
                and 0 < args.min_distance_pct < 100):
            parser.error("invalid preflight size, fee, slippage or distance")
    print("READ-ONLY / 仅查询公开账户；不下单、不读私钥、不修改配置或核验锁。", flush=True)
    try:
        while True:
            try:
                result = (collect(args.account, args.coin, args.dex, include_entry=True) if args.preflight
                          else collect(args.account, args.coin, args.dex))
                if args.preflight:
                    entry = result.get("entry")
                    result["hypothetical_preflight"] = {}
                    for buy in (True, False):
                        mark = (entry or {}).get("mark_px", 0)
                        limit = mark * (1 + args.slippage_bps / 1e4 if buy else 1 - args.slippage_bps / 1e4)
                        result["hypothetical_preflight"]["buy" if buy else "sell"] = project_entry(
                            result, entry, buy, args.quantity, limit, args.fee_bps,
                            args.min_distance_pct, 15.0)
            except Exception as exc:
                # No cached position is ever presented as a current safe result.
                result = {"status": "UNKNOWN", "error": str(exc)}
            result.update(utc=datetime.now(timezone.utc).isoformat(), coin=args.coin)
            print(json.dumps(result, ensure_ascii=False, allow_nan=False), flush=True)
            if args.once:
                return 1 if result["status"] == "UNKNOWN" or (args.preflight and not result.get("entry")) else 0
            time.sleep(args.interval)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
