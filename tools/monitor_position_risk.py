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
from entropy_arb.position_risk import parse_position_risk


API_URL = "https://api.hyperliquid.xyz/info"


def query(payload):
    # Fixed endpoint and allowlist: signing/exchange/order calls are not exposed.
    if payload.get("type") not in ("clearinghouseState", "metaAndAssetCtxs"):
        raise ValueError("read-only info requests only")
    request = urllib.request.Request(
        API_URL, data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def collect(account, coin, dex, request=query):
    state = request({"type": "clearinghouseState", "user": account, "dex": dex})
    contexts = None
    positions = state.get("assetPositions")
    if not isinstance(positions, list):
        raise ValueError("missing assetPositions")
    if any((row.get("position") or {}).get("coin") == coin for row in positions):
        contexts = request({"type": "metaAndAssetCtxs", "dex": dex})
    snapshot = parse_position_risk(state, coin, contexts)
    snapshot["status"] = "FLAT" if snapshot["position"] == 0 else "OBSERVED"
    return snapshot


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account", required=True, help="public trading account, NOT agent signer")
    parser.add_argument("--coin", default="io:SNDK")
    parser.add_argument("--dex", default="io")
    parser.add_argument("--interval", type=float, default=15.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    if (len(args.account) != 42 or not args.account.startswith("0x")
            or any(c not in "0123456789abcdefABCDEF" for c in args.account[2:])):
        parser.error("--account must be a public 0x address (42 characters)")
    if not 5 <= args.interval <= 3600:
        parser.error("--interval must be between 5 and 3600 seconds")
    if not args.dex or not args.coin.startswith(args.dex + ":"):
        parser.error("--coin must belong to --dex, e.g. io:SNDK / io")
    print("READ-ONLY / 仅查询公开账户；不下单、不读私钥、不修改配置或核验锁。", flush=True)
    try:
        while True:
            try:
                result = collect(args.account, args.coin, args.dex)
            except Exception as exc:
                # No cached position is ever presented as a current safe result.
                result = {"status": "UNKNOWN", "error": str(exc)}
            result.update(utc=datetime.now(timezone.utc).isoformat(), coin=args.coin)
            print(json.dumps(result, ensure_ascii=False, allow_nan=False), flush=True)
            if args.once:
                return 1 if result["status"] == "UNKNOWN" else 0
            time.sleep(args.interval)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
