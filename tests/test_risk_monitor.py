import importlib.util
from pathlib import Path
import time

import pytest

spec = importlib.util.spec_from_file_location(
    "risk_monitor", Path(__file__).resolve().parents[1] / "tools" / "monitor_position_risk.py")
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)
ACCOUNT = "0x" + "1" * 40


def test_flat_monitor_only_queries_public_state():
    calls = []
    def request(payload):
        calls.append(payload)
        return {"time": time.time() * 1000, "assetPositions": []}
    result = monitor.collect(ACCOUNT, "io:SNDK", "io", request)
    assert result["status"] == "FLAT"
    assert result["liquidation_px"] is None
    assert calls == [{"type": "clearinghouseState", "user": ACCOUNT, "dex": "io"}]


def test_position_monitor_uses_mark_not_mid():
    calls = []
    def request(payload):
        calls.append(payload)
        if payload["type"] == "metaAndAssetCtxs":
            return [{"universe": [{"name": "io:SNDK"}]}, [{"markPx": "100"}]]
        return {"time": time.time() * 1000, "assetPositions": [{"position": {
            "coin": "io:SNDK", "szi": "1", "marginUsed": "10", "liquidationPx": "90",
            "leverage": {"type": "isolated", "value": 15}}}]}
    result = monitor.collect(ACCOUNT, "io:SNDK", "io", request)
    assert result["status"] == "OBSERVED"
    assert result["distance_pct"] == 10
    assert all(c["type"] in ("clearinghouseState", "metaAndAssetCtxs") for c in calls)


def test_once_prints_unknown_and_nonzero_exit_on_error(monkeypatch, capsys):
    def fail(*args):
        raise ValueError("stale account risk snapshot")
    monkeypatch.setattr(monitor, "collect", fail)
    assert monitor.main(["--account", ACCOUNT, "--once"]) == 1
    output = capsys.readouterr().out
    assert '"status": "UNKNOWN"' in output
    assert '"position"' not in output


def test_once_success_does_not_write_review_lock(monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(tmp_path)
    lock = tmp_path / "position-review"
    lock.write_text("keep locked")
    monkeypatch.setattr(monitor, "collect", lambda *args: {"status": "FLAT", "position": 0})
    assert monitor.main(["--account", ACCOUNT, "--once"]) == 0
    assert lock.read_text() == "keep locked"
    assert list(tmp_path.iterdir()) == [lock]
    assert "FLAT" in capsys.readouterr().out


@pytest.mark.parametrize("extra", [["--interval", "nan"], ["--interval", "0"],
                                  ["--coin", "xyz:SNDK"]])
def test_monitor_rejects_invalid_inputs(extra):
    with pytest.raises(SystemExit):
        monitor.main(["--account", ACCOUNT, "--once", *extra])


def test_monitor_cannot_send_order_requests():
    with pytest.raises(ValueError, match="read-only"):
        monitor.query({"type": "order"})


def test_preflight_collects_only_three_public_info_types():
    from test_entry_projection import inputs
    active, meta, _ = inputs()
    active["user"] = ACCOUNT
    calls = []
    def request(payload):
        calls.append(payload)
        return {"clearinghouseState": {"time": time.time() * 1000, "assetPositions": []},
                "metaAndAssetCtxs": [meta, [{"markPx": "100"}]], "activeAssetData": active}[payload["type"]]
    result = monitor.collect(ACCOUNT, "io:SNDK", "io", request, include_entry=True)
    assert result["entry"]["leverage"] == 15
    assert result["status"] == "FLAT"
    assert [p["type"] for p in calls] == ["clearinghouseState", "metaAndAssetCtxs", "activeAssetData"]


def test_preflight_missing_inputs_cannot_start():
    with pytest.raises(SystemExit):
        monitor.main(["--account", ACCOUNT, "--preflight", "--once"])


def test_preflight_prints_both_estimates_without_orders(monkeypatch, capsys):
    from test_entry_projection import evidence
    snapshot, entry = evidence()
    snapshot.update(status="FLAT", entry=entry)
    monkeypatch.setattr(monitor, "collect", lambda *args, **kwargs: snapshot)
    assert monitor.main(["--account", ACCOUNT, "--once", "--preflight", "--quantity", ".04",
                         "--fee-bps", ".9", "--slippage-bps", "4", "--min-distance-pct", "3"]) == 0
    output = capsys.readouterr().out
    assert "hypothetical_preflight" in output and '"buy"' in output and '"sell"' in output


def test_preflight_missing_entry_evidence_nonzero_exit(monkeypatch, capsys):
    monkeypatch.setattr(monitor, "collect", lambda *args, **kwargs: {
        "status": "FLAT", "position": 0, "observed_at": time.time(), "entry_error": "offline"})
    assert monitor.main(["--account", ACCOUNT, "--once", "--preflight", "--quantity", ".04",
                         "--fee-bps", ".9", "--slippage-bps", "4", "--min-distance-pct", "3"]) == 1
    assert "position-risk-entry-unavailable" in capsys.readouterr().out
