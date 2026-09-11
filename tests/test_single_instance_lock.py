import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from main import SingleInstanceLock, live_lock_path  # noqa: E402


def test_second_live_lock_is_rejected_until_first_releases(tmp_path):
    path = str(tmp_path / ".entropy-arb-SNDK-arcus.lock")
    first = SingleInstanceLock(path)
    second = SingleInstanceLock(path)

    first.acquire()
    try:
        with pytest.raises(RuntimeError, match="already running"):
            second.acquire()
    finally:
        first.release()

    second.acquire()
    second.release()


def test_live_lock_path_is_shared_for_relative_and_absolute_config(tmp_path,
                                                                    monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert live_lock_path("config.yaml", "sndk", "ARCUS") == (
        live_lock_path(str(tmp_path / "config.yaml"), "SNDK", "arcus"))
