"""Arcus adapter signing, book parsing, and settlement without live orders."""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from entropy_arb.book import OrderBook
from entropy_arb.venue_arcus import (  # noqa: E402
    ArcusOrdersFeed, _decimal_string, _levels, _load_private_key,
    _public_key_hex, _to_units,
)


def test_exact_units_and_level_shapes():
    assert _to_units("600000.2", "0.1") == 6000002
    assert _to_units("0.051", "0.001") == 51
    assert _decimal_string(100.0) == "100"
    assert _decimal_string("0.0000100") == "0.00001"
    try:
        _to_units("1.01", "0.25")
    except ValueError:
        pass
    else:
        raise AssertionError("non-grid value must be rejected")
    contents = {
        "bids": [["100.1", "2"], ["100", "0"]],
        "asks": [{"price": "100.2", "size": "3"}],
    }
    assert _levels(contents, "bids") == {100.1: 2.0}
    assert _levels(contents, "asks") == {100.2: 3.0}


def test_raw_seed_load_and_sign():
    original = Ed25519PrivateKey.generate()
    seed = original.private_bytes_raw().hex()
    loaded = _load_private_key(seed, None)
    assert _public_key_hex(loaded) == _public_key_hex(original)
    message = b"arcus-signature-test"
    original.public_key().verify(loaded.sign(message), message)


def test_order_feed_resolves_partial_ioc():
    async def go():
        feed = ArcusOrdersFeed("ARCUS", "ws://unused",
                               "0x" + "1" * 40, 0, "BTC-USD")
        fut = feed.watch("entropy1")
        feed._handle({
            "clientId": "entropy1", "orderId": "o1",
            "state": "PARTIALLY_FILLED", "originalSize": "1.5",
            "remainingSize": "0.4", "avgFillPrice": "99.5",
        })
        result = await fut
        assert result == {"status": "partially_filled", "filled_base": 1.1,
                          "avg_px": 99.5}

    asyncio.run(go())
