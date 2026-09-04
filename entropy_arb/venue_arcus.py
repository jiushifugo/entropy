"""Arcus perpetuals venue adapter.

Public REST/WS feeds work without credentials for --record-only.  Live orders
use the Arcus Ed25519 typed-payload signature and settle from the public
account orders stream.  The Ethereum master private key is never needed.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from typing import Optional

import aiohttp

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:
    from websockets import connect as ws_connect  # type: ignore

from .book import OrderBook
from .config import VenueConf

log = logging.getLogger("arcus")
REST_TIMEOUT = 10.0
FINAL_STATES = {"FILLED", "CANCELED", "CANCELLED", "MARGIN_CANCELED",
                "REJECTED", "EXPIRED", "PARTIALLY_FILLED"}


def _decimal_places(value: str) -> int:
    return max(-Decimal(str(value)).as_tuple().exponent, 0)


def _to_units(value, unit: str) -> int:
    q = Decimal(str(value)) / Decimal(str(unit))
    if q != q.to_integral_value():
        raise ValueError(f"{value} is not an exact multiple of {unit}")
    return int(q)


def _decimal_string(value) -> str:
    text = format(Decimal(str(value)), "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def _load_private_key(value: Optional[str], filename: Optional[str]):
    try:
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric.ed25519 import (
            Ed25519PrivateKey,
        )
    except ImportError as e:
        raise RuntimeError(
            "live trading on Arcus needs cryptography — "
            "pip install -r requirements-live.txt") from e

    raw = Path(filename).expanduser().read_bytes() if filename else (
        value or "").replace("\\n", "\n").encode()
    if not raw:
        raise ValueError("missing Arcus Ed25519 private key")
    if b"-----BEGIN" in raw:
        return serialization.load_pem_private_key(raw, password=None)
    compact = re.sub(rb"\s+", b"", raw)
    try:
        decoded = bytes.fromhex(compact.removeprefix(b"0x").decode())
    except (ValueError, UnicodeDecodeError):
        import base64
        decoded = base64.b64decode(compact, validate=True)
    if len(decoded) == 32:
        return Ed25519PrivateKey.from_private_bytes(decoded)
    return serialization.load_der_private_key(decoded, password=None)


def _public_key_hex(private_key) -> str:
    from cryptography.hazmat.primitives import serialization
    return private_key.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw).hex()


def _records(value):
    if isinstance(value, list):
        for item in value:
            yield from _records(item)
    elif isinstance(value, dict):
        if any(k in value for k in ("orderId", "clientId", "marketId")):
            yield value
        for child in value.values():
            if isinstance(child, (dict, list)):
                yield from _records(child)


def _levels(contents: dict, side: str):
    rows = contents.get(side) or []
    if isinstance(rows, dict):
        rows = rows.values()
    out = {}
    for row in rows:
        if isinstance(row, dict):
            px = row.get("price", row.get("px"))
            sz = row.get("size", row.get("sz", row.get("quantity")))
        elif isinstance(row, (list, tuple)) and len(row) >= 2:
            px, sz = row[0], row[1]
        else:
            continue
        try:
            px, sz = float(px), float(sz)
        except (TypeError, ValueError):
            continue
        if px > 0 and sz > 0:
            out[px] = sz
    return out


class ArcusBookFeed:
    def __init__(self, name: str, ws_url: str, market: str,
                 book: OrderBook, notify) -> None:
        self.name, self.ws_url, self.market = name, ws_url, market
        self.book, self.notify = book, notify

    async def run(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        while not stop.is_set():
            try:
                async with ws_connect(self.ws_url, max_size=2**23,
                                      open_timeout=10, ping_interval=15,
                                      ping_timeout=15) as ws:
                    self.book.clear()
                    await ws.send(json.dumps({
                        "type": "subscribe", "channel": "l2Orderbook",
                        "id": self.market, "nLevels": 100}))
                    async for raw in ws:
                        backoff = 1.0
                        msg = json.loads(raw)
                        self.book.touch()
                        if (msg.get("channel") == "l2Orderbook"
                                and msg.get("type") in
                                ("subscribed", "channel_data")):
                            c = msg.get("contents") or {}
                            bids, asks = _levels(c, "bids"), _levels(c, "asks")
                            if bids or asks:
                                self.book.bids, self.book.asks = bids, asks
                                self.book.ready = bool(bids and asks)
                                self.book.last_update_ts = time.time()
                                self.notify()
                        if stop.is_set():
                            break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("[%s] book ws error: %s — reconnect in %.0fs",
                            self.name, e, backoff)
            self.book.ready = False
            self.notify()
            if not stop.is_set():
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)


class ArcusOrdersFeed:
    def __init__(self, name: str, ws_url: str, address: str,
                 account_index: int, market: str) -> None:
        self.name, self.ws_url = name, ws_url
        self.address, self.account_index, self.market = (
            address, account_index, market)
        self.ready = asyncio.Event()
        self._pending: dict[str, asyncio.Future] = {}
        self._early: dict[str, dict] = {}

    def watch(self, client_id: str) -> asyncio.Future:
        fut = asyncio.get_running_loop().create_future()
        early = self._early.pop(client_id, None)
        if early is not None:
            fut.set_result(early)
        else:
            self._pending[client_id] = fut
        return fut

    def unwatch(self, client_id: str) -> None:
        fut = self._pending.pop(client_id, None)
        if fut is not None and not fut.done():
            fut.cancel()

    def _handle(self, contents) -> None:
        for row in _records(contents):
            state = str(row.get("state") or row.get("status") or "").upper()
            if state not in FINAL_STATES:
                continue
            key = str(row.get("clientId") or "")
            if not key:
                continue
            original = float(row.get("originalSize") or 0)
            remaining = float(row.get("remainingSize") or 0)
            info = {
                "status": state.lower(),
                "filled_base": max(original - remaining, 0.0),
                "avg_px": (float(row["avgFillPrice"])
                           if row.get("avgFillPrice") else None),
            }
            fut = self._pending.pop(key, None)
            if fut is not None and not fut.done():
                fut.set_result(info)
            else:
                self._early[key] = info
                if len(self._early) > 512:
                    self._early.pop(next(iter(self._early)))

    def _handle_message(self, msg: dict) -> None:
        if (msg.get("channel") != "orders"
                or msg.get("type") not in ("subscribed", "channel_data")):
            return
        if not self.ready.is_set():
            log.info("[%s] orders ws ready (%s account %d)",
                     self.name, self.address, self.account_index)
        # Some Arcus deployments send the initial channel_data snapshot
        # without a separate subscribed acknowledgement.  Receiving either
        # proves the private order stream is live and is sufficient for
        # settlement.
        self.ready.set()
        self._handle(msg.get("contents"))

    async def run(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        while not stop.is_set():
            try:
                async with ws_connect(self.ws_url, max_size=2**23,
                                      open_timeout=10, ping_interval=15,
                                      ping_timeout=15) as ws:
                    await ws.send(json.dumps({
                        "type": "subscribe", "channel": "orders",
                        "id": self.address, "accountIndex": self.account_index,
                        "market": self.market, "snapshot": True}))
                    async for raw in ws:
                        backoff = 1.0
                        msg = json.loads(raw)
                        self._handle_message(msg)
                        if stop.is_set():
                            break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("[%s] orders ws error: %s — reconnect in %.0fs",
                            self.name, e, backoff)
            self.ready.clear()
            if not stop.is_set():
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)


class ArcusVenue:
    kind = "arcus"

    def __init__(self, conf: VenueConf, session: aiohttp.ClientSession,
                 settle_timeout_sec: float) -> None:
        self.conf, self.key, self.name = conf, conf.key, conf.label
        self.session, self.settle_timeout = session, settle_timeout_sec
        self.api_url, self.ws_url = conf.arcus_api_url, conf.arcus_ws_url
        self.book = OrderBook()
        self.position = self.cash = self.volume_usd = 0.0
        self.equity = self.free = self.start_equity = None
        self.fee_bps, self.cap_usd = conf.fee_bps, conf.cap_usd
        self.orders_per_min, self.last_traded_ts = conf.orders_per_min, 0.0
        self.market_id = -1
        self.market = ""
        self.tick_size = "0"
        self.step_size = "0"
        self.tick_tiers = []
        self.size_decimals = 0
        self.min_base = 0.0
        self.min_quote = 5.0
        self.private_key = None
        self.orders_feed: Optional[ArcusOrdersFeed] = None
        self._client_seq = int(time.time() * 1_000_000)

    async def _get(self, path: str, params=None, auth: bool = False):
        headers = {}
        creds = self.conf.arcus_creds
        if auth and creds and creds.api_key:
            headers["X-API-Key"] = creds.api_key.removeprefix("0x")
        async with self.session.get(
                self.api_url + path, params=params, headers=headers,
                timeout=aiohttp.ClientTimeout(total=REST_TIMEOUT)) as r:
            text = await r.text()
            if r.status == 404:
                return None
            if r.status == 429:
                raise RuntimeError(f"RATE_LIMITED: Arcus HTTP 429 {text[:150]}")
            if r.status >= 400:
                raise RuntimeError(f"Arcus HTTP {r.status}: {text[:250]}")
            return json.loads(text) if text else {}

    async def load_market(self) -> None:
        data = await self._get("/v1/markets")
        wanted = self.conf.symbol.upper()
        aliases = {wanted, wanted + "-USD"}
        for row in (data or {}).get("markets", []):
            if (str(row.get("marketDisplayName", "")).upper() not in aliases
                    and str(row.get("baseAsset", "")).upper() != wanted):
                continue
            if row.get("type", "PERPETUAL") != "PERPETUAL":
                continue
            if row.get("status") != "ONLINE":
                raise RuntimeError(f"[{self.name}] market status={row.get('status')}")
            self.market_id = int(row["marketId"])
            self.market = str(row["marketDisplayName"])
            self.tick_size = str(row["tickSize"])
            self.step_size = str(row["stepSize"])
            self.tick_tiers = row.get("tickTiers") or []
            self.size_decimals = _decimal_places(str(row["stepSize"]))
            self.min_base = float(row.get("minOrderSize") or row["stepSize"])
            self.min_quote = float(row.get("minOrderNotional") or 5.0)
            log.info("[%s] %s market_id=%d tick=%s step=%s",
                     self.name, self.market, self.market_id, self.tick_size,
                     row["stepSize"])
            return
        raise RuntimeError(f"[{self.name}] {wanted}-USD not found")

    def init_signer(self) -> None:
        c = self.conf.arcus_creds
        assert c is not None and c.complete, f"[{self.name}] missing credentials"
        if not re.fullmatch(r"0x[0-9a-fA-F]{40}", c.address or ""):
            raise RuntimeError("ARCUS_ADDRESS must be a public Ethereum address")
        if c.account_index is None or not 0 <= c.account_index <= 9:
            raise RuntimeError("ARCUS_ACCOUNT_INDEX must be between 0 and 9")
        self.private_key = _load_private_key(
            c.api_private_key, c.api_private_key_file)
        derived = _public_key_hex(self.private_key)
        supplied = (c.api_key or "").removeprefix("0x").lower()
        if derived != supplied:
            raise RuntimeError("ARCUS_API_KEY does not match the private key")
        log.info("[%s] signer ready (address %s account %d)",
                 self.name, c.address, c.account_index)

    def start_tasks(self, stop: asyncio.Event, notify, live: bool) -> list:
        tasks = [asyncio.create_task(
            ArcusBookFeed(self.name, self.ws_url, self.market,
                          self.book, notify).run(stop),
            name=f"book-{self.key}")]
        if live:
            c = self.conf.arcus_creds
            self.orders_feed = ArcusOrdersFeed(
                self.name, self.ws_url, c.address.lower(),
                c.account_index, self.market)
            tasks.append(asyncio.create_task(self.orders_feed.run(stop),
                                             name=f"acct-{self.key}"))
        return tasks

    def ready_to_trade(self) -> bool:
        return (self.private_key is not None and self.orders_feed is not None
                and self.orders_feed.ready.is_set())

    async def warm_http(self) -> None:
        try:
            await self._get("/v1/time")
        except Exception as e:
            log.debug("[%s] keepalive failed: %r", self.name, e)

    def _active_tick(self, px: float) -> Decimal:
        for tier in self.tick_tiers:
            ceiling = tier.get("upToPrice")
            if ceiling is None or Decimal(str(px)) <= Decimal(str(ceiling)):
                return Decimal(str(tier.get("tick") or self.tick_size))
        return Decimal(self.tick_size)

    def px_round(self, px: float, round_up: bool) -> float:
        step = self._active_tick(px)
        value = Decimal(str(px)) / step
        mode = ROUND_CEILING if round_up else ROUND_FLOOR
        return float(value.to_integral_value(rounding=mode) * step)

    def _next_client_id(self) -> str:
        self._client_seq += 1
        return f"entropy{self._client_seq:x}"[-36:]

    async def send_taker(self, *, is_buy: bool, qty: float, limit_px: float,
                         reduce_only: bool = False) -> dict:
        assert self.private_key is not None and self.market_id >= 0
        c = self.conf.arcus_creds
        ts = time.time_ns()
        client_id = self._next_client_id()
        good_til_us = int(time.time() * 1_000_000) + 40 * 86400 * 1_000_000
        price = _decimal_string(limit_px)
        quantity = _decimal_string(qty)
        payload = {
            "ad": c.address.lower(), "ai": c.account_index, "c": client_id,
            "ct": ts, "g": good_til_us * 1000, "m": self.market_id, "op": 1,
            "p": _to_units(price, self.tick_size),
            "q": _to_units(quantity, self.step_size),
            "r": 1 if reduce_only else 0, "s": 0 if is_buy else 1,
            "t": 2, "v": 1,
        }
        message = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        signature = self.private_key.sign(message.encode()).hex()
        body = {
            "address": c.address.lower(), "accountIndex": c.account_index,
            "marketId": self.market_id,
            "orderSide": "BUY" if is_buy else "SELL",
            "orderType": "LIMIT", "quantity": quantity, "price": price,
            "timeInForce": "IOC", "goodTilTime": str(good_til_us),
            "reduceOnly": reduce_only, "clientId": client_id, "timestamp": ts,
        }
        fut = self.orders_feed.watch(client_id) if self.orders_feed else None
        headers = {
            "X-API-Key": c.api_key.removeprefix("0x"),
            "X-Timestamp": str(ts), "X-Signature": signature,
            "Content-Type": "application/json",
        }
        try:
            async with self.session.post(
                    self.api_url + "/v1/placeOrder",
                    params={"address": c.address.lower()}, json=body,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=REST_TIMEOUT)) as r:
                text = await r.text()
                if r.status == 429:
                    raise RuntimeError(f"RATE_LIMITED: Arcus HTTP 429 {text[:150]}")
                if r.status >= 400:
                    if fut is not None:
                        self.orders_feed.unwatch(client_id)
                    return {"status": "send-failed", "filled_base": 0.0,
                            "avg_px": None,
                            "err": f"HTTP {r.status}: {text[:250]}",
                            "unresolved": False}
        except RuntimeError as e:
            if fut is not None:
                self.orders_feed.unwatch(client_id)
            return {"status": "send-failed", "filled_base": 0.0,
                    "avg_px": None, "err": str(e), "unresolved": False}
        except (asyncio.TimeoutError, aiohttp.ClientError):
            # The request may already be accepted.  Keep the WS watch alive and
            # resolve by clientId instead of retrying a potentially live order.
            pass
        if fut is None:
            return {"status": "sent-unconfirmed", "filled_base": 0.0,
                    "avg_px": None, "err": None, "unresolved": True}
        try:
            info = await asyncio.wait_for(fut, timeout=self.settle_timeout)
            return {**info, "err": None, "unresolved": False}
        except asyncio.TimeoutError:
            self.orders_feed.unwatch(client_id)
            return {"status": "timeout", "filled_base": 0.0,
                    "avg_px": None, "err": None, "unresolved": True}

    def _account_params(self):
        c = self.conf.arcus_creds
        return {"address": c.address.lower(), "accountIndex": c.account_index}

    async def fetch_equity(self):
        data = await self._get("/v1/account", self._account_params(), auth=True)
        if data is None:
            return 0.0, 0.0
        equity = float(data.get("equity"))
        free = float(data.get("freeCollateral",
                              data.get("availableBalance",
                                       data.get("netQuoteBalance", 0.0))))
        return equity, free

    async def fetch_position(self) -> float:
        params = {**self._account_params(), "market": self.market}
        data = await self._get("/v1/positions", params, auth=True)
        source = (data or {}).get("positions", data or {})
        rows = source if isinstance(source, list) else source.values()
        for p in rows:
            if int(p.get("marketId", -1)) != self.market_id:
                continue
            size = float(p.get("size", p.get("sizeBase", 0.0)))
            if str(p.get("side", "")).upper() == "SHORT" and size > 0:
                size = -size
            return size
        return 0.0

    async def close(self) -> None:
        pass
