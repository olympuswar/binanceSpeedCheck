import asyncio
import json
import time
from collections import deque
from datetime import datetime

import websockets

SUMMARY_INTERVAL_SEC = 5
SAMPLE_WINDOW = 10000

samples: dict[str, deque] = {}


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def parse_iso_ms(iso_ts: str) -> int:
    return int(datetime.fromisoformat(iso_ts.replace("Z", "+00:00")).timestamp() * 1000)


def record(exchange: str, diff_ms: int) -> None:
    buf = samples.setdefault(exchange, deque(maxlen=SAMPLE_WINDOW))
    first = len(buf) == 0
    buf.append(diff_ms)
    if first:
        print(f"[{exchange}] first tick received (delay {diff_ms} ms)")


async def binance() -> None:
    url = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
    async with websockets.connect(url) as ws:
        async for message in ws:
            t = now_ms()
            data = json.loads(message)
            event_ms = data.get("E")
            if event_ms:
                record("BINANCE", t - int(event_ms))


async def coinbase() -> None:
    url = "wss://ws-feed.exchange.coinbase.com"
    sub = {"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["matches"]}
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps(sub))
        async for message in ws:
            t = now_ms()
            data = json.loads(message)
            if data.get("type") != "match":
                continue
            iso = data.get("time")
            if iso:
                record("COINBASE", t - parse_iso_ms(iso))


async def kraken() -> None:
    url = "wss://ws.kraken.com/v2"
    sub = {"method": "subscribe", "params": {"channel": "trade", "symbol": ["BTC/USD"]}}
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps(sub))
        async for message in ws:
            t = now_ms()
            data = json.loads(message)
            if data.get("channel") != "trade" or data.get("type") != "update":
                continue
            for trade in data.get("data", []):
                iso = trade.get("timestamp")
                if iso:
                    record("KRAKEN", t - parse_iso_ms(iso))


async def gemini() -> None:
    url = "wss://api.gemini.com/v1/marketdata/BTCUSD"
    async with websockets.connect(url) as ws:
        async for message in ws:
            t = now_ms()
            data = json.loads(message)
            ts_ms = data.get("timestampms")
            if not ts_ms:
                continue
            events = data.get("events") or []
            if not any(e.get("type") == "trade" for e in events):
                continue
            record("GEMINI", t - int(ts_ms))


async def run_with_retry(name: str, coro_fn) -> None:
    while True:
        try:
            await coro_fn()
        except Exception as e:
            print(f"[{name}] error: {e!r} — reconnecting in 3s")
            await asyncio.sleep(3)


async def summary_printer() -> None:
    while True:
        await asyncio.sleep(SUMMARY_INTERVAL_SEC)
        print(f"\n--- summary (window = last {SAMPLE_WINDOW} samples) ---")
        print(f"{'exchange':<10} {'count':>6} {'min':>6} {'p50':>6} {'p99':>6} {'max':>6}  (ms)")
        for ex in ("BINANCE", "COINBASE", "KRAKEN", "GEMINI"):
            buf = samples.get(ex)
            if not buf:
                print(f"{ex:<10} {'-':>6} {'-':>6} {'-':>6} {'-':>6} {'-':>6}")
                continue
            s = sorted(buf)
            n = len(s)
            p50 = s[n // 2]
            p99 = s[min(n - 1, int(n * 0.99))]
            print(f"{ex:<10} {n:>6} {s[0]:>6} {p50:>6} {p99:>6} {s[-1]:>6}")
        print()


async def main() -> None:
    print("Connecting to Binance, Coinbase, Kraken, Gemini...")
    await asyncio.gather(
        run_with_retry("BINANCE", binance),
        run_with_retry("COINBASE", coinbase),
        run_with_retry("KRAKEN", kraken),
        run_with_retry("GEMINI", gemini),
        summary_printer(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
