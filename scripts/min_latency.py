import asyncio
import json
import time
from datetime import datetime, timezone

import httpx

EXCHANGES = {
    "binance": {
        "url": "https://fapi.binance.com/fapi/v1/ping",
        "method": "GET",
    },
    "bybit": {
        "url": "https://api.bybit.com/v5/market/time",
        "method": "GET",
    },
    "okx": {
        "url": "https://www.okx.com/api/v5/public/time",
        "method": "GET",
    },
    "hyperliquid": {
        "url": "https://api.hyperliquid.xyz/info",
        "method": "POST",
        "body": {"type": "meta"},
    },
    "nado": {
        "url": "https://gateway.prod.nado.xyz/v2/status",
        "method": "GET",
    },
    "coinbase": {
        "url": "https://api.exchange.coinbase.com/time",
        "method": "GET",
    },
    "extended": {
        "url": "https://api.extended.exchange/v1/markets",
        "method": "GET",
    },
    "kucoin": {
        "url": "https://api.kucoin.com/api/v1/timestamp",
        "method": "GET",
    },
    "apex": {
        "url": "https://pro.apex.exchange/api/v3/time",
        "method": "GET",
    },
}

SAMPLES = 1000
WARMUP = 10
TIMEOUT = 5.0  # seconds per request
CONCURRENCY = 50  # max in-flight requests per exchange


async def measure(name: str, cfg: dict) -> dict:
    url = cfg["url"]
    method = cfg["method"]
    body = cfg.get("body")
    sem = asyncio.Semaphore(CONCURRENCY)

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        req_kw = {"url": url}
        if method == "POST" and body is not None:
            req_kw["json"] = body

        async def fire():
            if method == "POST":
                return await client.post(**req_kw)
            return await client.get(**req_kw)

        async def timed_request():
            async with sem:
                t0 = time.perf_counter()
                try:
                    await fire()
                except Exception:
                    return None
                t1 = time.perf_counter()
                return (t1 - t0) * 1000

        print(f"  {name}: warming up ({WARMUP})...")
        await asyncio.gather(*[fire() for _ in range(WARMUP)], return_exceptions=True)

        print(f"  {name}: measuring ({SAMPLES})...")
        results = await asyncio.gather(*[timed_request() for _ in range(SAMPLES)])
        latencies = [r for r in results if r is not None]
        errors = SAMPLES - len(latencies)

    if errors:
        print(f"  {name}: {errors} errors out of {SAMPLES}")

    if not latencies:
        return {"name": name, "error": "all requests failed", "raw_rtt": []}

    latencies_sorted = sorted(latencies)
    n = len(latencies_sorted)
    stats = {
        "min_rtt": latencies_sorted[0],
        "floor": latencies_sorted[0] / 2,
        "mean": sum(latencies) / n,
        "median": latencies_sorted[n // 2],
        "p95": latencies_sorted[int(n * 0.95)],
        "p99": latencies_sorted[int(n * 0.99)],
        "samples": n,
    }
    return {"name": name, "stats": stats, "raw_rtt": latencies}


async def main():
    print(f"Latency floor measurement — {len(EXCHANGES)} exchanges, {SAMPLES} samples each\n")

    tasks = [measure(name, cfg) for name, cfg in EXCHANGES.items()]
    results = await asyncio.gather(*tasks)

    # summary table
    hdr = f"{'Exchange':<15} {'Min RTT':>8} {'Floor':>8} {'Mean':>8} {'Median':>8} {'p95':>8} {'p99':>8} {'N':>6}"
    sep = "─" * len(hdr)
    print(f"\n{hdr}")
    print(sep)
    for r in sorted(results, key=lambda r: r.get("stats", {}).get("floor", float("inf"))):
        name = r["name"]
        if "error" in r:
            print(f"{name:<15} {'FAILED':>8}")
            continue
        s = r["stats"]
        print(
            f"{name:<15} {s['min_rtt']:>7.2f}ms {s['floor']:>7.2f}ms "
            f"{s['mean']:>7.2f}ms {s['median']:>7.2f}ms "
            f"{s['p95']:>7.2f}ms {s['p99']:>7.2f}ms {s['samples']:>6}"
        )

    # json dump
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = {
        "timestamp": ts,
        "config": {"samples": SAMPLES, "warmup": WARMUP},
        "exchanges": {},
    }
    for r in results:
        name = r["name"]
        entry = {"raw_rtt": r["raw_rtt"]}
        if "stats" in r:
            entry["stats"] = r["stats"]
        if "error" in r:
            entry["error"] = r["error"]
        out["exchanges"][name] = entry

    fname = f"latency_{ts}.json"
    with open(fname, "w") as f:
        json.dump(out, f)
    print(f"\nRaw data saved to {fname}")


if __name__ == "__main__":
    asyncio.run(main())
