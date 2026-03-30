import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    "kraken": {
        "url": "https://api.kraken.com/0/public/Time",
        "method": "GET",
    },
}

WARMUP = 10
TIMEOUT = 5.0
WORKERS_PER_EXCHANGE = 20


def measure(name: str, cfg: dict, samples: int) -> dict:
    url = cfg["url"]
    method = cfg["method"]
    body = cfg.get("body")

    client = httpx.Client(timeout=TIMEOUT)

    def fire():
        if method == "POST":
            return client.post(url, json=body)
        return client.get(url)

    def timed_fire():
        t0 = time.perf_counter()
        try:
            fire()
        except Exception:
            return None
        t1 = time.perf_counter()
        return (t1 - t0) * 1000

    print(f"  {name}: warming up ({WARMUP})...")
    for _ in range(WARMUP):
        try:
            fire()
        except Exception:
            pass

    print(f"  {name}: measuring ({samples})...")
    latencies = []
    with ThreadPoolExecutor(max_workers=WORKERS_PER_EXCHANGE) as pool:
        futures = [pool.submit(timed_fire) for _ in range(samples)]
        for f in as_completed(futures):
            r = f.result()
            if r is not None:
                latencies.append(r)

    client.close()

    errors = samples - len(latencies)
    if errors:
        print(f"  {name}: {errors} errors out of {samples}")

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
        "p999": latencies_sorted[min(int(n * 0.999), n - 1)],
        "samples": n,
    }
    return {"name": name, "stats": stats, "raw_rtt": latencies}


def main():
    parser = argparse.ArgumentParser(description="Measure latency floor to crypto exchanges")
    parser.add_argument("-n", "--samples", type=int, default=1000, help="number of requests per exchange (default: 1000)")
    args = parser.parse_args()
    samples = args.samples

    print(f"Latency floor measurement — {len(EXCHANGES)} exchanges, {samples} samples each\n")

    # run all exchanges concurrently, each with its own thread pool
    results = []
    with ThreadPoolExecutor(max_workers=len(EXCHANGES)) as pool:
        futures = {
            pool.submit(measure, name, cfg, samples): name
            for name, cfg in EXCHANGES.items()
        }
        for f in as_completed(futures):
            results.append(f.result())

    # summary table
    hdr = f"{'Exchange':<15} {'One-Way Floor':>14} {'Min RTT':>10} {'Mean RTT':>10} {'Median RTT':>11} {'p95 RTT':>10} {'p99 RTT':>10} {'p99.9 RTT':>11} {'Samples':>8}"
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
            f"{name:<15} {s['floor']:>12.2f}ms {s['min_rtt']:>8.2f}ms "
            f"{s['mean']:>8.2f}ms {s['median']:>9.2f}ms "
            f"{s['p95']:>8.2f}ms {s['p99']:>8.2f}ms {s['p999']:>9.2f}ms {s['samples']:>8}"
        )

    # json dump
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = {
        "timestamp": ts,
        "config": {"samples": samples, "warmup": WARMUP},
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
    main()
