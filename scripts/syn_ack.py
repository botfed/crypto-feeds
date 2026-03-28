import asyncio
import httpx
import time


async def measure_binance_fapi(samples=1000, warmup=10):
    latencies = []
    offsets = []

    async with httpx.AsyncClient() as client:

        print(f"Warming up ({warmup} requests)...")
        for _ in range(warmup):
            await client.get("https://fapi.binance.com/fapi/v1/time")

        print(f"Measuring ({samples} requests)...")
        for _ in range(samples):
            t_send = time.perf_counter()
            r = await client.get("https://fapi.binance.com/fapi/v1/time")

            t_recv = time.perf_counter()
            rtt = (t_recv - t_send) * 1000  # ms

            server_time = r.json()["serverTime"]  # ms
            t_recv_ms = time.time() * 1000

            one_way = rtt / 2
            estimated_server_recv = t_recv_ms - one_way
            offset = server_time - estimated_server_recv

            latencies.append(rtt)
            offsets.append(offset)

    min_rtt = min(latencies)
    latency_floor = min_rtt / 2
    best_idx = latencies.index(min_rtt)
    best_offset = offsets[best_idx]

    print(f"\nResults:")
    print(f"Min RTT:        {min_rtt:.3f} ms")
    print(f"Latency floor:  {latency_floor:.3f} ms")
    print(f"Clock offset:   {best_offset:.3f} ms (+ means server ahead)")
    print(f"Mean RTT:       {sum(latencies)/len(latencies):.3f} ms")
    print(f"Median RTT:     {sorted(latencies)[len(latencies)//2]:.3f} ms")
    print(f"p95 RTT:        {sorted(latencies)[int(len(latencies)*0.95)]:.3f} ms")

    return latency_floor, best_offset


asyncio.run(measure_binance_fapi())
