//! Allocation-counter test for the HFT hot path.
//!
//! Uses a custom global allocator that counts every malloc. Asserts that
//! the hot path (WS framing → JSON parse → ring buffer push) performs
//! exactly ZERO heap allocations.
//!
//! This test MUST be in its own binary (not `#[cfg(test)]` in lib) because
//! `#[global_allocator]` is process-wide. All checks run in a SINGLE test
//! function to avoid parallel-test interference with the allocation counter.

#![cfg(feature = "hft")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

struct CountingAlloc;

static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static ARMED: AtomicBool = AtomicBool::new(false);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static A: CountingAlloc = CountingAlloc;

fn arm() -> u64 {
    let v = ALLOC_COUNT.load(Ordering::SeqCst);
    ARMED.store(true, Ordering::SeqCst);
    v
}

fn disarm(baseline: u64) -> u64 {
    ARMED.store(false, Ordering::SeqCst);
    ALLOC_COUNT.load(Ordering::SeqCst) - baseline
}

/// Single test function that checks all three layers sequentially.
/// Must be one function since `#[global_allocator]` is shared across
/// all test threads in the process.
#[test]
fn hot_path_zero_allocations() {
    use crypto_feeds::hft::binance::BinanceHftFeed;
    use crypto_feeds::hft::ws_framer::{WsFramer, OP_TEXT};
    use crypto_feeds::hft::{HftFeed, TickScratch, extract_str_after, extract_u64_after, parse_f64};
    use crypto_feeds::market_data::{InstrumentType, MarketData};
    use crypto_feeds::ring_buffer::RingBuffer;
    use rustc_hash::FxHashMap;
    use std::time::Instant;

    // ── Helper: build unmasked server text frame ───────────────────
    fn make_frame(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(0x80 | OP_TEXT);
        let plen = payload.len();
        if plen <= 125 {
            out.push(plen as u8);
        } else {
            out.push(126);
            out.extend_from_slice(&(plen as u16).to_be_bytes());
        }
        out.extend_from_slice(payload);
        out
    }

    // ── Pre-allocate everything ────────────────────────────────────

    let mut framer = WsFramer::new();
    let mut scratch = TickScratch::<MarketData>::new();
    let ring = RingBuffer::<MarketData>::new();

    let mut symbol_to_id = FxHashMap::default();
    symbol_to_id.insert("BTCUSDT".to_string(), 0usize);
    symbol_to_id.insert("ETHUSDT".to_string(), 1usize);
    let mut feed = BinanceHftFeed::with_lookup(InstrumentType::Perp, symbol_to_id);

    let sample_json = br#"{"stream":"btcusdt@bookTicker","data":{"s":"BTCUSDT","b":"67432.10","B":"1.5","a":"67432.50","A":"2.0","u":1,"E":1700000000000}}"#;
    let frame_bytes = make_frame(sample_json);

    let messages: Vec<Vec<u8>> = (0..5u64)
        .map(|i| {
            let json = format!(
                r#"{{"stream":"btcusdt@bookTicker","data":{{"s":"BTCUSDT","b":"67432.10","B":"1.5","a":"67432.50","A":"2.0","u":{},"E":1700000000000}}}}"#,
                100 + i
            );
            make_frame(json.as_bytes())
        })
        .collect();

    // ── Warmup: exercise every code path to flush lazy init ────────

    // Framer warmup
    framer.inject(&frame_bytes);
    let _ = framer.next_frame();
    framer.reset();

    // JSON scanner warmup
    let json_data = br#"{"data":{"s":"BTCUSDT","b":"67432.10","B":"1.5","a":"67432.50","A":"2.0","u":12345678,"E":1700000000000}}"#;
    let _ = extract_str_after(json_data, b"\"s\":\"");
    let _ = extract_str_after(json_data, b"\"b\":\"");
    let _ = extract_str_after(json_data, b"\"a\":\"");
    let _ = extract_str_after(json_data, b"\"B\":\"");
    let _ = extract_str_after(json_data, b"\"A\":\"");
    let _ = extract_u64_after(json_data, b"\"u\":");
    let _ = extract_u64_after(json_data, b"\"E\":");
    let _ = parse_f64("67432.10");
    let _ = parse_f64("1.5");
    let _ = parse_f64("67432.50");
    {
        let sym = extract_str_after(json_data, b"\"s\":\"").unwrap();
        assert_eq!(sym, "BTCUSDT");
        let bid_str = extract_str_after(json_data, b"\"b\":\"").unwrap();
        assert_eq!(parse_f64(bid_str).unwrap(), 67432.10);
    }

    // Full pipeline warmup
    for msg in &messages {
        framer.reset();
        framer.inject(msg);
        if let Some(frame) = framer.next_frame() {
            scratch.clear();
            feed.parse_text(frame.payload, Instant::now(), &mut scratch);
            for tick in scratch.as_slice() {
                ring.push(tick.item);
            }
        }
    }
    feed.reset_dedup();

    // ════════════════════════════════════════════════════════════════
    //  ARMED: Every allocation from here is a test failure
    // ════════════════════════════════════════════════════════════════
    let baseline = arm();

    // ── Layer 1: WsFramer ──────────────────────────────────────────
    for _ in 0..1000 {
        framer.reset();
        framer.inject(&frame_bytes);
        let frame = framer.next_frame().unwrap();
        assert_eq!(frame.opcode, OP_TEXT);
        assert_eq!(frame.payload, sample_json.as_slice());
    }

    // ── Layer 2: JSON byte scanner ─────────────────────────────────
    for _ in 0..1000 {
        let sym = extract_str_after(json_data, b"\"s\":\"").unwrap();
        assert_eq!(sym, "BTCUSDT");

        let bid = parse_f64(extract_str_after(json_data, b"\"b\":\"").unwrap()).unwrap();
        assert_eq!(bid, 67432.10);

        let ask = parse_f64(extract_str_after(json_data, b"\"a\":\"").unwrap()).unwrap();
        assert_eq!(ask, 67432.50);

        let uid = extract_u64_after(json_data, b"\"u\":").unwrap();
        assert_eq!(uid, 12345678);

        let et = extract_u64_after(json_data, b"\"E\":").unwrap();
        assert_eq!(et, 1700000000000);
    }

    // ── Layer 3: Full pipeline (framer → parse → ring buffer) ─────
    for _ in 0..100 {
        feed.reset_dedup_symbol(0);
        for msg in &messages {
            framer.reset();
            framer.inject(msg);
            while let Some(frame) = framer.next_frame() {
                scratch.clear();
                feed.parse_text(frame.payload, Instant::now(), &mut scratch);
                for tick in scratch.as_slice() {
                    ring.push(tick.item);
                }
            }
        }
    }

    // ════════════════════════════════════════════════════════════════
    //  DISARMED
    // ════════════════════════════════════════════════════════════════
    let allocs = disarm(baseline);

    // Verify data actually flowed through
    let md = ring.latest().unwrap();
    assert_eq!(md.bid.unwrap(), 67432.10);
    assert_eq!(md.ask.unwrap(), 67432.50);

    assert_eq!(
        allocs, 0,
        "HOT PATH ALLOCATED {} TIMES — zero-alloc contract violated",
        allocs
    );
}
