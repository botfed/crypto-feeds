//! HFT-compatible feed runtime.
//!
//! Zero-allocation hot path, no tokio, single-threaded mio/epoll event loop.
//! Data flows through the same seqlock ring buffers as the async path.
//!
//! # Architecture
//!
//! ```text
//! mio epoll_wait (all exchange sockets)
//!   → WsFramer::recv() into pre-allocated 256KB buffer
//!   → WsFramer::next_frame() returns &[u8] slice (zero-copy)
//!   → HftFeed::parse_text() writes to pre-allocated TickScratch
//!   → RingBuffer::push() copies 72-byte MarketData into seqlock slot
//! ```
//!
//! Total hot-path heap allocations: **zero**.

pub mod ws_framer;
pub mod binance;
pub mod hyperliquid;
pub mod risex;
pub mod engine;
pub mod alloc_guard;

use crate::market_data::FeedItem;
use crate::symbol_registry::SymbolId;
use std::time::Instant;

// ── Zero-alloc scratch buffer ──────────────────────────────────────

/// Maximum ticks produced by a single WS message (e.g. batch trade updates).
pub const MAX_TICKS_PER_MSG: usize = 64;

/// A parsed tick with pre-resolved SymbolId. Stack-only, no heap.
#[derive(Clone, Copy)]
pub struct ParsedTick<T: Copy> {
    pub symbol_id: SymbolId,
    pub item: T,
}

/// Pre-allocated scratch buffer for parse results. Reused across messages.
pub struct TickScratch<T: Copy + Default> {
    ticks: [ParsedTick<T>; MAX_TICKS_PER_MSG],
    count: usize,
}

impl<T: Copy + Default> TickScratch<T> {
    pub fn new() -> Self {
        Self {
            ticks: [ParsedTick {
                symbol_id: 0,
                item: T::default(),
            }; MAX_TICKS_PER_MSG],
            count: 0,
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.count = 0;
    }

    #[inline]
    pub fn push(&mut self, symbol_id: SymbolId, item: T) {
        if self.count < MAX_TICKS_PER_MSG {
            self.ticks[self.count] = ParsedTick { symbol_id, item };
            self.count += 1;
        }
    }

    #[inline]
    pub fn as_slice(&self) -> &[ParsedTick<T>] {
        &self.ticks[..self.count]
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.count
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

// ── HFT feed trait ─────────────────────────────────────────────────

/// HFT feed trait. Zero-alloc, sync, single-threaded.
///
/// Unlike `ExchangeFeed` (async, returns `Vec<(String, Item)>`), this trait
/// writes parse results into a pre-allocated `TickScratch` and resolves
/// symbols to `SymbolId` during parse — no heap allocation on the hot path.
pub trait HftFeed: Send {
    type Item: FeedItem;

    /// WebSocket URL(s) to connect to. Called once at startup.
    fn urls(&self) -> Vec<String>;

    /// Subscription messages to send after connect. Called once per connection.
    fn subscribe_messages(&self) -> Vec<String>;

    /// Heartbeat text to send periodically, if any.
    fn heartbeat_payload(&self) -> Option<&'static [u8]> {
        None
    }

    /// Heartbeat interval in milliseconds.
    fn heartbeat_interval_ms(&self) -> u64 {
        10_000
    }

    /// Called after a successful WebSocket connect. Default: no-op.
    /// Override to reset per-connection state (e.g. dedup counters).
    fn on_connected(&mut self, _conn_index: usize) {}

    /// Parse a WS text frame payload. Writes results into `scratch`.
    /// `payload` is a zero-copy slice into the WsFramer's pre-allocated buffer.
    ///
    /// Takes `&mut self` because feeds own mutable state (dedup counters, etc.).
    /// This is safe: HFT feeds are single-threaded by design.
    ///
    /// # Contract
    /// This method MUST NOT allocate on the heap. The allocation-counter
    /// test enforces this.
    fn parse_text(
        &mut self,
        payload: &[u8],
        received_instant: Instant,
        scratch: &mut TickScratch<Self::Item>,
    );

    /// Parse a WS binary frame payload. Default: ignore.
    fn parse_binary(
        &mut self,
        _payload: &[u8],
        _received_instant: Instant,
        _scratch: &mut TickScratch<Self::Item>,
    ) {
    }
}

// ── Zero-alloc JSON byte-scanning helpers ──────────────────────────

/// Find a byte pattern in a haystack. Returns the offset of the first match.
#[inline]
pub fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    // Use memchr for single-byte needles (fast SIMD path)
    if needle.len() == 1 {
        return haystack.iter().position(|&b| b == needle[0]);
    }
    haystack
        .windows(needle.len())
        .position(|w| w == needle)
}

/// Extract a JSON string value after a key pattern like `"key":"`.
/// Returns the string content (without quotes) as a `&str` slice into `json`.
/// Zero-allocation.
#[inline]
pub fn extract_str_after<'a>(json: &'a [u8], pattern: &[u8]) -> Option<&'a str> {
    let pos = find_bytes(json, pattern)?;
    let start = pos + pattern.len();
    if start >= json.len() {
        return None;
    }
    // Find closing quote
    let rest = &json[start..];
    let end = rest.iter().position(|&b| b == b'"')?;
    // SAFETY: Exchange JSON is ASCII. If it isn't, from_utf8 catches it.
    std::str::from_utf8(&rest[..end]).ok()
}

/// Extract a JSON unsigned integer value after a pattern like `"key":`.
/// Parses digits until a non-digit is found. Zero-allocation.
#[inline]
pub fn extract_u64_after(json: &[u8], pattern: &[u8]) -> Option<u64> {
    let pos = find_bytes(json, pattern)?;
    let mut start = pos + pattern.len();
    if start >= json.len() {
        return None;
    }
    // Skip optional leading quote (handles "key":"123" string form)
    if json[start] == b'"' {
        start += 1;
    }
    let mut val = 0u64;
    let mut i = start;
    while i < json.len() && json[i].is_ascii_digit() {
        val = val.wrapping_mul(10).wrapping_add((json[i] - b'0') as u64);
        i += 1;
    }
    if i == start {
        None
    } else {
        Some(val)
    }
}

/// Parse an f64 from a `&str`. Uses the standard library parser which
/// is stack-only (no heap allocation).
#[inline]
pub fn parse_f64(s: &str) -> Option<f64> {
    s.parse::<f64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_bytes() {
        let hay = b"hello world";
        assert_eq!(find_bytes(hay, b"world"), Some(6));
        assert_eq!(find_bytes(hay, b"hello"), Some(0));
        assert_eq!(find_bytes(hay, b"xyz"), None);
        assert_eq!(find_bytes(hay, b""), None);
    }

    #[test]
    fn test_extract_str_after() {
        let json = br#"{"coin":"BTC","time":123}"#;
        assert_eq!(extract_str_after(json, b"\"coin\":\""), Some("BTC"));
        assert_eq!(extract_str_after(json, b"\"missing\":\""), None);
    }

    #[test]
    fn test_extract_str_after_complex() {
        let json = br#"{"data":{"s":"BTCUSDT","b":"67432.10","a":"67432.50"}}"#;
        assert_eq!(extract_str_after(json, b"\"s\":\""), Some("BTCUSDT"));
        assert_eq!(extract_str_after(json, b"\"b\":\""), Some("67432.10"));
        assert_eq!(extract_str_after(json, b"\"a\":\""), Some("67432.50"));
    }

    #[test]
    fn test_extract_u64_after() {
        let json = br#"{"u":400900217,"E":1568014460893}"#;
        assert_eq!(extract_u64_after(json, b"\"u\":"), Some(400900217));
        assert_eq!(extract_u64_after(json, b"\"E\":"), Some(1568014460893));
        assert_eq!(extract_u64_after(json, b"\"missing\":"), None);
    }

    #[test]
    fn test_extract_u64_after_at_end() {
        let json = br#"{"u":42}"#;
        assert_eq!(extract_u64_after(json, b"\"u\":"), Some(42));
    }

    #[test]
    fn test_extract_u64_after_quoted_string() {
        // RiseX sends market_id as string: "market_id":"12"
        let json = br#"{"market_id":"12","channel":"orderbook"}"#;
        assert_eq!(extract_u64_after(json, b"\"market_id\":"), Some(12));
        // Also works with unquoted number
        let json2 = br#"{"market_id":12,"channel":"orderbook"}"#;
        assert_eq!(extract_u64_after(json2, b"\"market_id\":"), Some(12));
    }

    #[test]
    fn test_parse_f64_no_alloc() {
        assert_eq!(parse_f64("67432.10000000"), Some(67432.1));
        assert_eq!(parse_f64("0.00001"), Some(0.00001));
        assert_eq!(parse_f64("999999999.0"), Some(999999999.0));
        assert_eq!(parse_f64("not_a_number"), None);
    }

    #[test]
    fn test_tick_scratch() {
        use crate::market_data::MarketData;
        let mut scratch = TickScratch::<MarketData>::new();
        assert!(scratch.is_empty());

        scratch.push(42, MarketData::default());
        assert_eq!(scratch.len(), 1);
        assert_eq!(scratch.as_slice()[0].symbol_id, 42);

        scratch.clear();
        assert!(scratch.is_empty());
    }

    #[test]
    fn test_tick_scratch_overflow() {
        use crate::market_data::MarketData;
        let mut scratch = TickScratch::<MarketData>::new();

        // Fill to capacity
        for i in 0..MAX_TICKS_PER_MSG {
            scratch.push(i, MarketData::default());
        }
        assert_eq!(scratch.len(), MAX_TICKS_PER_MSG);

        // One more should be silently dropped
        scratch.push(999, MarketData::default());
        assert_eq!(scratch.len(), MAX_TICKS_PER_MSG);
    }
}
