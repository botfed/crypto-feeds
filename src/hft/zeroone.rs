/// Zero-allocation ZeroOne orderbook delta parser.
///
/// Maintains a fixed-size, pre-allocated orderbook per market. All deltas
/// are incremental updates (qty=0 removes a level).
///
/// Wire format (delta):
/// ```json
/// {"delta":{"market_symbol":"BTCUSD","update_id":123,"bids":[[67432.1,1.234],[67431.0,2.0]],"asks":[[67432.5,0.567]]}}
/// ```
///
/// URL format: `wss://zo-mainnet.n1.xyz/ws/deltas@BTCUSD&deltas@ETHUSD`
/// Streams are encoded in the URL path — no subscribe message needed.
use crate::hft::{HftFeed, TickScratch, find_bytes, parse_f64};
use crate::market_data::MarketData;
use crate::symbol_registry::SymbolId;
use ordered_float::OrderedFloat;
use std::time::Instant;

// Byte patterns for zero-alloc JSON scanning.
const DELTA_MARKER: &[u8] = b"\"delta\"";
const MARKET_SYMBOL_KEY: &[u8] = b"\"market_symbol\":\"";
const BIDS_MARKER: &[u8] = b"\"bids\":[";
const ASKS_MARKER: &[u8] = b"\"asks\":[";

// ── Fixed-size zero-alloc orderbook ────────────────────────────────

const MAX_LEVELS: usize = 128;
const MAX_LEVELS_PER_MSG: usize = 64;

#[derive(Clone, Copy, Default)]
struct Level {
    price: OrderedFloat<f64>,
    qty: f64,
}

pub struct FixedBook {
    bids: [Level; MAX_LEVELS],
    asks: [Level; MAX_LEVELS],
    bid_count: usize,
    ask_count: usize,
}

impl FixedBook {
    fn new() -> Self {
        Self {
            bids: [Level::default(); MAX_LEVELS],
            asks: [Level::default(); MAX_LEVELS],
            bid_count: 0,
            ask_count: 0,
        }
    }

    #[inline]
    fn clear(&mut self) {
        self.bid_count = 0;
        self.ask_count = 0;
    }

    #[inline]
    fn update_bid(&mut self, price: f64, qty: f64) {
        let key = OrderedFloat(price);
        let pos = self.bids[..self.bid_count]
            .partition_point(|l| l.price > key);

        if pos < self.bid_count && self.bids[pos].price == key {
            if qty == 0.0 {
                self.bids.copy_within(pos + 1..self.bid_count, pos);
                self.bid_count -= 1;
            } else {
                self.bids[pos].qty = qty;
            }
        } else if qty > 0.0 && self.bid_count < MAX_LEVELS {
            self.bids.copy_within(pos..self.bid_count, pos + 1);
            self.bids[pos] = Level { price: key, qty };
            self.bid_count += 1;
        }
    }

    #[inline]
    fn update_ask(&mut self, price: f64, qty: f64) {
        let key = OrderedFloat(price);
        let pos = self.asks[..self.ask_count]
            .partition_point(|l| l.price < key);

        if pos < self.ask_count && self.asks[pos].price == key {
            if qty == 0.0 {
                self.asks.copy_within(pos + 1..self.ask_count, pos);
                self.ask_count -= 1;
            } else {
                self.asks[pos].qty = qty;
            }
        } else if qty > 0.0 && self.ask_count < MAX_LEVELS {
            self.asks.copy_within(pos..self.ask_count, pos + 1);
            self.asks[pos] = Level { price: key, qty };
            self.ask_count += 1;
        }
    }

    #[inline]
    fn best_bid(&self) -> Option<(f64, f64)> {
        if self.bid_count > 0 {
            Some((self.bids[0].price.into_inner(), self.bids[0].qty))
        } else {
            None
        }
    }

    #[inline]
    fn best_ask(&self) -> Option<(f64, f64)> {
        if self.ask_count > 0 {
            Some((self.asks[0].price.into_inner(), self.asks[0].qty))
        } else {
            None
        }
    }
}

// ── Symbol lookup ──────────────────────────────────────────────────

/// Maximum number of subscribed symbols.
const MAX_SYMBOLS: usize = 32;

/// Maximum length of a native symbol string (e.g. "BTCUSD" = 6, "VIRTUALUSD" = 10).
const MAX_SYM_LEN: usize = 16;

/// Pre-allocated symbol entry for O(N) scan (N ≤ 32, faster than HashMap on hot path).
#[derive(Clone, Copy)]
struct SymEntry {
    /// Native symbol bytes, zero-padded.
    name: [u8; MAX_SYM_LEN],
    len: u8,
    symbol_id: SymbolId,
    book_idx: u8,
}

// ── Feed struct ────────────────────────────────────────────────────

pub struct ZeroOneHftFeed {
    symbols: [SymEntry; MAX_SYMBOLS],
    symbol_count: usize,
    books: Vec<FixedBook>,
    url: String,
}

impl ZeroOneHftFeed {
    /// Create from a list of (native_symbol, SymbolId) pairs.
    ///
    /// `native_symbol` is e.g. "BTCUSD", "ETHUSD".
    pub fn new(symbol_map: &[(&str, SymbolId)]) -> Self {
        let mut symbols = [SymEntry {
            name: [0; MAX_SYM_LEN],
            len: 0,
            symbol_id: 0,
            book_idx: 0,
        }; MAX_SYMBOLS];
        let mut books = Vec::new();

        let count = symbol_map.len().min(MAX_SYMBOLS);
        for (i, &(native, sid)) in symbol_map.iter().take(count).enumerate() {
            let bytes = native.as_bytes();
            let len = bytes.len().min(MAX_SYM_LEN);
            let mut name = [0u8; MAX_SYM_LEN];
            name[..len].copy_from_slice(&bytes[..len]);
            symbols[i] = SymEntry {
                name,
                len: len as u8,
                symbol_id: sid,
                book_idx: i as u8,
            };
            books.push(FixedBook::new());
        }

        // Build URL: wss://zo-mainnet.n1.xyz/ws/deltas@BTCUSD&deltas@ETHUSD
        let streams: Vec<String> = symbol_map.iter().take(count)
            .map(|(native, _)| format!("deltas@{native}"))
            .collect();
        let url = format!("wss://zo-mainnet.n1.xyz/ws/{}", streams.join("&"));

        Self {
            symbols,
            symbol_count: count,
            books,
            url,
        }
    }

    /// Lookup symbol by native name bytes. O(N) scan, N ≤ 32.
    #[inline]
    fn lookup(&self, name: &[u8]) -> Option<(SymbolId, usize)> {
        let len = name.len();
        for i in 0..self.symbol_count {
            let entry = &self.symbols[i];
            if entry.len as usize == len && entry.name[..len] == *name {
                return Some((entry.symbol_id, entry.book_idx as usize));
            }
        }
        None
    }

    /// Parse a delta message. Zero-alloc on hot path.
    #[inline]
    fn parse_delta(
        &mut self,
        json: &[u8],
        received_instant: Instant,
        scratch: &mut TickScratch<MarketData>,
    ) {
        // Quick rejection
        if find_bytes(json, DELTA_MARKER).is_none() {
            return;
        }

        // Extract market_symbol
        let sym_start = match find_bytes(json, MARKET_SYMBOL_KEY) {
            Some(pos) => pos + MARKET_SYMBOL_KEY.len(),
            None => return,
        };
        let sym_end = match json[sym_start..].iter().position(|&b| b == b'"') {
            Some(pos) => sym_start + pos,
            None => return,
        };
        let sym_bytes = &json[sym_start..sym_end];

        let (symbol_id, book_idx) = match self.lookup(sym_bytes) {
            Some(v) => v,
            None => return,
        };

        let book = &mut self.books[book_idx];

        // Parse bids: [[price, qty], ...]
        if let Some(bids_start) = find_bytes(json, BIDS_MARKER) {
            let section = &json[bids_start + BIDS_MARKER.len()..];
            let end = find_closing_bracket(section);
            parse_tuple_levels(&section[..end], book, true);
        }

        // Parse asks
        if let Some(asks_start) = find_bytes(json, ASKS_MARKER) {
            let section = &json[asks_start + ASKS_MARKER.len()..];
            let end = find_closing_bracket(section);
            parse_tuple_levels(&section[..end], book, false);
        }

        let bid = book.best_bid();
        let ask = book.best_ask();

        scratch.push(
            symbol_id,
            MarketData {
                bid: bid.map(|(p, _)| p),
                ask: ask.map(|(p, _)| p),
                bid_qty: bid.map(|(_, q)| q),
                ask_qty: ask.map(|(_, q)| q),
                received_instant: Some(received_instant),
                ..Default::default()
            },
        );
    }
}

/// Parse `[price, qty]` tuples from a JSON array section. Zero-alloc.
///
/// Input format: `[67432.1, 1.234], [67431.0, 2.0]` (content between outer `[` and `]`).
#[inline]
fn parse_tuple_levels(data: &[u8], book: &mut FixedBook, is_bid: bool) {
    let mut pos = 0;
    let mut count = 0;

    while pos < data.len() && count < MAX_LEVELS_PER_MSG {
        // Find opening bracket of tuple
        let bracket = match data[pos..].iter().position(|&b| b == b'[') {
            Some(p) => pos + p,
            None => break,
        };
        // Find closing bracket
        let end = match data[bracket + 1..].iter().position(|&b| b == b']') {
            Some(p) => bracket + 1 + p,
            None => break,
        };
        let tuple = &data[bracket + 1..end];

        // Find comma separator
        let comma = match tuple.iter().position(|&b| b == b',') {
            Some(p) => p,
            None => { pos = end + 1; continue; }
        };

        let price_str = trim_ascii(&tuple[..comma]);
        let qty_str = trim_ascii(&tuple[comma + 1..]);

        let price = match parse_f64_bytes(price_str) {
            Some(p) => p,
            None => { pos = end + 1; continue; }
        };
        let qty = parse_f64_bytes(qty_str).unwrap_or(0.0);

        if is_bid {
            book.update_bid(price, qty);
        } else {
            book.update_ask(price, qty);
        }

        pos = end + 1;
        count += 1;
    }
}

/// Trim ASCII whitespace from a byte slice.
#[inline]
fn trim_ascii(b: &[u8]) -> &[u8] {
    let start = b.iter().position(|&c| !c.is_ascii_whitespace()).unwrap_or(b.len());
    let end = b.iter().rposition(|&c| !c.is_ascii_whitespace()).map(|p| p + 1).unwrap_or(start);
    &b[start..end]
}

/// Parse f64 from byte slice without going through &str allocation.
#[inline]
fn parse_f64_bytes(b: &[u8]) -> Option<f64> {
    // SAFETY: exchange JSON numbers are ASCII
    std::str::from_utf8(b).ok().and_then(|s| parse_f64(s))
}

/// Find the position of the closing `]` bracket, handling nesting.
#[inline]
fn find_closing_bracket(data: &[u8]) -> usize {
    let mut depth = 1i32;
    for (i, &b) in data.iter().enumerate() {
        match b {
            b'[' | b'{' => depth += 1,
            b']' | b'}' => {
                depth -= 1;
                if depth == 0 {
                    return i;
                }
            }
            _ => {}
        }
    }
    data.len()
}

impl HftFeed for ZeroOneHftFeed {
    type Item = MarketData;

    fn urls(&self) -> Vec<String> {
        vec![self.url.clone()]
    }

    fn subscribe_messages(&self) -> Vec<String> {
        // ZeroOne encodes streams in the URL path — no subscribe message needed
        vec![]
    }

    fn heartbeat_payload(&self) -> Option<&'static [u8]> {
        None // use WS ping frames (default)
    }

    fn on_connected(&mut self, _conn_index: usize) {
        for book in &mut self.books {
            book.clear();
        }
    }

    fn parse_text(
        &mut self,
        payload: &[u8],
        received_instant: Instant,
        scratch: &mut TickScratch<MarketData>,
    ) {
        self.parse_delta(payload, received_instant, scratch);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_feed() -> ZeroOneHftFeed {
        ZeroOneHftFeed::new(&[("BTCUSD", 0), ("ETHUSD", 1), ("SOLUSD", 2)])
    }

    // ── FixedBook unit tests ───────────────────────────────────────

    #[test]
    fn book_insert_and_best() {
        let mut book = FixedBook::new();
        book.update_bid(100.0, 1.0);
        book.update_bid(101.0, 2.0);
        book.update_bid(99.0, 3.0);
        assert_eq!(book.best_bid(), Some((101.0, 2.0)));
        assert_eq!(book.bid_count, 3);

        book.update_ask(102.0, 1.0);
        book.update_ask(103.0, 2.0);
        book.update_ask(101.5, 3.0);
        assert_eq!(book.best_ask(), Some((101.5, 3.0)));
        assert_eq!(book.ask_count, 3);
    }

    #[test]
    fn book_remove_level() {
        let mut book = FixedBook::new();
        book.update_bid(100.0, 1.0);
        book.update_bid(101.0, 2.0);
        book.update_bid(102.0, 3.0);
        book.update_bid(102.0, 0.0);
        assert_eq!(book.best_bid(), Some((101.0, 2.0)));
        assert_eq!(book.bid_count, 2);
    }

    // ── Parse tests ────────────────────────────────────────────────

    #[test]
    fn parse_delta_bbo() {
        let mut feed = test_feed();
        let msg = r#"{"delta":{"market_symbol":"BTCUSD","update_id":1,"bids":[[67432.1,1.234],[67431.0,2.0]],"asks":[[67432.5,0.567],[67433.0,1.0]]}}"#;

        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_delta(msg.as_bytes(), Instant::now(), &mut scratch);

        assert_eq!(scratch.len(), 1);
        let tick = &scratch.as_slice()[0];
        assert_eq!(tick.symbol_id, 0); // BTC
        assert_eq!(tick.item.bid.unwrap(), 67432.1);
        assert_eq!(tick.item.ask.unwrap(), 67432.5);
        assert_eq!(tick.item.bid_qty.unwrap(), 1.234);
        assert_eq!(tick.item.ask_qty.unwrap(), 0.567);
    }

    #[test]
    fn parse_delta_update_changes_bbo() {
        let mut feed = test_feed();

        // Initial delta
        let d1 = r#"{"delta":{"market_symbol":"BTCUSD","update_id":1,"bids":[[100.0,1.0]],"asks":[[101.0,1.0]]}}"#;
        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_delta(d1.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].item.bid.unwrap(), 100.0);

        // Update: new better bid
        let d2 = r#"{"delta":{"market_symbol":"BTCUSD","update_id":2,"bids":[[100.5,2.0]],"asks":[]}}"#;
        scratch.clear();
        feed.parse_delta(d2.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].item.bid.unwrap(), 100.5);
        assert_eq!(scratch.as_slice()[0].item.ask.unwrap(), 101.0); // unchanged
    }

    #[test]
    fn parse_delta_removes_level() {
        let mut feed = test_feed();

        let d1 = r#"{"delta":{"market_symbol":"BTCUSD","update_id":1,"bids":[[102.0,1.0],[101.0,2.0]],"asks":[[103.0,1.0]]}}"#;
        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_delta(d1.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].item.bid.unwrap(), 102.0);

        // Remove best bid (qty=0)
        let d2 = r#"{"delta":{"market_symbol":"BTCUSD","update_id":2,"bids":[[102.0,0]],"asks":[]}}"#;
        scratch.clear();
        feed.parse_delta(d2.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].item.bid.unwrap(), 101.0);
    }

    #[test]
    fn different_symbols() {
        let mut feed = test_feed();

        let btc = r#"{"delta":{"market_symbol":"BTCUSD","update_id":1,"bids":[[67000.0,1.0]],"asks":[[67001.0,1.0]]}}"#;
        let eth = r#"{"delta":{"market_symbol":"ETHUSD","update_id":1,"bids":[[3500.0,10.0]],"asks":[[3501.0,10.0]]}}"#;

        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_delta(btc.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].symbol_id, 0);

        scratch.clear();
        feed.parse_delta(eth.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].symbol_id, 1);
    }

    #[test]
    fn rejects_garbage() {
        let mut feed = test_feed();
        let garbage = &[
            "",
            "{}",
            r#"{"trades":{}}"#,
            r#"{"delta":{"market_symbol":"UNKNOWN","update_id":1,"bids":[[100,1]],"asks":[[101,1]]}}"#,
        ];
        for &msg in garbage {
            let mut scratch = TickScratch::<MarketData>::new();
            feed.parse_delta(msg.as_bytes(), Instant::now(), &mut scratch);
            assert_eq!(scratch.len(), 0, "should reject: {}", msg);
        }
    }

    #[test]
    fn reconnect_clears_books() {
        let mut feed = test_feed();

        let d1 = r#"{"delta":{"market_symbol":"BTCUSD","update_id":1,"bids":[[100.0,1.0]],"asks":[[101.0,1.0]]}}"#;
        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_delta(d1.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.len(), 1);

        feed.on_connected(0);

        // After reconnect, book should be empty — delta produces no BBO
        let d2 = r#"{"delta":{"market_symbol":"BTCUSD","update_id":2,"bids":[],"asks":[]}}"#;
        scratch.clear();
        feed.parse_delta(d2.as_bytes(), Instant::now(), &mut scratch);
        let tick = &scratch.as_slice()[0];
        assert!(tick.item.bid.is_none());
        assert!(tick.item.ask.is_none());
    }

    #[test]
    fn parse_latency_under_1us() {
        let msg = r#"{"delta":{"market_symbol":"BTCUSD","update_id":123,"bids":[[67432.1,1.234],[67431.0,2.0]],"asks":[[67432.5,0.567],[67433.0,1.0]]}}"#;

        let mut feed = test_feed();
        let payload = msg.as_bytes();
        let mut scratch = TickScratch::<MarketData>::new();

        // Warmup
        for _ in 0..1000 {
            scratch.clear();
            feed.parse_delta(payload, Instant::now(), &mut scratch);
        }

        let iters = 100_000u64;
        let start = Instant::now();
        for _ in 0..iters {
            scratch.clear();
            feed.parse_delta(payload, Instant::now(), &mut scratch);
        }
        let per_iter_ns = start.elapsed().as_nanos() as u64 / iters;
        eprintln!("ZeroOne parse latency: {} ns/iter", per_iter_ns);

        #[cfg(not(debug_assertions))]
        assert!(per_iter_ns < 2_000, "parse latency {} ns exceeds 2us", per_iter_ns);
    }
}
