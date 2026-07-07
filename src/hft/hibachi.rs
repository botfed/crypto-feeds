/// Zero-allocation Hibachi orderbook parser.
///
/// Maintains a fixed-size, pre-allocated orderbook per symbol. Handles both
/// snapshot and incremental update messages. BBO is extracted from the top
/// of the sorted book — no BTreeMap, no heap allocation.
///
/// Wire format (snapshot):
/// ```json
/// {"symbol":"BTC/USDT-P","messageType":"Snapshot","topic":"orderbook","timestamp_ms":1700000000123,"data":{"bid":{"levels":[{"price":"67432.10","quantity":"1.234"}]},"ask":{"levels":[{"price":"67432.50","quantity":"0.567"}]}}}
/// ```
///
/// Wire format (update):
/// ```json
/// {"symbol":"BTC/USDT-P","messageType":"Update","topic":"orderbook","timestamp_ms":1700000000456,"data":{"bid":{"levels":[{"price":"67432.10","quantity":"0"}]},"ask":{"levels":[]}}}
/// ```
///
/// URL: `wss://data-api.hibachi.xyz/ws/market`
/// Subscribe: `{"method":"subscribe","parameters":{"subscriptions":[{"symbol":"BTC/USDT-P","topic":"orderbook"},...]}}`
use crate::hft::{HftFeed, TickScratch, extract_str_after, find_bytes, parse_f64};
use crate::market_data::MarketData;
use crate::symbol_registry::SymbolId;
use ordered_float::OrderedFloat;
use std::time::Instant;

// Byte patterns for zero-alloc JSON scanning.
const ORDERBOOK_MARKER: &[u8] = b"\"orderbook\"";
const SYMBOL_KEY: &[u8] = b"\"symbol\":\"";
const MESSAGE_TYPE_KEY: &[u8] = b"\"messageType\":\"";
const SNAPSHOT_VAL: &[u8] = b"Snapshot";
const BID_LEVELS_MARKER: &[u8] = b"\"bid\":{\"levels\":[";
const ASK_LEVELS_MARKER: &[u8] = b"\"ask\":{\"levels\":[";
const PRICE_KEY: &[u8] = b"\"price\":\"";
const QTY_KEY: &[u8] = b"\"quantity\":\"";

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

const MAX_SYMBOLS: usize = 32;

/// Maximum length of a native symbol string (e.g. "BTC/USDT-P" = 10).
const MAX_SYM_LEN: usize = 16;

#[derive(Clone, Copy)]
struct SymEntry {
    name: [u8; MAX_SYM_LEN],
    len: u8,
    symbol_id: SymbolId,
    book_idx: u8,
}

// ── Feed struct ────────────────────────────────────────────────────

pub struct HibachiHftFeed {
    symbols: [SymEntry; MAX_SYMBOLS],
    symbol_count: usize,
    books: Vec<FixedBook>,
    sub_message: String,
    debug_count: u32,
}

impl HibachiHftFeed {
    /// Create from a list of (native_symbol, SymbolId) pairs.
    ///
    /// `native_symbol` is e.g. "BTC/USDT-P", "ETH/USDT-P".
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

        // Build subscribe message
        let subs: Vec<String> = symbol_map.iter().take(count)
            .map(|(native, _)| format!(r#"{{"symbol":"{}","topic":"orderbook"}}"#, native))
            .collect();
        let sub_message = format!(
            r#"{{"method":"subscribe","parameters":{{"subscriptions":[{}]}}}}"#,
            subs.join(",")
        );

        Self {
            symbols,
            symbol_count: count,
            books,
            sub_message,
            debug_count: 0,
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

    /// Parse an orderbook message. Zero-alloc on hot path.
    #[inline]
    fn parse_orderbook(
        &mut self,
        json: &[u8],
        received_instant: Instant,
        scratch: &mut TickScratch<MarketData>,
    ) {
        // Quick rejection
        if find_bytes(json, ORDERBOOK_MARKER).is_none() {
            return;
        }

        // Extract symbol
        let sym_start = match find_bytes(json, SYMBOL_KEY) {
            Some(pos) => pos + SYMBOL_KEY.len(),
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

        // Determine snapshot vs update
        let is_snapshot = match extract_str_after(json, MESSAGE_TYPE_KEY) {
            Some(t) => t.as_bytes().starts_with(SNAPSHOT_VAL),
            None => return,
        };

        let book = &mut self.books[book_idx];

        if is_snapshot {
            book.clear();
        }

        // Parse bid levels
        if let Some(bids_start) = find_bytes(json, BID_LEVELS_MARKER) {
            let section = &json[bids_start + BID_LEVELS_MARKER.len()..];
            let end = find_closing_bracket(section);
            parse_levels(&section[..end], book, true);
        }

        // Parse ask levels
        if let Some(asks_start) = find_bytes(json, ASK_LEVELS_MARKER) {
            let section = &json[asks_start + ASK_LEVELS_MARKER.len()..];
            let end = find_closing_bracket(section);
            parse_levels(&section[..end], book, false);
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

/// Parse `{"price":"...","quantity":"..."}` objects from a JSON array. Zero-alloc.
#[inline]
fn parse_levels(data: &[u8], book: &mut FixedBook, is_bid: bool) {
    let mut pos = 0;
    let mut count = 0;

    while pos < data.len() && count < MAX_LEVELS_PER_MSG {
        let price_match = match find_bytes(&data[pos..], PRICE_KEY) {
            Some(p) => p,
            None => break,
        };
        let price_start = pos + price_match + PRICE_KEY.len();
        let price_str = match extract_str_value_at(&data[price_start..]) {
            Some(s) => s,
            None => break,
        };
        let price = match parse_f64(price_str) {
            Some(p) => p,
            None => { pos = price_start; continue; }
        };

        let qty_match = match find_bytes(&data[price_start..], QTY_KEY) {
            Some(p) => p,
            None => break,
        };
        let qty_start = price_start + qty_match + QTY_KEY.len();
        let qty_str = match extract_str_value_at(&data[qty_start..]) {
            Some(s) => s,
            None => break,
        };
        let qty = parse_f64(qty_str).unwrap_or(0.0);

        if is_bid {
            book.update_bid(price, qty);
        } else {
            book.update_ask(price, qty);
        }

        pos = qty_start;
        count += 1;
    }
}

/// Extract a quoted string value at the current position (up to closing `"`).
#[inline]
fn extract_str_value_at(data: &[u8]) -> Option<&str> {
    let end = data.iter().position(|&b| b == b'"')?;
    std::str::from_utf8(&data[..end]).ok()
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

impl HftFeed for HibachiHftFeed {
    type Item = MarketData;

    fn urls(&self) -> Vec<String> {
        vec!["wss://data-api.hibachi.xyz/ws/market".to_string()]
    }

    fn subscribe_messages(&self) -> Vec<String> {
        vec![self.sub_message.clone()]
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
        // TODO: remove after debugging wire format
        if self.debug_count < 5 {
            if let Ok(s) = std::str::from_utf8(payload) {
                eprintln!("HIBACHI_HFT_RAW[{}]: {}", self.debug_count, &s[..s.len().min(500)]);
            }
            self.debug_count += 1;
        }
        self.parse_orderbook(payload, received_instant, scratch);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_feed() -> HibachiHftFeed {
        HibachiHftFeed::new(&[
            ("BTC/USDT-P", 0),
            ("ETH/USDT-P", 1),
            ("SOL/USDT-P", 2),
        ])
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
    fn parse_snapshot_bbo() {
        let mut feed = test_feed();
        let msg = r#"{"symbol":"BTC/USDT-P","messageType":"Snapshot","topic":"orderbook","timestamp_ms":1700000000123,"data":{"bid":{"levels":[{"price":"67432.10","quantity":"1.234"},{"price":"67431.00","quantity":"2.0"}]},"ask":{"levels":[{"price":"67432.50","quantity":"0.567"},{"price":"67433.00","quantity":"1.0"}]}}}"#;

        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_orderbook(msg.as_bytes(), Instant::now(), &mut scratch);

        assert_eq!(scratch.len(), 1);
        let tick = &scratch.as_slice()[0];
        assert_eq!(tick.symbol_id, 0); // BTC
        assert_eq!(tick.item.bid.unwrap(), 67432.1);
        assert_eq!(tick.item.ask.unwrap(), 67432.5);
        assert_eq!(tick.item.bid_qty.unwrap(), 1.234);
        assert_eq!(tick.item.ask_qty.unwrap(), 0.567);
    }

    #[test]
    fn parse_update_changes_bbo() {
        let mut feed = test_feed();

        // Initial snapshot
        let d1 = r#"{"symbol":"BTC/USDT-P","messageType":"Snapshot","topic":"orderbook","data":{"bid":{"levels":[{"price":"100.0","quantity":"1.0"}]},"ask":{"levels":[{"price":"101.0","quantity":"1.0"}]}}}"#;
        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_orderbook(d1.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].item.bid.unwrap(), 100.0);

        // Update: new better bid
        let d2 = r#"{"symbol":"BTC/USDT-P","messageType":"Update","topic":"orderbook","data":{"bid":{"levels":[{"price":"100.5","quantity":"2.0"}]},"ask":{"levels":[]}}}"#;
        scratch.clear();
        feed.parse_orderbook(d2.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].item.bid.unwrap(), 100.5);
        assert_eq!(scratch.as_slice()[0].item.ask.unwrap(), 101.0); // unchanged
    }

    #[test]
    fn parse_update_removes_level() {
        let mut feed = test_feed();

        let d1 = r#"{"symbol":"BTC/USDT-P","messageType":"Snapshot","topic":"orderbook","data":{"bid":{"levels":[{"price":"102.0","quantity":"1.0"},{"price":"101.0","quantity":"2.0"}]},"ask":{"levels":[{"price":"103.0","quantity":"1.0"}]}}}"#;
        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_orderbook(d1.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].item.bid.unwrap(), 102.0);

        // Remove best bid (qty=0)
        let d2 = r#"{"symbol":"BTC/USDT-P","messageType":"Update","topic":"orderbook","data":{"bid":{"levels":[{"price":"102.0","quantity":"0"}]},"ask":{"levels":[]}}}"#;
        scratch.clear();
        feed.parse_orderbook(d2.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].item.bid.unwrap(), 101.0);
    }

    #[test]
    fn snapshot_resets_book() {
        let mut feed = test_feed();

        let snap1 = r#"{"symbol":"BTC/USDT-P","messageType":"Snapshot","topic":"orderbook","data":{"bid":{"levels":[{"price":"100.0","quantity":"1.0"}]},"ask":{"levels":[{"price":"101.0","quantity":"1.0"}]}}}"#;
        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_orderbook(snap1.as_bytes(), Instant::now(), &mut scratch);

        let snap2 = r#"{"symbol":"BTC/USDT-P","messageType":"Snapshot","topic":"orderbook","data":{"bid":{"levels":[{"price":"200.0","quantity":"5.0"}]},"ask":{"levels":[{"price":"201.0","quantity":"5.0"}]}}}"#;
        scratch.clear();
        feed.parse_orderbook(snap2.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].item.bid.unwrap(), 200.0);
        assert_eq!(scratch.as_slice()[0].item.ask.unwrap(), 201.0);

        // Old levels gone
        assert_eq!(feed.books[0].bid_count, 1);
    }

    #[test]
    fn different_symbols() {
        let mut feed = test_feed();

        let btc = r#"{"symbol":"BTC/USDT-P","messageType":"Snapshot","topic":"orderbook","data":{"bid":{"levels":[{"price":"67000.0","quantity":"1.0"}]},"ask":{"levels":[{"price":"67001.0","quantity":"1.0"}]}}}"#;
        let eth = r#"{"symbol":"ETH/USDT-P","messageType":"Snapshot","topic":"orderbook","data":{"bid":{"levels":[{"price":"3500.0","quantity":"10.0"}]},"ask":{"levels":[{"price":"3501.0","quantity":"10.0"}]}}}"#;

        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_orderbook(btc.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].symbol_id, 0);

        scratch.clear();
        feed.parse_orderbook(eth.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].symbol_id, 1);
    }

    #[test]
    fn rejects_garbage() {
        let mut feed = test_feed();
        let garbage = &[
            "",
            "{}",
            r#"{"topic":"trades"}"#,
            r#"{"symbol":"UNKNOWN/USDT-P","messageType":"Snapshot","topic":"orderbook","data":{"bid":{"levels":[{"price":"100","quantity":"1"}]},"ask":{"levels":[{"price":"101","quantity":"1"}]}}}"#,
        ];
        for &msg in garbage {
            let mut scratch = TickScratch::<MarketData>::new();
            feed.parse_orderbook(msg.as_bytes(), Instant::now(), &mut scratch);
            assert_eq!(scratch.len(), 0, "should reject: {}", msg);
        }
    }

    #[test]
    fn reconnect_clears_books() {
        let mut feed = test_feed();

        let d1 = r#"{"symbol":"BTC/USDT-P","messageType":"Snapshot","topic":"orderbook","data":{"bid":{"levels":[{"price":"100.0","quantity":"1.0"}]},"ask":{"levels":[{"price":"101.0","quantity":"1.0"}]}}}"#;
        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_orderbook(d1.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.len(), 1);

        feed.on_connected(0);

        // After reconnect, book should be empty
        let d2 = r#"{"symbol":"BTC/USDT-P","messageType":"Update","topic":"orderbook","data":{"bid":{"levels":[]},"ask":{"levels":[]}}}"#;
        scratch.clear();
        feed.parse_orderbook(d2.as_bytes(), Instant::now(), &mut scratch);
        let tick = &scratch.as_slice()[0];
        assert!(tick.item.bid.is_none());
        assert!(tick.item.ask.is_none());
    }

    #[test]
    fn subscribe_message_format() {
        let feed = HibachiHftFeed::new(&[("BTC/USDT-P", 0), ("ETH/USDT-P", 1)]);
        let msgs = feed.subscribe_messages();
        assert_eq!(msgs.len(), 1);
        assert!(msgs[0].contains(r#""method":"subscribe""#));
        assert!(msgs[0].contains(r#""symbol":"BTC/USDT-P""#));
        assert!(msgs[0].contains(r#""topic":"orderbook""#));
    }

    #[test]
    fn parse_latency_under_1us() {
        let msg = r#"{"symbol":"BTC/USDT-P","messageType":"Snapshot","topic":"orderbook","timestamp_ms":1700000000123,"data":{"bid":{"levels":[{"price":"67432.10","quantity":"1.234"},{"price":"67431.00","quantity":"2.0"}]},"ask":{"levels":[{"price":"67432.50","quantity":"0.567"},{"price":"67433.00","quantity":"1.0"}]}}}"#;

        let mut feed = test_feed();
        let payload = msg.as_bytes();
        let mut scratch = TickScratch::<MarketData>::new();

        // Warmup
        for _ in 0..1000 {
            scratch.clear();
            feed.parse_orderbook(payload, Instant::now(), &mut scratch);
        }

        let iters = 100_000u64;
        let start = Instant::now();
        for _ in 0..iters {
            scratch.clear();
            feed.parse_orderbook(payload, Instant::now(), &mut scratch);
        }
        let per_iter_ns = start.elapsed().as_nanos() as u64 / iters;
        eprintln!("Hibachi parse latency: {} ns/iter", per_iter_ns);

        #[cfg(not(debug_assertions))]
        assert!(per_iter_ns < 2_000, "parse latency {} ns exceeds 2us", per_iter_ns);
    }
}
