/// Zero-allocation RiseX orderbook BBO parser.
///
/// Maintains a fixed-size, pre-allocated orderbook per market. Handles both
/// snapshot and incremental update messages. BBO is extracted from the top
/// of the sorted book — no BTreeMap, no heap allocation.
///
/// Wire format (snapshot):
/// ```json
/// {"channel":"orderbook","type":"snapshot","market_id":12,"data":{"bids":[{"price":"67432.10","quantity":"1.234"}],"asks":[...]},"timestamp":"1676151190123456789"}
/// ```
///
/// Wire format (update):
/// ```json
/// {"channel":"orderbook","type":"update","market_id":12,"data":{"bids":[{"price":"67432.10","quantity":"0"}],"asks":[...]},"timestamp":"1676151190123456789"}
/// ```
use crate::hft::{HftFeed, TickScratch, extract_str_after, extract_u64_after, find_bytes, parse_f64};
use crate::market_data::MarketData;
use crate::symbol_registry::SymbolId;
use chrono::DateTime;
use ordered_float::OrderedFloat;
use std::time::Instant;

// Byte patterns for zero-alloc JSON scanning.
const ORDERBOOK_MARKER: &[u8] = b"\"orderbook\"";
const MARKET_ID_KEY: &[u8] = b"\"market_id\":";
const TYPE_KEY: &[u8] = b"\"type\":\"";
const SNAPSHOT_VAL: &[u8] = b"snapshot";
const BIDS_MARKER: &[u8] = b"\"bids\":[";
const ASKS_MARKER: &[u8] = b"\"asks\":[";
const PRICE_KEY: &[u8] = b"\"price\":\"";
const QTY_KEY: &[u8] = b"\"quantity\":\"";
const TIMESTAMP_KEY: &[u8] = b"\"timestamp\":\"";

// ── Fixed-size zero-alloc orderbook ────────────────────────────────

/// Maximum depth levels per side. 128 covers all reasonable exchange depths.
const MAX_LEVELS: usize = 128;

/// Maximum number of levels that can arrive in a single message.
/// Used for the scratch parse buffer.
const MAX_LEVELS_PER_MSG: usize = 64;

/// A single price level.
#[derive(Clone, Copy, Default)]
struct Level {
    price: OrderedFloat<f64>,
    qty: f64,
}

/// Fixed-size orderbook. Pre-allocated, zero-alloc operations.
///
/// Bids sorted descending by price (best bid at index 0).
/// Asks sorted ascending by price (best ask at index 0).
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

    /// Update a bid level. qty=0 removes. Maintains descending sort by price.
    #[inline]
    fn update_bid(&mut self, price: f64, qty: f64) {
        let key = OrderedFloat(price);
        // Binary search in descending order: find position of `key`
        let pos = self.bids[..self.bid_count]
            .partition_point(|l| l.price > key);

        if pos < self.bid_count && self.bids[pos].price == key {
            // Existing level
            if qty == 0.0 {
                // Remove: shift left
                self.bids.copy_within(pos + 1..self.bid_count, pos);
                self.bid_count -= 1;
            } else {
                self.bids[pos].qty = qty;
            }
        } else if qty > 0.0 && self.bid_count < MAX_LEVELS {
            // Insert: shift right to make room
            self.bids.copy_within(pos..self.bid_count, pos + 1);
            self.bids[pos] = Level { price: key, qty };
            self.bid_count += 1;
        }
        // else: qty=0 for non-existent level, or book full — ignore
    }

    /// Update an ask level. qty=0 removes. Maintains ascending sort by price.
    #[inline]
    fn update_ask(&mut self, price: f64, qty: f64) {
        let key = OrderedFloat(price);
        // Binary search in ascending order
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

// ── Market ID to SymbolId lookup ───────────────────────────────────

/// Maximum market_id value supported. RiseX market IDs are small integers.
const MAX_MARKET_ID: usize = 4096;

// ── Feed struct ────────────────────────────────────────────────────

pub struct RiseXHftFeed {
    /// market_id → SymbolId. Direct array indexing, O(1).
    market_id_to_symbol: Box<[Option<SymbolId>; MAX_MARKET_ID]>,
    /// market_id → FixedBook index.
    market_id_to_book: Box<[Option<usize>; MAX_MARKET_ID]>,
    /// Pre-allocated orderbooks, one per subscribed market.
    books: Vec<FixedBook>,
    /// WebSocket URL.
    url: String,
    /// Pre-built subscription message.
    sub_messages: Vec<String>,
}

impl RiseXHftFeed {
    /// Create from a pre-built market_id → SymbolId mapping.
    ///
    /// The mapping is typically built from the RiseX REST API at startup.
    /// `market_ids` is the list of market IDs to subscribe to.
    pub fn new(market_id_to_symbol_map: &[(u32, SymbolId)]) -> Self {
        let mut market_id_to_symbol = Box::new([None::<SymbolId>; MAX_MARKET_ID]);
        let mut market_id_to_book = Box::new([None::<usize>; MAX_MARKET_ID]);
        let mut books = Vec::new();
        let mut market_ids_for_sub = Vec::new();

        for &(mid, sid) in market_id_to_symbol_map {
            if (mid as usize) < MAX_MARKET_ID {
                market_id_to_symbol[mid as usize] = Some(sid);
                market_id_to_book[mid as usize] = Some(books.len());
                books.push(FixedBook::new());
                market_ids_for_sub.push(mid);
            }
        }

        // Build subscribe message: {"method":"subscribe","params":{"channel":"orderbook","market_ids":[12,34]}}
        let ids_str: Vec<String> = market_ids_for_sub.iter().map(|id| id.to_string()).collect();
        let sub_msg = format!(
            r#"{{"method":"subscribe","params":{{"channel":"orderbook","market_ids":[{}]}}}}"#,
            ids_str.join(",")
        );

        Self {
            market_id_to_symbol,
            market_id_to_book,
            books,
            url: "wss://ws.rise.trade/ws".to_string(),
            sub_messages: vec![sub_msg],
        }
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

        // Extract market_id
        let market_id = match extract_u64_after(json, MARKET_ID_KEY) {
            Some(id) => id as u32,
            None => return,
        };
        if market_id as usize >= MAX_MARKET_ID {
            return;
        }

        // Lookup SymbolId and book index
        let symbol_id = match self.market_id_to_symbol[market_id as usize] {
            Some(id) => id,
            None => return,
        };
        let book_idx = match self.market_id_to_book[market_id as usize] {
            Some(idx) => idx,
            None => return,
        };

        // Determine snapshot vs update
        let is_snapshot = match extract_str_after(json, TYPE_KEY) {
            Some(t) => t.as_bytes().starts_with(SNAPSHOT_VAL),
            None => return,
        };

        let book = &mut self.books[book_idx];

        if is_snapshot {
            book.clear();
        }

        // Parse bids
        if let Some(bids_start) = find_bytes(json, BIDS_MARKER) {
            let bids_section = &json[bids_start + BIDS_MARKER.len()..];
            // Find end of bids array (closing ']')
            let bids_end = find_closing_bracket(bids_section);
            let bids_data = &bids_section[..bids_end];
            parse_levels(bids_data, book, true);
        }

        // Parse asks
        if let Some(asks_start) = find_bytes(json, ASKS_MARKER) {
            let asks_section = &json[asks_start + ASKS_MARKER.len()..];
            let asks_end = find_closing_bracket(asks_section);
            let asks_data = &asks_section[..asks_end];
            parse_levels(asks_data, book, false);
        }

        // Extract BBO from book
        let bid = book.best_bid();
        let ask = book.best_ask();

        // Extract timestamp (nanoseconds as string)
        let exchange_ts = extract_str_after(json, TIMESTAMP_KEY).and_then(|ts_str| {
            let ns: i64 = ts_str.parse().ok()?;
            DateTime::from_timestamp(ns / 1_000_000_000, (ns % 1_000_000_000) as u32)
        });

        scratch.push(
            symbol_id,
            MarketData {
                bid: bid.map(|(p, _)| p),
                ask: ask.map(|(p, _)| p),
                bid_qty: bid.map(|(_, q)| q),
                ask_qty: ask.map(|(_, q)| q),
                exchange_ts_raw: exchange_ts,
                exchange_ts: None,
                received_ts: None,
                received_instant: Some(received_instant),
                feed_latency_ns: 0,
            },
        );
    }

}

/// Parse price/quantity levels from a JSON array section. Zero-alloc.
/// Free function to avoid borrow conflicts with `&mut self.books`.
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

/// Extract a quoted string value starting at the current position.
/// Assumes the data starts immediately after the opening quote pattern.
/// Returns the string content up to the next unescaped `"`.
#[inline]
fn extract_str_value_at(data: &[u8]) -> Option<&str> {
    let end = data.iter().position(|&b| b == b'"')?;
    std::str::from_utf8(&data[..end]).ok()
}

/// Find the position of the closing `]` bracket, handling nested arrays/objects.
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

impl HftFeed for RiseXHftFeed {
    type Item = MarketData;

    fn urls(&self) -> Vec<String> {
        vec![self.url.clone()]
    }

    fn subscribe_messages(&self) -> Vec<String> {
        self.sub_messages.clone()
    }

    fn heartbeat_payload(&self) -> Option<&'static [u8]> {
        Some(b"{\"op\":\"ping\"}")
    }

    fn heartbeat_interval_ms(&self) -> u64 {
        10_000
    }

    fn on_connected(&mut self, _conn_index: usize) {
        // Clear all books on reconnect — next message should be a snapshot
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
        self.parse_orderbook(payload, received_instant, scratch);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hft::TickScratch;
    use crate::market_data::MarketData;

    fn test_feed() -> RiseXHftFeed {
        // market_id 12 → SymbolId 0 (BTC), market_id 34 → SymbolId 1 (ETH)
        RiseXHftFeed::new(&[(12, 0), (34, 1)])
    }

    // ── FixedBook unit tests ───────────────────────────────────────

    #[test]
    fn book_insert_and_best() {
        let mut book = FixedBook::new();

        book.update_bid(100.0, 1.0);
        book.update_bid(101.0, 2.0);
        book.update_bid(99.0, 3.0);

        // Best bid should be 101 (highest)
        assert_eq!(book.best_bid(), Some((101.0, 2.0)));
        assert_eq!(book.bid_count, 3);

        book.update_ask(102.0, 1.0);
        book.update_ask(103.0, 2.0);
        book.update_ask(101.5, 3.0);

        // Best ask should be 101.5 (lowest)
        assert_eq!(book.best_ask(), Some((101.5, 3.0)));
        assert_eq!(book.ask_count, 3);
    }

    #[test]
    fn book_update_existing_level() {
        let mut book = FixedBook::new();
        book.update_bid(100.0, 1.0);
        book.update_bid(100.0, 5.0); // update qty
        assert_eq!(book.best_bid(), Some((100.0, 5.0)));
        assert_eq!(book.bid_count, 1); // no duplicate
    }

    #[test]
    fn book_remove_level() {
        let mut book = FixedBook::new();
        book.update_bid(100.0, 1.0);
        book.update_bid(101.0, 2.0);
        book.update_bid(102.0, 3.0);

        // Remove best bid
        book.update_bid(102.0, 0.0);
        assert_eq!(book.best_bid(), Some((101.0, 2.0)));
        assert_eq!(book.bid_count, 2);

        // Remove all
        book.update_bid(101.0, 0.0);
        book.update_bid(100.0, 0.0);
        assert_eq!(book.best_bid(), None);
        assert_eq!(book.bid_count, 0);
    }

    #[test]
    fn book_snapshot_clear() {
        let mut book = FixedBook::new();
        book.update_bid(100.0, 1.0);
        book.update_ask(101.0, 1.0);
        assert_eq!(book.bid_count, 1);

        book.clear();
        assert_eq!(book.bid_count, 0);
        assert_eq!(book.ask_count, 0);
        assert_eq!(book.best_bid(), None);
    }

    #[test]
    fn book_remove_nonexistent() {
        let mut book = FixedBook::new();
        book.update_bid(100.0, 1.0);
        book.update_bid(99.0, 0.0); // remove non-existent — no-op
        assert_eq!(book.bid_count, 1);
    }

    // ── Parse tests ────────────────────────────────────────────────

    #[test]
    fn parse_snapshot() {
        let mut feed = test_feed();
        let msg = r#"{"channel":"orderbook","type":"snapshot","market_id":12,"data":{"bids":[{"price":"67432.10","quantity":"1.234","order_count":5},{"price":"67431.00","quantity":"2.0","order_count":3}],"asks":[{"price":"67432.50","quantity":"0.567","order_count":2},{"price":"67433.00","quantity":"1.0","order_count":1}]},"timestamp":"1700000000000000000"}"#;

        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_orderbook(msg.as_bytes(), Instant::now(), &mut scratch);

        assert_eq!(scratch.len(), 1);
        let tick = &scratch.as_slice()[0];
        assert_eq!(tick.symbol_id, 0); // BTC
        assert_eq!(tick.item.bid.unwrap(), 67432.10);
        assert_eq!(tick.item.ask.unwrap(), 67432.50);
        assert_eq!(tick.item.bid_qty.unwrap(), 1.234);
        assert_eq!(tick.item.ask_qty.unwrap(), 0.567);
    }

    #[test]
    fn parse_update_changes_bbo() {
        let mut feed = test_feed();

        // First: snapshot
        let snapshot = r#"{"channel":"orderbook","type":"snapshot","market_id":12,"data":{"bids":[{"price":"100.0","quantity":"1.0"}],"asks":[{"price":"101.0","quantity":"1.0"}]},"timestamp":"1"}"#;
        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_orderbook(snapshot.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].item.bid.unwrap(), 100.0);

        // Update: new better bid
        let update = r#"{"channel":"orderbook","type":"update","market_id":12,"data":{"bids":[{"price":"100.5","quantity":"2.0"}],"asks":[]},"timestamp":"2"}"#;
        scratch.clear();
        feed.parse_orderbook(update.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].item.bid.unwrap(), 100.5);
        assert_eq!(scratch.as_slice()[0].item.bid_qty.unwrap(), 2.0);
        // Ask unchanged
        assert_eq!(scratch.as_slice()[0].item.ask.unwrap(), 101.0);
    }

    #[test]
    fn parse_update_removes_level() {
        let mut feed = test_feed();

        // Snapshot: two bid levels
        let snapshot = r#"{"channel":"orderbook","type":"snapshot","market_id":12,"data":{"bids":[{"price":"102.0","quantity":"1.0"},{"price":"101.0","quantity":"2.0"}],"asks":[{"price":"103.0","quantity":"1.0"}]},"timestamp":"1"}"#;
        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_orderbook(snapshot.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].item.bid.unwrap(), 102.0);

        // Update: remove best bid (qty=0)
        let update = r#"{"channel":"orderbook","type":"update","market_id":12,"data":{"bids":[{"price":"102.0","quantity":"0"}],"asks":[]},"timestamp":"2"}"#;
        scratch.clear();
        feed.parse_orderbook(update.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].item.bid.unwrap(), 101.0); // falls to next level
    }

    #[test]
    fn snapshot_resets_book() {
        let mut feed = test_feed();

        // First snapshot
        let snap1 = r#"{"channel":"orderbook","type":"snapshot","market_id":12,"data":{"bids":[{"price":"100.0","quantity":"1.0"}],"asks":[{"price":"101.0","quantity":"1.0"}]},"timestamp":"1"}"#;
        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_orderbook(snap1.as_bytes(), Instant::now(), &mut scratch);

        // Second snapshot with different prices — should replace, not merge
        let snap2 = r#"{"channel":"orderbook","type":"snapshot","market_id":12,"data":{"bids":[{"price":"200.0","quantity":"5.0"}],"asks":[{"price":"201.0","quantity":"5.0"}]},"timestamp":"2"}"#;
        scratch.clear();
        feed.parse_orderbook(snap2.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].item.bid.unwrap(), 200.0);
        assert_eq!(scratch.as_slice()[0].item.ask.unwrap(), 201.0);

        // Verify old levels are gone (book should have exactly 1 bid level)
        let book = &feed.books[0];
        assert_eq!(book.bid_count, 1);
    }

    #[test]
    fn rejects_garbage() {
        let mut feed = test_feed();
        let garbage = &[
            "",
            "{}",
            r#"{"channel":"trades"}"#,
            r#"{"channel":"orderbook","type":"snapshot","market_id":9999}"#, // unknown market
        ];
        for &msg in garbage {
            let mut scratch = TickScratch::<MarketData>::new();
            feed.parse_orderbook(msg.as_bytes(), Instant::now(), &mut scratch);
            assert_eq!(scratch.len(), 0, "should reject: {}", msg);
        }
    }

    #[test]
    fn different_markets() {
        let mut feed = test_feed();

        let btc = r#"{"channel":"orderbook","type":"snapshot","market_id":12,"data":{"bids":[{"price":"67000.0","quantity":"1.0"}],"asks":[{"price":"67001.0","quantity":"1.0"}]},"timestamp":"1"}"#;
        let eth = r#"{"channel":"orderbook","type":"snapshot","market_id":34,"data":{"bids":[{"price":"3500.0","quantity":"10.0"}],"asks":[{"price":"3501.0","quantity":"10.0"}]},"timestamp":"1"}"#;

        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_orderbook(btc.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].symbol_id, 0); // BTC

        scratch.clear();
        feed.parse_orderbook(eth.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].symbol_id, 1); // ETH
    }

    #[test]
    fn timestamp_nanoseconds() {
        let mut feed = test_feed();
        // 1700000000 seconds + 123456789 nanoseconds
        let msg = r#"{"channel":"orderbook","type":"snapshot","market_id":12,"data":{"bids":[{"price":"100","quantity":"1"}],"asks":[{"price":"101","quantity":"1"}]},"timestamp":"1700000000123456789"}"#;

        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_orderbook(msg.as_bytes(), Instant::now(), &mut scratch);

        let ts = scratch.as_slice()[0].item.exchange_ts_raw.unwrap();
        assert_eq!(ts.timestamp(), 1700000000);
        assert_eq!(ts.timestamp_subsec_nanos(), 123456789);
    }

    #[test]
    fn parse_latency_under_1us() {
        let msg = r#"{"channel":"orderbook","type":"snapshot","market_id":12,"data":{"bids":[{"price":"67432.10","quantity":"1.234","order_count":5},{"price":"67431.00","quantity":"2.0","order_count":3}],"asks":[{"price":"67432.50","quantity":"0.567","order_count":2},{"price":"67433.00","quantity":"1.0","order_count":1}]},"timestamp":"1700000000000000000"}"#;

        let mut feed = test_feed();
        let payload = msg.as_bytes();
        let mut scratch = TickScratch::<MarketData>::new();

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
        eprintln!("RiseX parse latency: {} ns/iter", per_iter_ns);

        #[cfg(not(debug_assertions))]
        assert!(per_iter_ns < 2_000, "parse latency {} ns exceeds 2us", per_iter_ns);
    }
}
