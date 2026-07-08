/// Zero-allocation Bulk Trade orderbook parser.
///
/// Subscribes to l2Delta for initial snapshot + real-time deltas.
/// Wire format (l2Delta):
/// ```json
/// {"type":"l2delta","data":{"updateType":"snapshot","symbol":"BTC-USD","levels":[[{"px":102490,"sz":1.5,"n":3}],[{"px":102500,"sz":2.0,"n":5}]],"timestamp":1704067200000}}
/// ```
/// Delta: same format, sz=0 means remove level.
/// levels[0] = bids (highest to lowest), levels[1] = asks (lowest to highest)
///
/// URL: `wss://exchange-ws1.bulk.trade`
use crate::hft::{HftFeed, TickScratch, find_bytes, parse_f64};
use crate::market_data::MarketData;
use crate::symbol_registry::SymbolId;
use ordered_float::OrderedFloat;
use std::time::Instant;

// Byte patterns for zero-alloc JSON scanning.
const L2DELTA_TYPE: &[u8] = b"\"l2delta\"";
const L2SNAPSHOT_TYPE: &[u8] = b"\"l2snapshot\"";
const TICKER_TYPE: &[u8] = b"\"ticker\"";
const SYMBOL_KEY: &[u8] = b"\"symbol\":\"";
const UPDATE_TYPE_KEY: &[u8] = b"\"updateType\":\"";
const SNAPSHOT_VAL: &[u8] = b"snapshot";
const LEVELS_KEY: &[u8] = b"\"levels\":";
const PX_KEY: &[u8] = b"\"px\":";
const SZ_KEY: &[u8] = b"\"sz\":";

// ── Fixed-size zero-alloc orderbook ────────────────────────────────

const MAX_LEVELS: usize = 128;

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
        let pos = self.bids[..self.bid_count].partition_point(|l| l.price > key);
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
        let pos = self.asks[..self.ask_count].partition_point(|l| l.price < key);
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
const MAX_SYM_LEN: usize = 16;

#[derive(Clone, Copy)]
struct SymEntry {
    name: [u8; MAX_SYM_LEN],
    len: u8,
    symbol_id: SymbolId,
    book_idx: u8,
}

// ── Feed struct ────────────────────────────────────────────────────

pub struct BulkHftFeed {
    symbols: [SymEntry; MAX_SYMBOLS],
    symbol_count: usize,
    books: Vec<FixedBook>,
    sub_message: String,
    ws_url: String,
}

impl BulkHftFeed {
    /// Create from a list of (native_symbol, SymbolId) pairs.
    /// `native_symbol` is e.g. "BTC-USD", "ETH-USD".
    pub fn new(symbol_map: &[(&str, SymbolId)]) -> Self {
        Self::with_url(symbol_map, "wss://exchange-ws1.bulk.trade")
    }

    pub fn with_url(symbol_map: &[(&str, SymbolId)], ws_url: &str) -> Self {
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

        // Build subscribe message — l2Delta for orderbook
        let mut subs: Vec<String> = symbol_map.iter().take(count)
            .map(|(native, _)| format!(r#"{{"type":"l2Delta","symbol":"{}"}}"#, native))
            .collect();
        // Also subscribe ticker for mark/funding
        for (native, _) in symbol_map.iter().take(count) {
            subs.push(format!(r#"{{"type":"ticker","symbol":"{}"}}"#, native));
        }
        let sub_message = format!(
            r#"{{"method":"subscribe","subscription":[{}]}}"#,
            subs.join(",")
        );

        Self {
            symbols,
            symbol_count: count,
            books,
            sub_message,
            ws_url: ws_url.to_string(),
        }
    }

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

    #[inline]
    fn parse_message(
        &mut self,
        json: &[u8],
        received_instant: Instant,
        scratch: &mut TickScratch<MarketData>,
    ) {
        // Route by message type
        if find_bytes(json, L2DELTA_TYPE).is_some() || find_bytes(json, L2SNAPSHOT_TYPE).is_some() {
            self.parse_l2(json, received_instant, scratch);
        } else if find_bytes(json, TICKER_TYPE).is_some() {
            self.parse_ticker(json, received_instant, scratch);
        }
    }

    /// Parse l2Delta/l2Snapshot message.
    /// Format: {"type":"l2delta","data":{"updateType":"snapshot|delta","symbol":"BTC-USD",
    ///          "levels":[[{bids}],[{asks}]],"timestamp":...}}
    #[inline]
    fn parse_l2(
        &mut self,
        json: &[u8],
        received_instant: Instant,
        scratch: &mut TickScratch<MarketData>,
    ) {
        // Extract symbol from the data section
        let sym_bytes = match extract_symbol(json) {
            Some(s) => s,
            None => return,
        };

        let (symbol_id, book_idx) = match self.lookup(sym_bytes) {
            Some(v) => v,
            None => return,
        };

        // Determine if snapshot
        let is_snapshot = match find_bytes(json, UPDATE_TYPE_KEY) {
            Some(pos) => {
                let after = pos + UPDATE_TYPE_KEY.len();
                json[after..].starts_with(SNAPSHOT_VAL)
            }
            None => false,
        };

        let book = &mut self.books[book_idx];
        if is_snapshot {
            book.clear();
        }

        // Find "levels": and parse the two arrays [bids_array, asks_array]
        let Some(levels_pos) = find_bytes(json, LEVELS_KEY) else { return };
        let levels_start = levels_pos + LEVELS_KEY.len();

        // Find the outer array start '['
        let Some(outer_start) = json[levels_start..].iter().position(|&b| b == b'[') else { return };
        let outer = &json[levels_start + outer_start + 1..]; // skip outer '['

        // Parse bids array (first inner array)
        if let Some((bids_data, rest)) = extract_inner_array(outer) {
            parse_levels_numeric(bids_data, book, true);
            // Parse asks array (second inner array)
            if let Some((asks_data, _)) = extract_inner_array(rest) {
                parse_levels_numeric(asks_data, book, false);
            }
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

    /// Parse ticker message for exchange_ts.
    /// Format: {"type":"ticker","data":{"ticker":{"lastPrice":...,"markPrice":...,"timestamp":...}}}
    #[inline]
    fn parse_ticker(
        &mut self,
        json: &[u8],
        received_instant: Instant,
        scratch: &mut TickScratch<MarketData>,
    ) {
        // Extract symbol
        let sym_bytes = match extract_symbol(json) {
            Some(s) => s,
            None => return,
        };

        let (symbol_id, book_idx) = match self.lookup(sym_bytes) {
            Some(v) => v,
            None => return,
        };

        // Extract lastPrice for a quick BBO-like update if book is empty
        let book = &self.books[book_idx];
        let bid = book.best_bid();
        let ask = book.best_ask();

        // Only emit if we have book data (ticker alone isn't enough for BBO)
        if bid.is_some() || ask.is_some() {
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
}

/// Extract symbol string bytes from JSON containing "symbol":"..."
#[inline]
fn extract_symbol(json: &[u8]) -> Option<&[u8]> {
    let pos = find_bytes(json, SYMBOL_KEY)?;
    let start = pos + SYMBOL_KEY.len();
    let end = json[start..].iter().position(|&b| b == b'"')?;
    Some(&json[start..start + end])
}

/// Extract the content of the first [...] inner array, returning (content, rest_after_array).
#[inline]
fn extract_inner_array(data: &[u8]) -> Option<(&[u8], &[u8])> {
    // Find opening '['
    let open = data.iter().position(|&b| b == b'[')?;
    let inner = &data[open + 1..];

    // Find matching close ']'
    let mut depth = 1i32;
    for (i, &b) in inner.iter().enumerate() {
        match b {
            b'[' => depth += 1,
            b']' => {
                depth -= 1;
                if depth == 0 {
                    let content = &inner[..i];
                    let rest = &inner[i + 1..];
                    return Some((content, rest));
                }
            }
            _ => {}
        }
    }
    None
}

/// Parse `{"px":...,"sz":...,"n":...}` objects from array content (numeric values, not strings).
#[inline]
fn parse_levels_numeric(data: &[u8], book: &mut FixedBook, is_bid: bool) {
    let mut pos = 0;
    let mut count = 0;

    while pos < data.len() && count < 64 {
        // Find "px":
        let px_match = match find_bytes(&data[pos..], PX_KEY) {
            Some(p) => p,
            None => break,
        };
        let px_start = pos + px_match + PX_KEY.len();
        let price = match extract_number_at(&data[px_start..]) {
            Some(v) => v,
            None => { pos = px_start; continue; }
        };

        // Find "sz":
        let sz_match = match find_bytes(&data[px_start..], SZ_KEY) {
            Some(p) => p,
            None => break,
        };
        let sz_start = px_start + sz_match + SZ_KEY.len();
        let qty = extract_number_at(&data[sz_start..]).unwrap_or(0.0);

        if is_bid {
            book.update_bid(price, qty);
        } else {
            book.update_ask(price, qty);
        }

        pos = sz_start;
        count += 1;
    }
}

/// Extract a number (f64) starting at the current position until a non-numeric char.
/// Handles both integer and decimal, with optional leading quote.
#[inline]
fn extract_number_at(data: &[u8]) -> Option<f64> {
    let mut start = 0;
    // Skip optional quote
    if !data.is_empty() && data[0] == b'"' {
        start = 1;
    }
    let mut end = start;
    while end < data.len() {
        let b = data[end];
        if b.is_ascii_digit() || b == b'.' || b == b'-' || b == b'e' || b == b'E' || b == b'+' {
            end += 1;
        } else {
            break;
        }
    }
    if end == start { return None; }
    let s = std::str::from_utf8(&data[start..end]).ok()?;
    parse_f64(s)
}


impl HftFeed for BulkHftFeed {
    type Item = MarketData;

    fn urls(&self) -> Vec<String> {
        vec![self.ws_url.clone()]
    }

    fn subscribe_messages(&self) -> Vec<String> {
        vec![self.sub_message.clone()]
    }

    fn heartbeat_payload(&self) -> Option<&'static [u8]> {
        None // server sends WS pings, client responds with pong (handled by framer)
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
        self.parse_message(payload, received_instant, scratch);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_feed() -> BulkHftFeed {
        BulkHftFeed::new(&[
            ("BTC-USD", 0),
            ("ETH-USD", 1),
        ])
    }

    #[test]
    fn parse_l2_snapshot() {
        let mut feed = test_feed();
        let msg = r#"{"type":"l2delta","data":{"updateType":"snapshot","symbol":"BTC-USD","levels":[[{"px":102490,"sz":1.5,"n":3},{"px":102480,"sz":2.0,"n":5}],[{"px":102500,"sz":1.0,"n":2},{"px":102510,"sz":0.5,"n":1}]],"timestamp":1704067200000}}"#;

        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_message(msg.as_bytes(), Instant::now(), &mut scratch);

        assert_eq!(scratch.len(), 1);
        let tick = &scratch.as_slice()[0];
        assert_eq!(tick.symbol_id, 0);
        assert_eq!(tick.item.bid.unwrap(), 102490.0);
        assert_eq!(tick.item.ask.unwrap(), 102500.0);
        assert_eq!(tick.item.bid_qty.unwrap(), 1.5);
        assert_eq!(tick.item.ask_qty.unwrap(), 1.0);
    }

    #[test]
    fn parse_l2_delta_update() {
        let mut feed = test_feed();

        // Snapshot
        let snap = r#"{"type":"l2delta","data":{"updateType":"snapshot","symbol":"BTC-USD","levels":[[{"px":100,"sz":1.0,"n":1}],[{"px":101,"sz":1.0,"n":1}]],"timestamp":1}}"#;
        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_message(snap.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].item.bid.unwrap(), 100.0);

        // Delta: add better bid
        let delta = r#"{"type":"l2delta","data":{"updateType":"delta","symbol":"BTC-USD","levels":[[{"px":100.5,"sz":2.0,"n":1}],[]],"timestamp":2}}"#;
        scratch.clear();
        feed.parse_message(delta.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].item.bid.unwrap(), 100.5);
        assert_eq!(scratch.as_slice()[0].item.ask.unwrap(), 101.0); // unchanged
    }

    #[test]
    fn parse_l2_delta_remove() {
        let mut feed = test_feed();

        let snap = r#"{"type":"l2delta","data":{"updateType":"snapshot","symbol":"BTC-USD","levels":[[{"px":102,"sz":1.0,"n":1},{"px":101,"sz":2.0,"n":1}],[{"px":103,"sz":1.0,"n":1}]],"timestamp":1}}"#;
        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_message(snap.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].item.bid.unwrap(), 102.0);

        // Remove best bid
        let delta = r#"{"type":"l2delta","data":{"updateType":"delta","symbol":"BTC-USD","levels":[[{"px":102,"sz":0,"n":0}],[]],"timestamp":2}}"#;
        scratch.clear();
        feed.parse_message(delta.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].item.bid.unwrap(), 101.0);
    }

    #[test]
    fn parse_different_symbols() {
        let mut feed = test_feed();

        let btc = r#"{"type":"l2delta","data":{"updateType":"snapshot","symbol":"BTC-USD","levels":[[{"px":67000,"sz":1.0,"n":1}],[{"px":67001,"sz":1.0,"n":1}]],"timestamp":1}}"#;
        let eth = r#"{"type":"l2delta","data":{"updateType":"snapshot","symbol":"ETH-USD","levels":[[{"px":3500,"sz":10.0,"n":1}],[{"px":3501,"sz":10.0,"n":1}]],"timestamp":1}}"#;

        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_message(btc.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].symbol_id, 0);

        scratch.clear();
        feed.parse_message(eth.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].symbol_id, 1);
    }

    #[test]
    fn reconnect_clears_books() {
        let mut feed = test_feed();
        let snap = r#"{"type":"l2delta","data":{"updateType":"snapshot","symbol":"BTC-USD","levels":[[{"px":100,"sz":1.0,"n":1}],[{"px":101,"sz":1.0,"n":1}]],"timestamp":1}}"#;
        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_message(snap.as_bytes(), Instant::now(), &mut scratch);

        feed.on_connected(0);
        assert_eq!(feed.books[0].bid_count, 0);
    }

    #[test]
    fn subscribe_message_format() {
        let feed = BulkHftFeed::new(&[("BTC-USD", 0), ("ETH-USD", 1)]);
        let msgs = feed.subscribe_messages();
        assert_eq!(msgs.len(), 1);
        assert!(msgs[0].contains(r#""method":"subscribe""#));
        assert!(msgs[0].contains(r#""type":"l2Delta""#));
        assert!(msgs[0].contains(r#""symbol":"BTC-USD""#));
        assert!(msgs[0].contains(r#""type":"ticker""#));
    }

    #[test]
    fn rejects_unknown_symbol() {
        let mut feed = test_feed();
        let msg = r#"{"type":"l2delta","data":{"updateType":"snapshot","symbol":"XYZ-USD","levels":[[{"px":100,"sz":1.0,"n":1}],[{"px":101,"sz":1.0,"n":1}]],"timestamp":1}}"#;
        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_message(msg.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.len(), 0);
    }
}
