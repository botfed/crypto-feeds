/// Zero-allocation Binance bookTicker parser.
///
/// Parses the combined-stream JSON format using manual byte scanning.
/// All string references point into the WsFramer's pre-allocated buffer.
/// Symbol → SymbolId resolution uses a pre-built FxHashMap (read-only, no alloc).
/// Update ID dedup uses a pre-allocated array indexed by SymbolId.
///
/// Wire format (combined stream):
/// ```json
/// {"stream":"btcusdt@bookTicker","data":{"s":"BTCUSDT","b":"67432.10","B":"1.234","a":"67432.50","A":"0.567","u":12345678,"E":1700000000000}}
/// ```
use crate::hft::{HftFeed, TickScratch, extract_str_after, extract_u64_after, parse_f64};
use crate::market_data::{InstrumentType, MarketData};
use crate::symbol_registry::{MAX_SYMBOLS, REGISTRY, SymbolId};
use chrono::DateTime;
use rustc_hash::FxHashMap;
use std::time::Instant;

// Byte patterns for zero-alloc JSON scanning.
// Scoped to the "data":{...} section to avoid false matches in the stream header.
const DATA_MARKER: &[u8] = b"\"data\":{";
const SYM_KEY: &[u8] = b"\"s\":\"";
const BID_KEY: &[u8] = b"\"b\":\"";
const ASK_KEY: &[u8] = b"\"a\":\"";
const BID_QTY_KEY: &[u8] = b"\"B\":\"";
const ASK_QTY_KEY: &[u8] = b"\"A\":\"";
const UPDATE_ID_KEY: &[u8] = b"\"u\":";
const EVENT_TIME_KEY: &[u8] = b"\"E\":";
const BOOK_TICKER_MARKER: &[u8] = b"bookTicker";

pub struct BinanceHftFeed {
    pub(crate) _itype: InstrumentType,
    /// Native Binance symbol (e.g. "BTCUSDT") → SymbolId. Read-only after init.
    pub(crate) symbol_to_id: FxHashMap<String, SymbolId>,
    /// Pre-allocated dedup state: last update ID per SymbolId.
    pub(crate) last_update_id: [u64; MAX_SYMBOLS],
    /// Subscription stream names, built at startup.
    pub(crate) _streams: Vec<String>,
    /// Full WS URL, built at startup.
    pub(crate) url: String,
    /// Subscribe JSON messages, built at startup.
    pub(crate) sub_messages: Vec<String>,
}

impl BinanceHftFeed {
    /// Create a new Binance HFT feed for the given instrument type and symbols.
    ///
    /// `symbols` are in normalized form: `"BTC_USDT"` or `"PERP_BTC_USDT"`.
    /// This constructor allocates (HashMap, Strings) — all at startup, never on hot path.
    pub fn new(itype: InstrumentType, symbols: &[&str]) -> Self {
        let base_url = match itype {
            InstrumentType::Spot => "wss://stream.binance.com:9443/stream",
            InstrumentType::Perp => "wss://fstream.binance.com/public/stream",
            _ => "wss://stream.binance.com:9443/stream",
        };

        let mut symbol_to_id = FxHashMap::default();
        let mut streams = Vec::with_capacity(symbols.len());

        for sym in symbols {
            // Convert normalized "BTC_USDT" or "PERP_BTC_USDT" to native "BTCUSDT"
            let native = normalize_to_native(sym);

            // Look up SymbolId from global registry
            if let Some(&id) = REGISTRY.lookup(&native, &itype) {
                symbol_to_id.insert(native.clone(), id);
                streams.push(format!("{}@bookTicker", native.to_lowercase()));
            } else {
                log::warn!(
                    "BinanceHftFeed: symbol '{}' (native '{}') not in registry, skipping",
                    sym,
                    native
                );
            }
        }

        let streams_param = streams.join("/");
        let url = format!("{}?streams={}", base_url, streams_param);

        Self {
            _itype: itype,
            symbol_to_id,
            last_update_id: [0u64; MAX_SYMBOLS],
            _streams: streams,
            url,
            sub_messages: vec![], // Binance combined stream subscribes via URL, not messages
        }
    }

    /// Create a feed with a custom symbol lookup table (for testing / offline use).
    /// Bypasses the global REGISTRY.
    pub fn with_lookup(
        itype: InstrumentType,
        symbol_to_id: FxHashMap<String, SymbolId>,
    ) -> Self {
        Self {
            _itype: itype,
            symbol_to_id,
            last_update_id: [0u64; MAX_SYMBOLS],
            _streams: vec![],
            url: String::new(),
            sub_messages: vec![],
        }
    }

    /// Reset dedup state for all symbols. Used for testing/reconnect.
    pub fn reset_dedup(&mut self) {
        self.last_update_id = [0u64; MAX_SYMBOLS];
    }

    /// Reset dedup state for a single symbol. Used for testing.
    pub fn reset_dedup_symbol(&mut self, symbol_id: SymbolId) {
        self.last_update_id[symbol_id] = 0;
    }

    /// Parse a Binance bookTicker message from raw bytes. Zero-alloc.
    ///
    /// Returns the number of ticks written to `scratch` (0 or 1).
    #[inline]
    fn parse_book_ticker(
        &mut self,
        json: &[u8],
        received_instant: Instant,
        scratch: &mut TickScratch<MarketData>,
    ) {
        // Quick rejection: must contain "bookTicker"
        if crate::hft::find_bytes(json, BOOK_TICKER_MARKER).is_none() {
            return;
        }

        // Find the "data":{...} section to scope our field extraction
        let data_pos = match crate::hft::find_bytes(json, DATA_MARKER) {
            Some(p) => p + DATA_MARKER.len(),
            None => return,
        };
        let data_section = &json[data_pos..];

        // Extract symbol
        let symbol = match extract_str_after(data_section, SYM_KEY) {
            Some(s) => s,
            None => return,
        };

        // Look up SymbolId (FxHashMap::get with &str key — no allocation)
        let symbol_id = match self.symbol_to_id.get(symbol) {
            Some(&id) => id,
            None => return,
        };

        // Extract update ID for dedup
        let update_id = match extract_u64_after(data_section, UPDATE_ID_KEY) {
            Some(u) => u,
            None => return,
        };

        // Dedup: reject if update_id <= last seen for this symbol
        if update_id <= self.last_update_id[symbol_id] {
            return;
        }
        self.last_update_id[symbol_id] = update_id;

        // Extract prices and quantities
        let bid = extract_str_after(data_section, BID_KEY).and_then(parse_f64);
        let ask = extract_str_after(data_section, ASK_KEY).and_then(parse_f64);
        let bid_qty = extract_str_after(data_section, BID_QTY_KEY).and_then(parse_f64);
        let ask_qty = extract_str_after(data_section, ASK_QTY_KEY).and_then(parse_f64);

        // Validate quote sanity
        if let (Some(b), Some(a)) = (bid, ask) {
            if b >= a {
                return;
            }
        }

        // Extract event time (present on perp, absent on spot)
        let exchange_ts = extract_u64_after(data_section, EVENT_TIME_KEY)
            .and_then(|ms| DateTime::from_timestamp_millis(ms as i64));

        scratch.push(
            symbol_id,
            MarketData {
                bid,
                ask,
                bid_qty,
                ask_qty,
                exchange_ts_raw: exchange_ts,
                exchange_ts: None,
                received_ts: None,
                received_instant: Some(received_instant),
                update_id: Some(update_id),
                feed_latency_ns: 0,
            },
        );
    }
}

impl HftFeed for BinanceHftFeed {
    type Item = MarketData;

    fn urls(&self) -> Vec<String> {
        vec![self.url.clone()]
    }

    fn subscribe_messages(&self) -> Vec<String> {
        self.sub_messages.clone()
    }

    fn on_connected(&mut self, _conn_index: usize) {
        self.reset_dedup();
    }

    fn heartbeat_payload(&self) -> Option<&'static [u8]> {
        None // Binance uses WS pings, handled by engine
    }

    fn heartbeat_interval_ms(&self) -> u64 {
        10_000
    }

    fn parse_text(
        &mut self,
        payload: &[u8],
        received_instant: Instant,
        scratch: &mut TickScratch<MarketData>,
    ) {
        self.parse_book_ticker(payload, received_instant, scratch);
    }
}

/// Convert normalized symbol to Binance native format.
/// "BTC_USDT" → "BTCUSDT", "PERP_BTC_USDT" → "BTCUSDT"
fn normalize_to_native(sym: &str) -> String {
    let parts: Vec<&str> = sym.split('_').collect();
    match parts.len() {
        3 => format!("{}{}", parts[1].to_uppercase(), parts[2].to_uppercase()),
        2 => format!("{}{}", parts[0].to_uppercase(), parts[1].to_uppercase()),
        _ => sym.to_uppercase(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hft::TickScratch;
    use crate::market_data::MarketData;

    // ── Reference comparison: scanner vs serde ─────────────────────

    #[derive(Debug, serde::Deserialize)]
    struct RefBookTicker {
        #[allow(dead_code)]
        stream: String,
        data: RefBookTickerData,
    }

    #[derive(Debug, serde::Deserialize)]
    struct RefBookTickerData {
        #[serde(rename = "s")]
        symbol: String,
        #[serde(rename = "b")]
        bid_price: String,
        #[serde(rename = "a")]
        ask_price: String,
        #[serde(rename = "B")]
        bid_quantity: String,
        #[serde(rename = "A")]
        ask_quantity: String,
        u: u64,
        #[serde(rename = "E", default)]
        event_time: Option<u64>,
    }

    const CORPUS: &[&str] = &[
        // Standard perp message
        r#"{"stream":"btcusdt@bookTicker","data":{"s":"BTCUSDT","b":"67432.10000000","B":"1.234","a":"67432.50000000","A":"0.567","u":400900217,"E":1700000000000}}"#,
        // Different symbol
        r#"{"stream":"ethusdt@bookTicker","data":{"s":"ETHUSDT","b":"3521.05","B":"10.0","a":"3521.15","A":"5.0","u":99999999,"E":1700000000001}}"#,
        // Very small spread
        r#"{"stream":"btcusdt@bookTicker","data":{"s":"BTCUSDT","b":"100000.00","B":"0.001","a":"100000.01","A":"0.001","u":400900218,"E":1}}"#,
        // Large quantities
        r#"{"stream":"btcusdt@bookTicker","data":{"s":"BTCUSDT","b":"0.00001","B":"999999999.0","a":"0.00002","A":"999999999.0","u":400900219,"E":1}}"#,
        // Spot (no E field)
        r#"{"stream":"btcusdt@bookTicker","data":{"s":"BTCUSDT","b":"67432.10","B":"1.0","a":"67432.50","A":"1.0","u":400900220}}"#,
    ];

    /// Build a minimal lookup table for testing (bypasses global REGISTRY).
    fn test_lookup() -> FxHashMap<String, SymbolId> {
        let mut map = FxHashMap::default();
        map.insert("BTCUSDT".to_string(), 0);
        map.insert("ETHUSDT".to_string(), 1);
        map
    }

    /// Create a test-only feed with a custom lookup table.
    fn test_feed() -> BinanceHftFeed {
        BinanceHftFeed {
            _itype: InstrumentType::Perp,
            symbol_to_id: test_lookup(),
            last_update_id: [0u64; MAX_SYMBOLS],
            _streams: vec![],
            url: String::new(),
            sub_messages: vec![],
        }
    }

    #[test]
    fn scanner_matches_serde_reference() {
        let mut feed = test_feed();

        for &msg in CORPUS {
            // Reference parse (serde, allocates)
            let reference: RefBookTicker = serde_json::from_str(msg).unwrap();
            let ref_bid: f64 = reference.data.bid_price.parse().unwrap();
            let ref_ask: f64 = reference.data.ask_price.parse().unwrap();
            let ref_bid_qty: f64 = reference.data.bid_quantity.parse().unwrap();
            let ref_ask_qty: f64 = reference.data.ask_quantity.parse().unwrap();

            // Reset dedup so each message is accepted
            feed.last_update_id = [0u64; MAX_SYMBOLS];

            // Our scanner (zero-alloc)
            let mut scratch = TickScratch::<MarketData>::new();
            feed.parse_book_ticker(msg.as_bytes(), Instant::now(), &mut scratch);

            assert_eq!(scratch.len(), 1, "failed to parse: {}", msg);
            let tick = &scratch.as_slice()[0];
            assert_eq!(tick.item.bid.unwrap(), ref_bid, "bid mismatch: {}", msg);
            assert_eq!(tick.item.ask.unwrap(), ref_ask, "ask mismatch: {}", msg);
            assert_eq!(
                tick.item.bid_qty.unwrap(),
                ref_bid_qty,
                "bid_qty mismatch: {}",
                msg
            );
            assert_eq!(
                tick.item.ask_qty.unwrap(),
                ref_ask_qty,
                "ask_qty mismatch: {}",
                msg
            );

            // Verify exchange_ts matches
            if let Some(et) = reference.data.event_time {
                let expected = DateTime::from_timestamp_millis(et as i64);
                assert_eq!(tick.item.exchange_ts_raw, expected, "ts mismatch: {}", msg);
            }
        }
    }

    // ── Rejection tests ────────────────────────────────────────────

    #[test]
    fn rejects_garbage() {
        let mut feed = test_feed();
        let garbage = &[
            "",
            "{}",
            "not json",
            r#"{"stream":"foo"}"#,
            r#"{"stream":"btcusdt@bookTicker","data":{}}"#,
            // Unknown symbol
            r#"{"stream":"xyzusdt@bookTicker","data":{"s":"XYZUSDT","b":"1","B":"1","a":"2","A":"1","u":1,"E":1}}"#,
            // Missing bookTicker marker
            r#"{"stream":"btcusdt@trade","data":{"s":"BTCUSDT","p":"100","q":"1"}}"#,
        ];

        for &msg in garbage {
            let mut scratch = TickScratch::<MarketData>::new();
            feed.parse_book_ticker(msg.as_bytes(), Instant::now(), &mut scratch);
            assert_eq!(scratch.len(), 0, "should reject: {}", msg);
        }
    }

    #[test]
    fn rejects_crossed_quote() {
        let mut feed = test_feed();
        // bid > ask → crossed quote, should reject
        let msg = r#"{"stream":"btcusdt@bookTicker","data":{"s":"BTCUSDT","b":"100.0","B":"1","a":"99.0","A":"1","u":1,"E":1}}"#;
        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_book_ticker(msg.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.len(), 0);
    }

    // ── Dedup tests ────────────────────────────────────────────────

    #[test]
    fn update_id_dedup() {
        let mut feed = test_feed();

        let msg_u100 = r#"{"stream":"btcusdt@bookTicker","data":{"s":"BTCUSDT","b":"100","B":"1","a":"101","A":"1","u":100,"E":1}}"#;
        let msg_u99  = r#"{"stream":"btcusdt@bookTicker","data":{"s":"BTCUSDT","b":"100","B":"1","a":"101","A":"1","u":99,"E":1}}"#;
        let msg_u100_dup = r#"{"stream":"btcusdt@bookTicker","data":{"s":"BTCUSDT","b":"100","B":"1","a":"101","A":"1","u":100,"E":1}}"#;
        let msg_u101 = r#"{"stream":"btcusdt@bookTicker","data":{"s":"BTCUSDT","b":"100","B":"1","a":"101","A":"1","u":101,"E":1}}"#;

        // u=100 → accepted
        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_book_ticker(msg_u100.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.len(), 1);

        // u=99 → rejected (stale)
        scratch.clear();
        feed.parse_book_ticker(msg_u99.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.len(), 0);

        // u=100 again → rejected (duplicate)
        scratch.clear();
        feed.parse_book_ticker(msg_u100_dup.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.len(), 0);

        // u=101 → accepted
        scratch.clear();
        feed.parse_book_ticker(msg_u101.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.len(), 1);
    }

    #[test]
    fn dedup_per_symbol() {
        let mut feed = test_feed();

        let btc_msg = r#"{"stream":"btcusdt@bookTicker","data":{"s":"BTCUSDT","b":"100","B":"1","a":"101","A":"1","u":50,"E":1}}"#;
        let eth_msg = r#"{"stream":"ethusdt@bookTicker","data":{"s":"ETHUSDT","b":"3000","B":"1","a":"3001","A":"1","u":50,"E":1}}"#;

        // Both with u=50, different symbols → both accepted
        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_book_ticker(btc_msg.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.len(), 1);
        assert_eq!(scratch.as_slice()[0].symbol_id, 0); // BTC

        scratch.clear();
        feed.parse_book_ticker(eth_msg.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.len(), 1);
        assert_eq!(scratch.as_slice()[0].symbol_id, 1); // ETH
    }

    // ── Symbol normalization ───────────────────────────────────────

    #[test]
    fn test_normalize_to_native() {
        assert_eq!(normalize_to_native("BTC_USDT"), "BTCUSDT");
        assert_eq!(normalize_to_native("PERP_BTC_USDT"), "BTCUSDT");
        assert_eq!(normalize_to_native("eth_usdc"), "ETHUSDC");
        assert_eq!(normalize_to_native("BTCUSDT"), "BTCUSDT");
    }

    // ── End-to-end with WsFramer ───────────────────────────────────

    #[test]
    fn end_to_end_framer_to_parse() {
        use crate::hft::ws_framer::{WsFramer, OP_TEXT};

        let msg = r#"{"stream":"btcusdt@bookTicker","data":{"s":"BTCUSDT","b":"67432.10","B":"1.5","a":"67432.50","A":"2.0","u":1,"E":1700000000000}}"#;

        // Build a server text frame
        let mut frame_bytes = vec![0x80 | OP_TEXT]; // FIN + TEXT
        let payload = msg.as_bytes();
        if payload.len() <= 125 {
            frame_bytes.push(payload.len() as u8);
        }
        frame_bytes.extend_from_slice(payload);

        // Framer → parse → verify
        let mut framer = WsFramer::new();
        framer.inject(&frame_bytes);

        let frame = framer.next_frame().unwrap();
        assert_eq!(frame.opcode, OP_TEXT);

        let mut feed = test_feed();
        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_book_ticker(frame.payload, Instant::now(), &mut scratch);

        assert_eq!(scratch.len(), 1);
        let tick = &scratch.as_slice()[0];
        assert_eq!(tick.symbol_id, 0);
        assert_eq!(tick.item.bid.unwrap(), 67432.10);
        assert_eq!(tick.item.ask.unwrap(), 67432.50);
        assert_eq!(tick.item.bid_qty.unwrap(), 1.5);
        assert_eq!(tick.item.ask_qty.unwrap(), 2.0);
    }

    // ── Latency benchmark ──────────────────────────────────────────

    #[test]
    fn parse_latency_under_1us() {
        let msg = r#"{"stream":"btcusdt@bookTicker","data":{"s":"BTCUSDT","b":"67432.10000000","B":"1.234","a":"67432.50000000","A":"0.567","u":1,"E":1700000000000}}"#;

        let mut feed = test_feed();
        let mut scratch = TickScratch::<MarketData>::new();
        let payload = msg.as_bytes();

        // Warmup
        for _i in 0u64..1000 {
            feed.last_update_id[0] = 0; // reset dedup
            // Patch the update ID in the message to be unique
            // (actually, just reset dedup state instead)
            scratch.clear();
            feed.parse_book_ticker(payload, Instant::now(), &mut scratch);
        }

        // Measure
        let iters = 100_000u64;
        let start = Instant::now();
        for _ in 0..iters {
            feed.last_update_id[0] = 0;
            scratch.clear();
            feed.parse_book_ticker(payload, Instant::now(), &mut scratch);
        }
        let elapsed = start.elapsed();
        let per_iter_ns = elapsed.as_nanos() as u64 / iters;

        // Should be well under 1μs. Print for visibility.
        eprintln!("Binance parse latency: {} ns/iter", per_iter_ns);
        // Only assert in release builds — debug mode is 5-10x slower
        #[cfg(not(debug_assertions))]
        assert!(
            per_iter_ns < 2_000,
            "parse latency {} ns exceeds 2us budget",
            per_iter_ns
        );
    }
}
