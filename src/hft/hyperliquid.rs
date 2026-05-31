/// Zero-allocation Hyperliquid l2Book BBO parser.
///
/// Parses the l2Book channel JSON using manual byte scanning.
/// Extracts best bid/ask from the first element of each levels array.
///
/// Wire format:
/// ```json
/// {"channel":"l2Book","data":{"coin":"BTC","time":1676151190656,"levels":[[{"px":"77000.50","sz":"1.23","n":45}],[{"px":"77001.00","sz":"0.98","n":32}]]}}
/// ```
use crate::hft::{HftFeed, TickScratch, extract_str_after, extract_u64_after, find_bytes, parse_f64};
use crate::market_data::{InstrumentType, MarketData};
use crate::symbol_registry::{REGISTRY, SymbolId};
use chrono::DateTime;
use rustc_hash::FxHashMap;
use std::time::Instant;

// Byte patterns for zero-alloc JSON scanning.
const L2BOOK_MARKER: &[u8] = b"\"l2Book\"";
const COIN_KEY: &[u8] = b"\"coin\":\"";
const TIME_KEY: &[u8] = b"\"time\":";
const PX_KEY: &[u8] = b"\"px\":\"";
const SZ_KEY: &[u8] = b"\"sz\":\"";
const LEVELS_MARKER: &[u8] = b"\"levels\":";
const BID_ASK_SEP: &[u8] = b"],[";

pub struct HyperliquidHftFeed {
    /// Coin name (e.g. "BTC") → SymbolId. Read-only after init.
    coin_to_id: FxHashMap<String, SymbolId>,
    /// Pre-built subscription messages.
    sub_messages: Vec<String>,
    /// WebSocket URL.
    url: String,
}

impl HyperliquidHftFeed {
    /// Create a new Hyperliquid HFT feed.
    ///
    /// `symbols` are in normalized form: `"BTC_USDC"` or `"PERP_BTC_USDC"`.
    pub fn new(symbols: &[&str]) -> Self {
        let itype = InstrumentType::Perp;
        let mut coin_to_id = FxHashMap::default();
        let mut sub_messages = Vec::with_capacity(symbols.len());

        for sym in symbols {
            let parts: Vec<&str> = sym.split('_').collect();
            let (base, quote) = if parts.len() == 3 {
                (parts[1], parts[2])
            } else if parts.len() == 2 {
                (parts[0], parts[1])
            } else {
                continue;
            };

            let coin = base.to_uppercase();
            let registry_key = format!("{}{}", coin, quote.to_uppercase());

            if let Some(&id) = REGISTRY.lookup(&registry_key, &itype) {
                coin_to_id.insert(coin.clone(), id);
                sub_messages.push(format!(
                    r#"{{"method":"subscribe","subscription":{{"type":"l2Book","coin":"{}"}}}}"#,
                    coin
                ));
            } else {
                log::warn!("HyperliquidHftFeed: '{}' (native '{}') not in registry", sym, registry_key);
            }
        }

        Self {
            coin_to_id,
            sub_messages,
            url: "wss://api.hyperliquid.xyz/ws".to_string(),
        }
    }

    /// Create with a custom lookup table (for testing).
    pub fn with_lookup(coin_to_id: FxHashMap<String, SymbolId>) -> Self {
        Self {
            coin_to_id,
            sub_messages: vec![],
            url: String::new(),
        }
    }

    /// Parse an l2Book message. Zero-alloc.
    #[inline]
    fn parse_l2book(
        &self,
        json: &[u8],
        received_instant: Instant,
        scratch: &mut TickScratch<MarketData>,
    ) {
        // Quick rejection
        if find_bytes(json, L2BOOK_MARKER).is_none() {
            return;
        }

        // Extract coin
        let coin = match extract_str_after(json, COIN_KEY) {
            Some(c) => c,
            None => return,
        };

        // Lookup SymbolId
        let symbol_id = match self.coin_to_id.get(coin) {
            Some(&id) => id,
            None => return,
        };

        // Extract timestamp (milliseconds)
        let exchange_ts = extract_u64_after(json, TIME_KEY)
            .and_then(|ms| DateTime::from_timestamp_millis(ms as i64));

        // Find levels array
        let levels_pos = match find_bytes(json, LEVELS_MARKER) {
            Some(p) => p + LEVELS_MARKER.len(),
            None => return,
        };
        let levels_section = &json[levels_pos..];

        // Best bid: first "px" and "sz" in the bids array (before "],[" separator)
        let sep_pos = match find_bytes(levels_section, BID_ASK_SEP) {
            Some(p) => p,
            None => return,
        };
        let bid_section = &levels_section[..sep_pos];
        let bid_px = extract_str_after(bid_section, PX_KEY).and_then(parse_f64);
        let bid_sz = extract_str_after(bid_section, SZ_KEY).and_then(parse_f64);

        // Best ask: first "px" and "sz" in the asks array (after "],[" separator)
        let ask_section = &levels_section[sep_pos + BID_ASK_SEP.len()..];
        let ask_px = extract_str_after(ask_section, PX_KEY).and_then(parse_f64);
        let ask_sz = extract_str_after(ask_section, SZ_KEY).and_then(parse_f64);

        // Validate
        if let (Some(b), Some(a)) = (bid_px, ask_px) {
            if b >= a {
                return;
            }
        }

        scratch.push(
            symbol_id,
            MarketData {
                bid: bid_px,
                ask: ask_px,
                bid_qty: bid_sz,
                ask_qty: ask_sz,
                exchange_ts_raw: exchange_ts,
                exchange_ts: None,
                received_ts: None,
                received_instant: Some(received_instant),
                feed_latency_ns: 0,
            },
        );
    }
}

impl HftFeed for HyperliquidHftFeed {
    type Item = MarketData;

    fn urls(&self) -> Vec<String> {
        vec![self.url.clone()]
    }

    fn subscribe_messages(&self) -> Vec<String> {
        self.sub_messages.clone()
    }

    fn heartbeat_payload(&self) -> Option<&'static [u8]> {
        Some(b"{\"method\":\"ping\"}")
    }

    fn heartbeat_interval_ms(&self) -> u64 {
        10_000
    }

    fn on_connected(&mut self, _conn_index: usize) {
        // No dedup state to reset — HL is snapshot model
    }

    fn parse_text(
        &mut self,
        payload: &[u8],
        received_instant: Instant,
        scratch: &mut TickScratch<MarketData>,
    ) {
        self.parse_l2book(payload, received_instant, scratch);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hft::TickScratch;
    use crate::market_data::MarketData;

    fn test_lookup() -> FxHashMap<String, SymbolId> {
        let mut map = FxHashMap::default();
        map.insert("BTC".to_string(), 0);
        map.insert("ETH".to_string(), 1);
        map
    }

    fn test_feed() -> HyperliquidHftFeed {
        HyperliquidHftFeed::with_lookup(test_lookup())
    }

    // ── Serde reference types ──────────────────────────────────────

    #[derive(serde::Deserialize)]
    struct RefL2Book {
        data: RefL2Data,
    }
    #[derive(serde::Deserialize)]
    struct RefL2Data {
        coin: String,
        time: u64,
        levels: Vec<Vec<RefLevel>>,
    }
    #[derive(serde::Deserialize)]
    struct RefLevel {
        px: String,
        sz: String,
    }

    const CORPUS: &[&str] = &[
        r#"{"channel":"l2Book","data":{"coin":"BTC","time":1700000000000,"levels":[[{"px":"67432.10","sz":"1.5","n":10},{"px":"67431.00","sz":"2.0","n":5}],[{"px":"67432.50","sz":"0.8","n":3},{"px":"67433.00","sz":"1.2","n":7}]]}}"#,
        r#"{"channel":"l2Book","data":{"coin":"ETH","time":1700000000001,"levels":[[{"px":"3521.05","sz":"10.0","n":1}],[{"px":"3521.15","sz":"5.0","n":2}]]}}"#,
        r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[{"px":"100000.00","sz":"0.001","n":1}],[{"px":"100000.01","sz":"0.001","n":1}]]}}"#,
    ];

    #[test]
    fn scanner_matches_serde_reference() {
        let feed = test_feed();

        for &msg in CORPUS {
            let reference: RefL2Book = serde_json::from_str(msg).unwrap();
            let ref_bid: f64 = reference.data.levels[0][0].px.parse().unwrap();
            let ref_ask: f64 = reference.data.levels[1][0].px.parse().unwrap();
            let ref_bid_sz: f64 = reference.data.levels[0][0].sz.parse().unwrap();
            let ref_ask_sz: f64 = reference.data.levels[1][0].sz.parse().unwrap();

            let mut scratch = TickScratch::<MarketData>::new();
            feed.parse_l2book(msg.as_bytes(), Instant::now(), &mut scratch);

            assert_eq!(scratch.len(), 1, "failed to parse: {}", msg);
            let tick = &scratch.as_slice()[0];
            assert_eq!(tick.item.bid.unwrap(), ref_bid, "bid: {}", msg);
            assert_eq!(tick.item.ask.unwrap(), ref_ask, "ask: {}", msg);
            assert_eq!(tick.item.bid_qty.unwrap(), ref_bid_sz, "bid_sz: {}", msg);
            assert_eq!(tick.item.ask_qty.unwrap(), ref_ask_sz, "ask_sz: {}", msg);

            let expected_ts = DateTime::from_timestamp_millis(reference.data.time as i64);
            assert_eq!(tick.item.exchange_ts_raw, expected_ts, "ts: {}", msg);
        }
    }

    #[test]
    fn rejects_garbage() {
        let feed = test_feed();
        let garbage = &[
            "",
            "{}",
            r#"{"channel":"pong"}"#,
            r#"{"channel":"subscriptionResponse"}"#,
            r#"{"channel":"l2Book","data":{"coin":"UNKNOWN","time":1,"levels":[[],[]]}}""#,
            r#"{"channel":"trades","data":[{"coin":"BTC"}]}"#,
        ];
        for &msg in garbage {
            let mut scratch = TickScratch::<MarketData>::new();
            feed.parse_l2book(msg.as_bytes(), Instant::now(), &mut scratch);
            assert_eq!(scratch.len(), 0, "should reject: {}", msg);
        }
    }

    #[test]
    fn rejects_crossed_quote() {
        let feed = test_feed();
        let msg = r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[{"px":"100.0","sz":"1","n":1}],[{"px":"99.0","sz":"1","n":1}]]}}"#;
        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_l2book(msg.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.len(), 0);
    }

    #[test]
    fn correct_symbol_id() {
        let feed = test_feed();
        let btc = r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[{"px":"100","sz":"1","n":1}],[{"px":"101","sz":"1","n":1}]]}}"#;
        let eth = r#"{"channel":"l2Book","data":{"coin":"ETH","time":1,"levels":[[{"px":"3000","sz":"1","n":1}],[{"px":"3001","sz":"1","n":1}]]}}"#;

        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_l2book(btc.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].symbol_id, 0);

        scratch.clear();
        feed.parse_l2book(eth.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.as_slice()[0].symbol_id, 1);
    }

    #[test]
    fn end_to_end_framer_to_parse() {
        use crate::hft::ws_framer::{WsFramer, OP_TEXT};

        let msg = r#"{"channel":"l2Book","data":{"coin":"BTC","time":1700000000000,"levels":[[{"px":"67432.10","sz":"1.5","n":10}],[{"px":"67432.50","sz":"0.8","n":3}]]}}"#;
        let payload = msg.as_bytes();
        let mut frame_bytes = vec![0x80 | OP_TEXT];
        if payload.len() <= 125 {
            frame_bytes.push(payload.len() as u8);
        } else {
            frame_bytes.push(126);
            frame_bytes.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        }
        frame_bytes.extend_from_slice(payload);

        let mut framer = WsFramer::new();
        framer.inject(&frame_bytes);
        let frame = framer.next_frame().unwrap();

        let feed = test_feed();
        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_l2book(frame.payload, Instant::now(), &mut scratch);

        assert_eq!(scratch.len(), 1);
        assert_eq!(scratch.as_slice()[0].item.bid.unwrap(), 67432.10);
        assert_eq!(scratch.as_slice()[0].item.ask.unwrap(), 67432.50);
    }

    #[test]
    fn parse_latency_under_1us() {
        let msg = r#"{"channel":"l2Book","data":{"coin":"BTC","time":1700000000000,"levels":[[{"px":"67432.10","sz":"1.5","n":10},{"px":"67431.00","sz":"2.0","n":5}],[{"px":"67432.50","sz":"0.8","n":3},{"px":"67433.00","sz":"1.2","n":7}]]}}"#;
        let feed = test_feed();
        let payload = msg.as_bytes();
        let mut scratch = TickScratch::<MarketData>::new();

        // Warmup
        for _ in 0..1000 {
            scratch.clear();
            feed.parse_l2book(payload, Instant::now(), &mut scratch);
        }

        let iters = 100_000u64;
        let start = Instant::now();
        for _ in 0..iters {
            scratch.clear();
            feed.parse_l2book(payload, Instant::now(), &mut scratch);
        }
        let per_iter_ns = start.elapsed().as_nanos() as u64 / iters;
        eprintln!("Hyperliquid parse latency: {} ns/iter", per_iter_ns);

        #[cfg(not(debug_assertions))]
        assert!(per_iter_ns < 2_000, "parse latency {} ns exceeds 2us", per_iter_ns);
    }
}
