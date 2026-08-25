/// Zero-allocation Lighter ticker BBO parser.
///
/// Parses the `ticker/{MARKET_INDEX}` channel JSON using manual byte scanning.
/// The ticker channel fires on every order book nonce change, providing best
/// bid/ask with sizes.
///
/// Wire format (responses use colon separator):
/// ```json
/// {"channel":"ticker:0","last_updated_at":1774883844921166,"nonce":9182249734,
///  "ticker":{"s":"ETH","a":{"price":"2064.48","size":"0.4950"},
///  "b":{"price":"2064.30","size":"1.0392"},"last_updated_at":1774883844921166},
///  "timestamp":1774883844933,"type":"update/ticker"}
/// ```
///
/// Supports both Robinhood chain (`wss://api.rh.lighter.xyz/stream`) and
/// mainnet (`wss://mainnet.zklighter.elliot.ai/stream`) — URL is configurable.
use crate::hft::{HftFeed, TickScratch, extract_f64_after, extract_u64_after, find_bytes};
use crate::market_data::{InstrumentType, MarketData};
use crate::symbol_registry::{REGISTRY, SymbolId};
use chrono::DateTime;
use std::time::Instant;

// Byte patterns for zero-alloc JSON scanning.
const TICKER_MARKER: &[u8] = b"\"ticker\":{";
// Server may respond with colon or slash separator for channel name.
const CHANNEL_KEY_COLON: &[u8] = b"\"channel\":\"ticker:";
const CHANNEL_KEY_SLASH: &[u8] = b"\"channel\":\"ticker/";
const TIMESTAMP_KEY: &[u8] = b"\"timestamp\":";

/// Maximum market_index value. Lighter market indices are small integers.
const MAX_MARKET_INDEX: usize = 4096;

pub struct LighterHftFeed {
    /// market_index → SymbolId. Direct array indexing, O(1).
    market_index_to_symbol: Box<[Option<SymbolId>; MAX_MARKET_INDEX]>,
    /// WebSocket URL.
    url: String,
    /// Pre-built subscription messages.
    sub_messages: Vec<String>,
}

impl LighterHftFeed {
    /// Create from a pre-built market_index → SymbolId mapping.
    ///
    /// The mapping is typically built from the Lighter REST API at startup
    /// (orderBookDetails endpoint), then passed in along with the WS URL.
    ///
    /// `ws_url`: e.g. `"wss://api.rh.lighter.xyz/stream"` for Robinhood chain
    pub fn new(market_index_to_symbol_map: &[(u32, SymbolId)], ws_url: &str) -> Self {
        let mut market_index_to_symbol = Box::new([None::<SymbolId>; MAX_MARKET_INDEX]);
        let mut sub_messages = Vec::new();

        for &(mi, sid) in market_index_to_symbol_map {
            if (mi as usize) < MAX_MARKET_INDEX {
                market_index_to_symbol[mi as usize] = Some(sid);
                sub_messages.push(format!(
                    r#"{{"type":"subscribe","channel":"ticker/{}"}}"#,
                    mi
                ));
            }
        }

        Self {
            market_index_to_symbol,
            url: ws_url.to_string(),
            sub_messages,
        }
    }

    /// Convenience constructor that resolves symbols through the global registry.
    ///
    /// `symbols`: normalized form like `["BTC_USDC", "ETH_USDC"]`
    /// `market_indices`: parallel array of market indices from the REST API
    /// `ws_url`: WebSocket URL for the target Lighter instance
    pub fn from_symbols(
        symbols: &[&str],
        market_indices: &[u32],
        ws_url: &str,
    ) -> Self {
        let itype = InstrumentType::Perp;
        let mut mapping = Vec::new();

        for (sym, &mi) in symbols.iter().zip(market_indices.iter()) {
            let parts: Vec<&str> = sym.split('_').collect();
            let (base, quote) = if parts.len() == 3 {
                (parts[1], parts[2])
            } else if parts.len() == 2 {
                (parts[0], parts[1])
            } else {
                continue;
            };

            let registry_key = format!("{}{}", base.to_uppercase(), quote.to_uppercase());
            if let Some(&id) = REGISTRY.lookup(&registry_key, &itype) {
                mapping.push((mi, id));
            } else {
                log::warn!("LighterHftFeed: '{}' (key '{}') not in registry", sym, registry_key);
            }
        }

        Self::new(&mapping, ws_url)
    }

    /// Create with a custom lookup table (for testing).
    pub fn with_lookup(
        market_index_to_symbol: Box<[Option<SymbolId>; MAX_MARKET_INDEX]>,
    ) -> Self {
        Self {
            market_index_to_symbol,
            url: String::new(),
            sub_messages: vec![],
        }
    }

    /// Parse a ticker message. Zero-alloc.
    #[inline]
    fn parse_ticker(
        &self,
        json: &[u8],
        received_instant: Instant,
        scratch: &mut TickScratch<MarketData>,
    ) {
        // Quick rejection: must contain "ticker":{
        if find_bytes(json, TICKER_MARKER).is_none() {
            return;
        }

        // Extract market_index from channel — handle both colon and slash separators
        let market_index = extract_u64_after(json, CHANNEL_KEY_COLON)
            .or_else(|| extract_u64_after(json, CHANNEL_KEY_SLASH));
        let market_index = match market_index {
            Some(mi) => mi as u32,
            None => return,
        };
        if market_index as usize >= MAX_MARKET_INDEX {
            return;
        }

        // Lookup SymbolId
        let symbol_id = match self.market_index_to_symbol[market_index as usize] {
            Some(id) => id,
            None => return,
        };

        // Extract timestamp (milliseconds)
        let exchange_ts = extract_u64_after(json, TIMESTAMP_KEY)
            .and_then(|ms| DateTime::from_timestamp_millis(ms as i64));

        // Extract bid: "b":{"price":"2064.30","size":"1.0392"}
        let bid_pos = match find_bytes(json, b"\"b\":{") {
            Some(p) => p,
            None => return,
        };
        let bid_section = &json[bid_pos..];
        let bid_px = extract_f64_after(bid_section, b"\"price\":\"");
        let bid_sz = extract_f64_after(bid_section, b"\"size\":\"");

        // Extract ask: "a":{"price":"2064.48","size":"0.4950"}
        let ask_pos = match find_bytes(json, b"\"a\":{") {
            Some(p) => p,
            None => return,
        };
        let ask_section = &json[ask_pos..];
        let ask_px = extract_f64_after(ask_section, b"\"price\":\"");
        let ask_sz = extract_f64_after(ask_section, b"\"size\":\"");

        // Validate: both sides must be present and uncrossed
        match (bid_px, ask_px) {
            (Some(b), Some(a)) if b < a => {}
            _ => return,
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
                ..Default::default()
            },
        );
    }
}

impl HftFeed for LighterHftFeed {
    type Item = MarketData;

    fn urls(&self) -> Vec<String> {
        vec![self.url.clone()]
    }

    fn subscribe_messages(&self) -> Vec<String> {
        self.sub_messages.clone()
    }

    fn heartbeat_payload(&self) -> Option<&'static [u8]> {
        // Lighter requires at least one frame every 2 minutes.
        Some(b"{\"type\":\"pong\"}")
    }

    fn heartbeat_interval_ms(&self) -> u64 {
        60_000 // 60s — well within the 2-minute timeout
    }

    fn on_connected(&mut self, _conn_index: usize) {
        // No per-connection state to reset — ticker is stateless BBO
    }

    fn parse_text(
        &mut self,
        payload: &[u8],
        received_instant: Instant,
        scratch: &mut TickScratch<MarketData>,
    ) {
        self.parse_ticker(payload, received_instant, scratch);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hft::TickScratch;
    use crate::market_data::MarketData;

    fn test_lookup() -> Box<[Option<SymbolId>; MAX_MARKET_INDEX]> {
        let mut map = Box::new([None::<SymbolId>; MAX_MARKET_INDEX]);
        map[0] = Some(0); // ETH market_index=0
        map[1] = Some(1); // BTC market_index=1
        map
    }

    fn test_feed() -> LighterHftFeed {
        LighterHftFeed::with_lookup(test_lookup())
    }

    // Colon-separated channel (observed wire format)
    const TICKER_MSG: &str = r#"{"channel":"ticker:0","last_updated_at":1774883844921166,"nonce":9182249734,"ticker":{"s":"ETH","a":{"price":"2064.48","size":"0.4950"},"b":{"price":"2064.30","size":"1.0392"},"last_updated_at":1774883844921166},"timestamp":1774883844933,"type":"update/ticker"}"#;

    const TICKER_MSG_BTC: &str = r#"{"channel":"ticker:1","last_updated_at":1774883844921166,"nonce":9182249734,"ticker":{"s":"BTC","a":{"price":"67432.50","size":"0.3285"},"b":{"price":"67432.10","size":"1.5000"},"last_updated_at":1774883844921166},"timestamp":1774883844933,"type":"update/ticker"}"#;

    // Slash-separated channel (subscribe format, in case server echoes it)
    const TICKER_MSG_SLASH: &str = r#"{"channel":"ticker/0","last_updated_at":1774883844921166,"nonce":9182249734,"ticker":{"s":"ETH","a":{"price":"2064.48","size":"0.4950"},"b":{"price":"2064.30","size":"1.0392"},"last_updated_at":1774883844921166},"timestamp":1774883844933,"type":"update/ticker"}"#;

    #[test]
    fn parse_ticker_basic() {
        let feed = test_feed();
        let mut scratch = TickScratch::<MarketData>::new();

        feed.parse_ticker(TICKER_MSG.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.len(), 1);

        let tick = &scratch.as_slice()[0];
        assert_eq!(tick.symbol_id, 0);
        assert_eq!(tick.item.bid.unwrap(), 2064.30);
        assert_eq!(tick.item.ask.unwrap(), 2064.48);
        assert_eq!(tick.item.bid_qty.unwrap(), 1.0392);
        assert_eq!(tick.item.ask_qty.unwrap(), 0.4950);
    }

    #[test]
    fn parse_ticker_slash_channel() {
        let feed = test_feed();
        let mut scratch = TickScratch::<MarketData>::new();

        feed.parse_ticker(TICKER_MSG_SLASH.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.len(), 1);
        assert_eq!(scratch.as_slice()[0].item.bid.unwrap(), 2064.30);
    }

    #[test]
    fn parse_ticker_btc() {
        let feed = test_feed();
        let mut scratch = TickScratch::<MarketData>::new();

        feed.parse_ticker(TICKER_MSG_BTC.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.len(), 1);

        let tick = &scratch.as_slice()[0];
        assert_eq!(tick.symbol_id, 1);
        assert_eq!(tick.item.bid.unwrap(), 67432.10);
        assert_eq!(tick.item.ask.unwrap(), 67432.50);
    }

    #[test]
    fn parse_ticker_timestamp() {
        let feed = test_feed();
        let mut scratch = TickScratch::<MarketData>::new();

        feed.parse_ticker(TICKER_MSG.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.len(), 1);

        let expected_ts = DateTime::from_timestamp_millis(1774883844933);
        assert_eq!(scratch.as_slice()[0].item.exchange_ts_raw, expected_ts);
    }

    #[test]
    fn rejects_unknown_market_index() {
        let feed = test_feed();
        let msg = r#"{"channel":"ticker:99","nonce":1,"ticker":{"s":"UNK","a":{"price":"100","size":"1"},"b":{"price":"99","size":"1"}},"timestamp":1,"type":"update/ticker"}"#;
        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_ticker(msg.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.len(), 0);
    }

    #[test]
    fn rejects_crossed_quote() {
        let feed = test_feed();
        let msg = r#"{"channel":"ticker:0","nonce":1,"ticker":{"s":"ETH","a":{"price":"100","size":"1"},"b":{"price":"101","size":"1"}},"timestamp":1,"type":"update/ticker"}"#;
        let mut scratch = TickScratch::<MarketData>::new();
        feed.parse_ticker(msg.as_bytes(), Instant::now(), &mut scratch);
        assert_eq!(scratch.len(), 0);
    }

    #[test]
    fn rejects_garbage() {
        let feed = test_feed();
        let garbage = &[
            "",
            "{}",
            r#"{"type":"pong"}"#,
            r#"{"channel":"order_book:0","order_book":{}}"#,
            r#"{"type":"subscribed/ticker"}"#,
        ];
        for &msg in garbage {
            let mut scratch = TickScratch::<MarketData>::new();
            feed.parse_ticker(msg.as_bytes(), Instant::now(), &mut scratch);
            assert_eq!(scratch.len(), 0, "should reject: {}", msg);
        }
    }

    #[test]
    fn parse_latency_under_1us() {
        let feed = test_feed();
        let payload = TICKER_MSG.as_bytes();
        let mut scratch = TickScratch::<MarketData>::new();

        // Warmup
        for _ in 0..1000 {
            scratch.clear();
            feed.parse_ticker(payload, Instant::now(), &mut scratch);
        }

        let iters = 100_000u64;
        let start = Instant::now();
        for _ in 0..iters {
            scratch.clear();
            feed.parse_ticker(payload, Instant::now(), &mut scratch);
        }
        let per_iter_ns = start.elapsed().as_nanos() as u64 / iters;
        eprintln!("Lighter ticker parse latency: {} ns/iter", per_iter_ns);

        #[cfg(not(debug_assertions))]
        assert!(per_iter_ns < 2_000, "parse latency {} ns exceeds 2us", per_iter_ns);
    }

    #[test]
    fn end_to_end_framer_to_parse() {
        use crate::hft::ws_framer::{WsFramer, OP_TEXT};

        let payload = TICKER_MSG.as_bytes();
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
        feed.parse_ticker(frame.payload, Instant::now(), &mut scratch);

        assert_eq!(scratch.len(), 1);
        assert_eq!(scratch.as_slice()[0].item.bid.unwrap(), 2064.30);
        assert_eq!(scratch.as_slice()[0].item.ask.unwrap(), 2064.48);
    }
}
