use crate::exchange_fees::{ExchangeFees, FeeSchedule};
use crate::exchanges::connection::{
    ConnectionConfig, ExchangeFeed, WireMessage, listen_with_reconnect,
};
use crate::market_data::{InstrumentType, MarketData, MarketDataCollection};
use crate::orderbook::OrderBook;
use crate::trade_data::{TradeData, TradeDataCollection, TradeSide};
use anyhow::Result;
use chrono::{DateTime, Utc};
use log::debug;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

pub fn get_fees() -> ExchangeFees {
    ExchangeFees::new(FeeSchedule::new(2.0, 5.0), FeeSchedule::new(2.0, 5.0))
}

// --- Orderbook delta message ---

#[derive(Debug, Deserialize)]
struct ZeroOneDeltaMsg {
    delta: ZeroOneDelta,
}

#[derive(Debug, Deserialize)]
struct ZeroOneDelta {
    market_symbol: String,
    #[allow(dead_code)]
    update_id: u64,
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
}

// --- Trade message ---

#[derive(Debug, Deserialize)]
struct ZeroOneTradeMsg {
    trades: ZeroOneTradeWrapper,
}

#[derive(Debug, Deserialize)]
struct ZeroOneTradeWrapper {
    market_symbol: String,
    #[allow(dead_code)]
    update_id: u64,
    trades: Vec<ZeroOneTradeEntry>,
}

#[derive(Debug, Deserialize)]
struct ZeroOneTradeEntry {
    side: String,
    price: f64,
    #[serde(alias = "base_size", alias = "size")]
    qty: f64,
    #[serde(default)]
    time: Option<String>,
}

// --- Helper: config symbol -> native ---

fn to_native(sym: &str) -> String {
    let parts: Vec<&str> = sym.split('_').collect();
    let base = if parts.len() == 3 { parts[1] } else if parts.len() == 2 { parts[0] } else { sym };
    format!("{}USD", base.to_uppercase())
}

fn build_native_to_registry(symbols: &[&str]) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for sym in symbols {
        let native = to_native(sym);
        let parts: Vec<&str> = sym.split('_').collect();
        let base = if parts.len() == 3 { parts[1] } else if parts.len() == 2 { parts[0] } else { continue };
        let registry = format!("{}USD", base.to_uppercase());
        map.insert(native, registry);
    }
    map
}

// --- BBO Feed (from deltas) ---

struct ZeroOneBboFeed {
    itype: InstrumentType,
    native_to_registry: HashMap<String, String>,
    books: std::sync::Mutex<HashMap<String, OrderBook>>,
}

impl ZeroOneBboFeed {
    fn new(symbols: &[&str], itype: InstrumentType) -> Self {
        Self {
            itype,
            native_to_registry: build_native_to_registry(symbols),
            books: std::sync::Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl ExchangeFeed for ZeroOneBboFeed {
    type Item = MarketData;

    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }

    fn build_url(&self, symbols: &[&str]) -> Result<String> {
        let streams: Vec<String> = symbols
            .iter()
            .map(|s| format!("deltas@{}", to_native(s)))
            .collect();
        Ok(format!("wss://zo-mainnet.n1.xyz/ws/{}", streams.join("&")))
    }

    fn timestamp_dedup(&self) -> bool {
        false
    }

    fn parse_message(
        &self,
        msg: WireMessage<'_>,
        received_ts: DateTime<Utc>,
        received_instant: std::time::Instant,
    ) -> Result<Vec<(String, MarketData)>> {
        match msg {
            WireMessage::Text(text) => {
                if !text.contains("\"delta\"") {
                    debug!("ZeroOne non-delta message");
                    return Ok(vec![]);
                }

                let msg: ZeroOneDeltaMsg = serde_json::from_str(text)?;
                let delta = msg.delta;

                let registry_sym = match self.native_to_registry.get(&delta.market_symbol) {
                    Some(s) => s.clone(),
                    None => {
                        debug!("Unknown symbol from ZeroOne: {}", delta.market_symbol);
                        return Ok(vec![]);
                    }
                };

                let mut book_map = self.books.lock().unwrap();
                let book = book_map
                    .entry(delta.market_symbol)
                    .or_insert_with(OrderBook::new);

                book.update_bids_f64(&delta.bids);
                book.update_asks_f64(&delta.asks);

                let bid = book.best_bid();
                let ask = book.best_ask();

                // No exchange timestamp in delta messages
                let market_data = MarketData {
                    bid: bid.map(|(p, _)| p),
                    ask: ask.map(|(p, _)| p),
                    bid_qty: bid.map(|(_, q)| q),
                    ask_qty: ask.map(|(_, q)| q),
                    exchange_ts_raw: None,
                    exchange_ts: None,
                    received_ts: Some(received_ts),
                    received_instant: Some(received_instant),
                    feed_latency_ns: 0,
                };

                Ok(vec![(registry_sym, market_data)])
            }
            WireMessage::Binary(_) => Ok(vec![]),
        }
    }
}

/// Max symbols per WS connection. ZeroOne silently drops connections when too
/// many (or any unsupported) streams are requested in a single URL.
const MAX_STREAMS_PER_WS: usize = 5;

pub async fn listen_perp_bbo(
    data: Arc<MarketDataCollection>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    if symbols.len() <= MAX_STREAMS_PER_WS {
        let feed = Arc::new(ZeroOneBboFeed::new(symbols, InstrumentType::Perp));
        return listen_with_reconnect(
            data, symbols, feed, "zeroone_perp",
            ConnectionConfig::default(), shutdown,
        ).await;
    }

    let mut handles = Vec::new();
    for (i, chunk) in symbols.chunks(MAX_STREAMS_PER_WS).enumerate() {
        let owned: Vec<String> = chunk.iter().map(|s| s.to_string()).collect();
        let feed = Arc::new(ZeroOneBboFeed::new(chunk, InstrumentType::Perp));
        let data = Arc::clone(&data);
        let shutdown = Arc::clone(&shutdown);
        let feed_name = format!("zeroone_perp_{}", i);
        handles.push(tokio::spawn(async move {
            let refs: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();
            if let Err(e) = listen_with_reconnect(
                data, &refs, feed, &feed_name,
                ConnectionConfig::default(), shutdown,
            ).await {
                log::error!("{} error: {:?}", feed_name, e);
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    Ok(())
}

// --- Trade Feed ---

struct ZeroOneTradeFeed {
    itype: InstrumentType,
    native_to_registry: HashMap<String, String>,
}

impl ZeroOneTradeFeed {
    fn new(symbols: &[&str], itype: InstrumentType) -> Self {
        Self {
            itype,
            native_to_registry: build_native_to_registry(symbols),
        }
    }
}

#[async_trait::async_trait]
impl ExchangeFeed for ZeroOneTradeFeed {
    type Item = TradeData;

    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }

    fn build_url(&self, symbols: &[&str]) -> Result<String> {
        let streams: Vec<String> = symbols
            .iter()
            .map(|s| format!("trades@{}", to_native(s)))
            .collect();
        Ok(format!("wss://zo-mainnet.n1.xyz/ws/{}", streams.join("&")))
    }

    fn timestamp_dedup(&self) -> bool {
        false
    }

    fn parse_message(
        &self,
        msg: WireMessage<'_>,
        received_ts: DateTime<Utc>,
        received_instant: std::time::Instant,
    ) -> Result<Vec<(String, TradeData)>> {
        match msg {
            WireMessage::Text(text) => {
                if !text.contains("\"trades\"") {
                    debug!("ZeroOne non-trade message");
                    return Ok(vec![]);
                }

                let msg: ZeroOneTradeMsg = serde_json::from_str(text)?;
                let wrapper = msg.trades;

                let registry_sym = match self.native_to_registry.get(&wrapper.market_symbol) {
                    Some(s) => s.clone(),
                    None => {
                        debug!("Unknown symbol from ZeroOne: {}", wrapper.market_symbol);
                        return Ok(vec![]);
                    }
                };

                let mut results = Vec::with_capacity(wrapper.trades.len());
                for entry in &wrapper.trades {
                    let side = match entry.side.as_str() {
                        "bid" => TradeSide::Buy,
                        "ask" => TradeSide::Sell,
                        _ => TradeSide::Unknown,
                    };

                    let exchange_ts = entry
                        .time
                        .as_deref()
                        .and_then(|t| t.parse::<DateTime<Utc>>().ok());

                    results.push((
                        registry_sym.clone(),
                        TradeData {
                            price: entry.price,
                            qty: entry.qty,
                            side,
                            exchange_ts_raw: exchange_ts,
                            exchange_ts: None,
                            received_ts: Some(received_ts),
                            received_instant: Some(received_instant),
                            feed_latency_ns: 0,
                        },
                    ));
                }

                Ok(results)
            }
            WireMessage::Binary(_) => Ok(vec![]),
        }
    }
}

pub async fn listen_perp_trades(
    data: Arc<TradeDataCollection>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    if symbols.len() <= MAX_STREAMS_PER_WS {
        let feed = Arc::new(ZeroOneTradeFeed::new(symbols, InstrumentType::Perp));
        return listen_with_reconnect(
            data, symbols, feed, "zeroone_perp_trades",
            ConnectionConfig::default(), shutdown,
        ).await;
    }

    let mut handles = Vec::new();
    for (i, chunk) in symbols.chunks(MAX_STREAMS_PER_WS).enumerate() {
        let owned: Vec<String> = chunk.iter().map(|s| s.to_string()).collect();
        let feed = Arc::new(ZeroOneTradeFeed::new(chunk, InstrumentType::Perp));
        let data = Arc::clone(&data);
        let shutdown = Arc::clone(&shutdown);
        let feed_name = format!("zeroone_perp_trades_{}", i);
        handles.push(tokio::spawn(async move {
            let refs: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();
            if let Err(e) = listen_with_reconnect(
                data, &refs, feed, &feed_name,
                ConnectionConfig::default(), shutdown,
            ).await {
                log::error!("{} error: {:?}", feed_name, e);
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    Ok(())
}
