use anyhow::Result;
use chrono::{DateTime, Utc};
use log::warn;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

use crate::exchange_fees::{ExchangeFees, FeeSchedule};
use crate::exchanges::connection::{
    ConnectionConfig, ExchangeFeed, WireMessage, listen_with_reconnect,
};
use crate::mappers::{BinanceMapper, SymbolMapper};
use crate::market_data::{InstrumentType, MarketData, MarketDataCollection};
use crate::trade_data::{TradeData, TradeDataCollection, TradeSide};

pub fn get_fees() -> ExchangeFees {
    ExchangeFees::new(FeeSchedule::new(10.0, 10.0), FeeSchedule::new(5.0, 2.0))
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct BinanceBookTicker {
    stream: String,
    data: BinanceBookTickerData,
}

#[derive(Debug, Deserialize)]
struct BinanceBookTickerData {
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
    /// Event time (ms) — present on futures bookTicker, absent on spot
    #[serde(rename = "E", default)]
    event_time: Option<u64>,
}

/// Binance feed implemented using the generic connection abstraction.
struct BinanceFeed {
    /// "wss://stream.binance.com:9443/stream" for spot
    /// "wss://fstream.binance.com/stream" for perp
    base_url: &'static str,
    itype: InstrumentType,
    mapper: BinanceMapper,
    /// Dedup by update ID (spot has no event_time, so connection-loop
    /// timestamp dedup is a no-op; we use the `u` field instead).
    last_update_id: std::sync::Mutex<HashMap<String, u64>>,
}

impl BinanceFeed {
    fn new_spot() -> Self {
        Self {
            base_url: "wss://stream.binance.com:9443/stream",
            itype: InstrumentType::Spot,
            mapper: BinanceMapper,
            last_update_id: std::sync::Mutex::new(HashMap::new()),
        }
    }

    fn new_perp() -> Self {
        Self {
            base_url: "wss://fstream.binance.com/stream",
            itype: InstrumentType::Perp,
            mapper: BinanceMapper,
            last_update_id: std::sync::Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl ExchangeFeed for BinanceFeed {
    type Item = MarketData;

    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }
    fn build_url(&self, symbols: &[&str]) -> Result<String> {
        // Now symbols can be either normalized or native
        let streams: Vec<String> = symbols
            .iter()
            .map(|s| {
                // Try to denormalize first, fall back to treating as native
                let native = self
                    .mapper
                    .denormalize(s, self.itype)
                    .unwrap()
                    .to_lowercase();
                format!("{}@bookTicker", native)
            })
            .collect();

        let streams_str = streams.join("/");
        warn!("Binance url {}", streams_str);
        Ok(format!("{}?streams={}", self.base_url, streams_str))
    }

    fn parse_message(
        &self,
        msg: WireMessage<'_>,
        received_ts: chrono::DateTime<Utc>,
        received_instant: std::time::Instant,
    ) -> Result<Vec<(String, MarketData)>> {
        // Some exchanges send non-data frames; Binance combined stream sends JSON objects
        // Return Ok(None) on parse failure? Here we propagate error so caller can log.
        match msg {
            WireMessage::Text(text) => {
                let msg = serde_json::from_str::<BinanceBookTicker>(text)?;

                // Dedup by update ID (monotonically increasing per symbol)
                {
                    let mut map = self.last_update_id.lock().unwrap();
                    let last = map.entry(msg.data.symbol.clone()).or_insert(0);
                    if msg.data.u <= *last {
                        return Ok(vec![]);
                    }
                    *last = msg.data.u;
                }

                let bid = msg.data.bid_price.parse::<f64>().ok();
                let ask = msg.data.ask_price.parse::<f64>().ok();
                let bid_qty = msg.data.bid_quantity.parse::<f64>().ok();
                let ask_qty = msg.data.ask_quantity.parse::<f64>().ok();

                // Validate quote sanity
                if let (Some(b), Some(a)) = (bid, ask) {
                    if b >= a {
                        warn!(
                            "Invalid quote for {}: bid={} >= ask={}",
                            msg.data.symbol, b, a
                        );
                        return Ok(vec![]);
                    }
                }

                let exchange_ts = msg
                    .data
                    .event_time
                    .and_then(|ms| DateTime::from_timestamp_millis(ms as i64));

                let market_data = MarketData {
                    bid,
                    ask,
                    bid_qty,
                    ask_qty,
                    exchange_ts_raw: exchange_ts,
                    exchange_ts: None,
                    received_ts: Some(received_ts),
                    received_instant: Some(received_instant),
                    feed_latency_ns: 0,
                };

                Ok(vec![(msg.data.symbol, market_data)])
            }
            WireMessage::Binary(_) => Ok(vec![]),
        }
    }
}

pub async fn listen_spot_bbo(
    data: Arc<MarketDataCollection>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let feed = Arc::new(BinanceFeed::new_spot());
    listen_with_reconnect(
        data,
        symbols,
        feed,
        "binance_spot",
        ConnectionConfig::default(),
        shutdown,
    )
    .await
}

pub async fn listen_perp_bbo(
    data: Arc<MarketDataCollection>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let feed = Arc::new(BinanceFeed::new_perp());
    listen_with_reconnect(
        data,
        symbols,
        feed,
        "binance_perp",
        ConnectionConfig::default(),
        shutdown,
    )
    .await
}

// --- Aggregate Trade Feed ---

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct BinanceAggTrade {
    stream: String,
    data: BinanceAggTradeData,
}

#[derive(Debug, Deserialize)]
struct BinanceAggTradeData {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
    /// Trade time (ms)
    #[serde(rename = "T")]
    trade_time: u64,
    /// Is the buyer the market maker?
    #[serde(rename = "m")]
    buyer_is_maker: bool,
}

struct BinanceTradeFeed {
    base_url: &'static str,
    itype: InstrumentType,
    mapper: BinanceMapper,
}

impl BinanceTradeFeed {
    fn new_perp() -> Self {
        Self {
            base_url: "wss://fstream.binance.com/stream",
            itype: InstrumentType::Perp,
            mapper: BinanceMapper,
        }
    }
}

#[async_trait::async_trait]
impl ExchangeFeed for BinanceTradeFeed {
    type Item = TradeData;

    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }

    fn build_url(&self, symbols: &[&str]) -> Result<String> {
        let streams: Vec<String> = symbols
            .iter()
            .map(|s| {
                let native = self
                    .mapper
                    .denormalize(s, self.itype)
                    .unwrap()
                    .to_lowercase();
                format!("{}@aggTrade", native)
            })
            .collect();
        let streams_str = streams.join("/");
        Ok(format!("{}?streams={}", self.base_url, streams_str))
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
                let msg = serde_json::from_str::<BinanceAggTrade>(text)?;
                let price = msg.data.price.parse::<f64>()?;
                let qty = msg.data.quantity.parse::<f64>()?;
                let side = if msg.data.buyer_is_maker {
                    TradeSide::Sell
                } else {
                    TradeSide::Buy
                };
                let exchange_ts = DateTime::from_timestamp_millis(msg.data.trade_time as i64);

                let trade = TradeData {
                    price,
                    qty,
                    side,
                    exchange_ts_raw: exchange_ts,
                    exchange_ts: None,
                    received_ts: Some(received_ts),
                    received_instant: Some(received_instant),
                    feed_latency_ns: 0,
                };
                Ok(vec![(msg.data.symbol, trade)])
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
    let feed = Arc::new(BinanceTradeFeed::new_perp());
    listen_with_reconnect(
        data,
        symbols,
        feed,
        "binance_perp_trades",
        ConnectionConfig::default(),
        shutdown,
    )
    .await
}
