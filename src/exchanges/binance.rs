use anyhow::Result;
use chrono::{DateTime, Utc};
use log::warn;
use serde::Deserialize;
use std::sync::{Arc, Mutex};

use crate::exchange_fees::{ExchangeFees, FeeSchedule};
use crate::exchanges::connection::{
    ConnectionConfig, ExchangeFeed, WireMessage, listen_with_reconnect,
};
use crate::mappers::{BinanceMapper, SymbolMapper};
use crate::market_data::{InstrumentType, MarketData, MarketDataCollection};

pub fn get_fees() -> ExchangeFees {
    ExchangeFees::new(FeeSchedule::new(10.0, 10.0), FeeSchedule::new(5.0, 2.0))
}

#[derive(Debug, Deserialize)]
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
#[derive(Clone)]
struct BinanceFeed {
    /// "wss://stream.binance.com:9443/stream" for spot
    /// "wss://fstream.binance.com/stream" for perp
    base_url: &'static str,
    itype: InstrumentType,
    mapper: BinanceMapper,
}

impl BinanceFeed {
    fn new_spot() -> Self {
        Self {
            base_url: "wss://stream.binance.com:9443/stream",
            itype: InstrumentType::Spot,
            mapper: BinanceMapper,
        }
    }

    fn new_perp() -> Self {
        Self {
            base_url: "wss://fstream.binance.com/stream",
            itype: InstrumentType::Perp,
            mapper: BinanceMapper,
        }
    }
}

#[async_trait::async_trait]
impl ExchangeFeed for BinanceFeed {
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
    ) -> Result<Option<(String, MarketData)>> {
        // Some exchanges send non-data frames; Binance combined stream sends JSON objects
        // Return Ok(None) on parse failure? Here we propagate error so caller can log.
        match msg {
            WireMessage::Text(text) => {
                let msg = serde_json::from_str::<BinanceBookTicker>(text)?;
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
                        return Ok(None);
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
                    exchange_ts,
                    received_ts: Some(received_ts),
                };

                Ok(Some((msg.data.symbol, market_data)))
            }
            WireMessage::Binary(_) => Ok(None),
        }
    }
}

pub async fn listen_spot_bbo(
    data: Arc<Mutex<MarketDataCollection>>,
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
    data: Arc<Mutex<MarketDataCollection>>,
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
