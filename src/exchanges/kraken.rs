use crate::mappers::{KrakenMapper, SymbolMapper};
use crate::market_data::{InstrumentType, MarketData, MarketDataCollection};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, stream::SplitSink};
use log::{debug, warn};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

use crate::exchange_fees::{ExchangeFees, FeeSchedule};
use crate::exchanges::connection::{
    ConnectionConfig, ExchangeFeed, WireMessage, listen_with_reconnect,
};

pub fn get_fees() -> ExchangeFees {
    ExchangeFees::new(FeeSchedule::new(40.0, 25.0), FeeSchedule::new(25.0, 25.0))
}

struct KrakenFeed {
    itype: InstrumentType,
    mapper: KrakenMapper,
}

impl KrakenFeed {
    fn new_spot() -> Self {
        Self {
            itype: InstrumentType::Spot,
            mapper: KrakenMapper,
        }
    }
}

#[async_trait::async_trait]
impl ExchangeFeed for KrakenFeed {
    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }
    fn build_url(&self, _symbols: &[&str]) -> Result<String> {
        Ok("wss://ws.kraken.com".to_string())
    }

    async fn send_subscription(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbols: &[&str],
    ) -> Result<()> {
        let pairs: Vec<String> = symbols
            .iter()
            .map(|s| self.mapper.denormalize(s, self.itype))
            .collect::<Result<Vec<_>, _>>()?;
        let subscribe_msg = json!({
            "event": "subscribe",
            "pair": pairs,
            "subscription": {
                "name": "spread"
            }
        });
        write
            .send(Message::Text(subscribe_msg.to_string().into()))
            .await
            .context("Failed to subscribe to kraken")?;
        Ok(())
    }
    fn parse_message(
        &self,
        msg: WireMessage<'_>,
        received_ts: DateTime<Utc>,
    ) -> Result<Option<(String, MarketData)>> {
        match self.itype {
            InstrumentType::Spot => {
                match msg {
                    WireMessage::Binary(_) => Ok(None),
                    WireMessage::Text(text) => {
                        // Handle subscription confirmation
                        if text.contains("\"event\":\"subscriptionStatus\"") {
                            debug!("Kraken subscription confirmed");
                            return Ok(None);
                        }

                        // Handle heartbeat
                        if text.contains("\"event\":\"heartbeat\"") {
                            return Ok(None);
                        }

                        // Parse spread data: [channelID, [bid, ask, timestamp, bidVolume, askVolume], "spread", "XBT/USD"]
                        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
                            if let Some(array) = value.as_array() {
                                if array.len() >= 4 {
                                    // Check if it's a spread message
                                    if let Some(channel_name) =
                                        array.get(2).and_then(|v| v.as_str())
                                    {
                                        if channel_name == "spread" {
                                            // Get symbol
                                            let symbol =
                                                array.get(3).and_then(|v| v.as_str()).unwrap_or("");

                                            // Get spread data [bid, ask, timestamp, bidVolume, askVolume]
                                            if let Some(spread_array) =
                                                array.get(1).and_then(|v| v.as_array())
                                            {
                                                let bid = spread_array
                                                    .get(0)
                                                    .and_then(|v| v.as_str())
                                                    .and_then(|s| s.parse::<f64>().ok());

                                                let ask = spread_array
                                                    .get(1)
                                                    .and_then(|v| v.as_str())
                                                    .and_then(|s| s.parse::<f64>().ok());

                                                let bid_qty = spread_array
                                                    .get(3)
                                                    .and_then(|v| v.as_str())
                                                    .and_then(|s| s.parse::<f64>().ok());

                                                let ask_qty = spread_array
                                                    .get(4)
                                                    .and_then(|v| v.as_str())
                                                    .and_then(|s| s.parse::<f64>().ok());

                                                let exchange_ts = spread_array
                                                    .get(2)
                                                    .and_then(|v| v.as_str())
                                                    .and_then(|s| s.parse::<f64>().ok())
                                                    .and_then(|secs| {
                                                        let whole = secs as i64;
                                                        let nanos = ((secs - whole as f64) * 1_000_000_000.0) as u32;
                                                        DateTime::from_timestamp(whole, nanos)
                                                    });

                                                let market_data = MarketData {
                                                    bid,
                                                    ask,
                                                    bid_qty,
                                                    ask_qty,
                                                    exchange_ts_raw: exchange_ts,
                                                    exchange_ts: None,
                                                    received_ts: Some(received_ts),
                                                };
                                                return Ok(Some((symbol.to_string(), market_data)));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        return Ok(None);
                    }
                }
            }
            _ => Ok(None),
        }
    }
}

pub async fn listen_spot_bbo(
    data: Arc<MarketDataCollection>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let feed = Arc::new(KrakenFeed::new_spot());
    listen_with_reconnect(
        data,
        symbols,
        feed,
        "kraken_spot",
        ConnectionConfig::default(),
        shutdown,
    )
    .await
}

// --- Kraken Futures WebSocket (perps) ---

#[derive(Debug, Deserialize)]
struct KrakenFuturesTicker {
    feed: String,
    product_id: String,
    bid: f64,
    ask: f64,
    bid_size: f64,
    ask_size: f64,
    time: u64,
}

#[derive(Clone)]
struct KrakenFuturesFeed {
    itype: InstrumentType,
    mapper: KrakenMapper,
}

impl KrakenFuturesFeed {
    fn new_perp() -> Self {
        Self {
            itype: InstrumentType::Perp,
            mapper: KrakenMapper,
        }
    }
}

#[async_trait::async_trait]
impl ExchangeFeed for KrakenFuturesFeed {
    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }

    fn build_url(&self, _symbols: &[&str]) -> Result<String> {
        Ok("wss://futures.kraken.com/ws/v1".to_string())
    }

    async fn send_subscription(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbols: &[&str],
    ) -> Result<()> {
        let product_ids: Vec<String> = symbols
            .iter()
            .map(|s| self.mapper.denormalize(s, self.itype))
            .collect::<Result<Vec<_>, _>>()?;

        let subscribe_msg = json!({
            "event": "subscribe",
            "feed": "ticker",
            "product_ids": product_ids
        });

        write
            .send(Message::Text(subscribe_msg.to_string().into()))
            .await
            .context("Failed to subscribe to kraken futures")?;

        Ok(())
    }

    fn parse_message(
        &self,
        msg: WireMessage<'_>,
        received_ts: DateTime<Utc>,
    ) -> Result<Option<(String, MarketData)>> {
        match msg {
            WireMessage::Text(text) => {
                // Ignore subscription confirmations and info messages
                if text.contains("\"event\"") {
                    return Ok(None);
                }

                let ticker: KrakenFuturesTicker = match serde_json::from_str(text) {
                    Ok(t) => t,
                    Err(_) => return Ok(None),
                };

                if ticker.feed != "ticker" && ticker.feed != "ticker_snapshot" {
                    return Ok(None);
                }

                if ticker.bid >= ticker.ask {
                    warn!(
                        "Invalid Kraken futures quote for {}: bid={} >= ask={}",
                        ticker.product_id, ticker.bid, ticker.ask
                    );
                    return Ok(None);
                }

                let exchange_ts =
                    DateTime::from_timestamp_millis(ticker.time as i64);

                let market_data = MarketData {
                    bid: Some(ticker.bid),
                    ask: Some(ticker.ask),
                    bid_qty: Some(ticker.bid_size),
                    ask_qty: Some(ticker.ask_size),
                    exchange_ts_raw: exchange_ts,
                    exchange_ts: None,
                    received_ts: Some(received_ts),
                };

                // PI_XBTUSD -> BTCUSD for registry lookup
                let (base, quote) = self.mapper.parse(&ticker.product_id, self.itype)?;
                let sym = format!("{}{}", base, quote);

                Ok(Some((sym, market_data)))
            }
            WireMessage::Binary(_) => Ok(None),
        }
    }
}

pub async fn listen_perp_bbo(
    data: Arc<MarketDataCollection>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let feed = Arc::new(KrakenFuturesFeed::new_perp());
    listen_with_reconnect(
        data,
        symbols,
        feed,
        "kraken_perp",
        ConnectionConfig::default(),
        shutdown,
    )
    .await
}
