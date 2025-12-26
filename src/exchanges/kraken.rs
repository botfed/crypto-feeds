use crate::mappers::{KrakenMapper, SymbolMapper};
use crate::market_data::{InstrumentType, MarketData, MarketDataCollection};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, stream::SplitSink};
use log::debug;
use serde_json::json;
use std::sync::{Arc, Mutex};
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
        match self.itype {
            InstrumentType::Spot => Ok("wss://ws.kraken.com".to_string()),
            InstrumentType::Perp => Ok("wss://ws.kraken.com".to_string()),
            _ => anyhow::bail!("Invalid instrument type"),
        }
    }

    async fn send_subscription(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbols: &[&str],
    ) -> Result<()> {
        match self.itype {
            InstrumentType::Spot => {
                // Kraken uses pairs like "XBT/USD", "ETH/USD"
                let pairs: Vec<String> = symbols
                    .iter()
                    .map(|s| self.mapper.denormalize(s, self.itype))
                    .collect::<Result<Vec<_>, _>>()?;
                let subscribe_msg = json!({
                    "event": "subscribe",
                    "pair": pairs,
                    "subscription": {
                        "name": "spread"  // Spread channel gives BBO
                    }
                });
                write
                    .send(Message::Text(subscribe_msg.to_string().into()))
                    .await
                    .context("Failed to subscribe to kraken")?;
                Ok(())
            }
            InstrumentType::Perp => Ok(()),
            _ => anyhow::bail!("Invalid instrument type"),
        }
    }
    fn parse_message(
        &self,
        msg: WireMessage<'_>,
        received_ts: DateTime<Utc>,
    ) -> Result<Option<(String, MarketData)>> {
        match self.itype {
            InstrumentType::Perp => Ok(None),
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

                                                let market_data = MarketData {
                                                    bid,
                                                    ask,
                                                    bid_qty,
                                                    ask_qty,
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
    data: Arc<Mutex<MarketDataCollection>>,
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
