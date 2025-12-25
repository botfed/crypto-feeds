use crate::mappers::{BybitMapper, SymbolMapper};
use crate::market_data::{InstrumentType, MarketData, MarketDataCollection};
use anyhow::Result;
use chrono::{DateTime, Utc};
use futures_util::SinkExt;
use futures_util::stream::SplitSink;
use log::{debug, error};
use serde::Deserialize;
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

use crate::exchange_fees::{ExchangeFees, FeeSchedule};
use crate::exchanges::connection::{
    ConnectionConfig, ExchangeFeed, WireMessage, listen_with_reconnect,
};

pub fn get_fees() -> ExchangeFees {
    ExchangeFees::new(FeeSchedule::new(10.0, 10.0), FeeSchedule::new(5.5, 2.0))
}

#[derive(Clone)]
struct BybitFeed {
    url: &'static str,
    market: InstrumentType,
    mapper: BybitMapper,
}

impl BybitFeed {
    fn new_spot() -> Self {
        Self {
            url: "wss://stream.bybit.com/v5/public/spot",
            market: InstrumentType::Spot,
            mapper: BybitMapper,
        }
    }
    fn new_perp() -> Self {
        Self {
            url: "wss://stream.bybit.com/v5/public/linear",
            market: InstrumentType::Perp,
            mapper: BybitMapper,
        }
    }
}

#[async_trait::async_trait]
impl ExchangeFeed for BybitFeed {
    fn build_url(&self, _symbols: &[&str]) -> Result<String> {
        // Coinbase uses a fixed URL; subscription carries symbols.
        Ok(self.url.to_string())
    }
    async fn send_subscription(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbols: &[&str],
    ) -> Result<()> {
        let args: Vec<String> = symbols
            .iter()
            .map(|symbol| {
                format!(
                    "orderbook.1.{}",
                    self.mapper.denormalize(symbol, self.market).unwrap()
                )
            })
            .collect();

        // Subscribe to spot tickers
        let subscribe_msg = json!({
            "op": "subscribe",
            "args": args
        });

        write
            .send(Message::Text(subscribe_msg.to_string().into()))
            .await?;
        Ok(())
    }

    fn parse_message(
        &self,
        msg: WireMessage<'_>,
        received_ts: DateTime<Utc>,
    ) -> Result<Option<(String, MarketData)>> {
        match msg {
            WireMessage::Text(text) => {
                // Check if it's a subscription confirmation
                if text.contains("\"success\":true") {
                    debug!("Bybit subscription confirmed");
                    return Ok(None);
                }

                // Try to parse as ticker data
                match serde_json::from_str::<BybitResponse>(&text) {
                    Ok(response) => {
                        if response.topic.starts_with("orderbook.") {
                            let bid = response
                                .data
                                .bids
                                .get(0)
                                .and_then(|(price, _)| price.parse::<f64>().ok());
                            let ask = response
                                .data
                                .asks
                                .get(0)
                                .and_then(|(price, _)| price.parse::<f64>().ok());
                            let bid_qty = response
                                .data
                                .bids
                                .get(0)
                                .and_then(|(_, size)| size.parse::<f64>().ok());
                            let ask_qty = response
                                .data
                                .asks
                                .get(0)
                                .and_then(|(_, size)| size.parse::<f64>().ok());

                            let market_data = MarketData {
                                bid,
                                ask,
                                bid_qty,
                                ask_qty,
                                received_ts: Some(received_ts),
                            };
                            return Ok(Some((response.data.symbol, market_data)));
                        }
                    }
                    Err(e) => {
                        // Ignore ping/pong and other messages
                        if !text.contains("\"op\":\"pong\"")
                            && !text.contains("\"ret_msg\":\"pong\"")
                        {
                            error!("Got error parsing bybit message: {} \n {}", e, text);
                        }
                    }
                }
            }
            WireMessage::Binary(_) => {
                return Ok(None);
            }
        }
        Ok(None)
    }
}

#[derive(Debug, Deserialize)]
struct BybitResponse {
    topic: String,
    #[serde(rename = "type")]
    msg_type: String,
    data: BybitOrderbookData,
    ts: u64,
}

#[derive(Debug, Deserialize)]
struct BybitOrderbookData {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b")]
    bids: Vec<(String, String)>,
    #[serde(rename = "a")]
    asks: Vec<(String, String)>,
}

pub async fn listen_spot_bbo(
    data: Arc<Mutex<MarketDataCollection>>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let feed = Arc::new(BybitFeed::new_spot());
    listen_with_reconnect(
        data,
        symbols,
        feed,
        "bybit_spot",
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
    let feed = Arc::new(BybitFeed::new_perp());
    listen_with_reconnect(
        data,
        symbols,
        feed,
        "bybit_perp",
        ConnectionConfig::default(),
        shutdown,
    )
    .await
}
