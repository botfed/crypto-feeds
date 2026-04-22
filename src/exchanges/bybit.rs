use crate::mappers::{BybitMapper, SymbolMapper};
use crate::market_data::{InstrumentType, MarketData, MarketDataCollection};
use crate::trade_data::{TradeData, TradeDataCollection, TradeSide};
use anyhow::Result;
use chrono::{DateTime, Utc};
use futures_util::SinkExt;
use futures_util::stream::SplitSink;
use log::{debug, error};
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
    ExchangeFees::new(FeeSchedule::new(10.0, 10.0), FeeSchedule::new(5.5, 2.0))
}

#[derive(Clone)]
struct BybitFeed {
    url: &'static str,
    itype: InstrumentType,
    mapper: BybitMapper,
}

impl BybitFeed {
    fn new_spot() -> Self {
        Self {
            url: "wss://stream.bybit.com/v5/public/spot",
            itype: InstrumentType::Spot,
            mapper: BybitMapper,
        }
    }
    fn new_perp() -> Self {
        Self {
            url: "wss://stream.bybit.com/v5/public/linear",
            itype: InstrumentType::Perp,
            mapper: BybitMapper,
        }
    }
}

#[async_trait::async_trait]
impl ExchangeFeed for BybitFeed {
    type Item = MarketData;

    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }
    fn heartbeat_message(&self) -> Option<Message> {
        // Bybit requires application-level {"op":"ping"} every 20s;
        // raw WebSocket pings are ignored and the server disconnects.
        Some(Message::Text(r#"{"op":"ping"}"#.into()))
    }

    fn build_url(&self, _symbols: &[&str]) -> Result<String> {
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
                    self.mapper.denormalize(symbol, self.itype).unwrap()
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
        received_instant: std::time::Instant,
    ) -> Result<Vec<(String, MarketData)>> {
        match msg {
            WireMessage::Text(text) => {
                // Check if it's a subscription confirmation
                if text.contains("\"success\":true") {
                    debug!("Bybit subscription confirmed");
                    return Ok(vec![]);
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

                            let exchange_ts =
                                DateTime::from_timestamp_millis(response.ts as i64)
                                    .map(|dt| dt.with_timezone(&Utc));

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
                            return Ok(vec![(response.data.symbol, market_data)]);
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
                return Ok(vec![]);
            }
        }
        Ok(vec![])
    }
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
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
    data: Arc<MarketDataCollection>,
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
    data: Arc<MarketDataCollection>,
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

// --- Public Trade Feed ---

#[derive(Debug, Deserialize)]
struct BybitTradeResponse {
    topic: String,
    #[serde(rename = "type")]
    _msg_type: String,
    data: Vec<BybitTradeEntry>,
    #[allow(dead_code)]
    ts: u64,
}

#[derive(Debug, Deserialize)]
struct BybitTradeEntry {
    #[serde(rename = "T")]
    trade_time: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "S")]
    side: String,
    #[serde(rename = "v")]
    size: String,
    #[serde(rename = "p")]
    price: String,
}

#[derive(Clone)]
struct BybitTradeFeed {
    url: &'static str,
    itype: InstrumentType,
    mapper: BybitMapper,
}

impl BybitTradeFeed {
    fn new_perp() -> Self {
        Self {
            url: "wss://stream.bybit.com/v5/public/linear",
            itype: InstrumentType::Perp,
            mapper: BybitMapper,
        }
    }
}

#[async_trait::async_trait]
impl ExchangeFeed for BybitTradeFeed {
    type Item = TradeData;

    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }

    fn heartbeat_message(&self) -> Option<Message> {
        Some(Message::Text(r#"{"op":"ping"}"#.into()))
    }

    fn build_url(&self, _symbols: &[&str]) -> Result<String> {
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
                    "publicTrade.{}",
                    self.mapper.denormalize(symbol, self.itype).unwrap()
                )
            })
            .collect();

        let subscribe_msg = json!({
            "op": "subscribe",
            "args": args
        });

        write
            .send(Message::Text(subscribe_msg.to_string().into()))
            .await?;
        Ok(())
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
                if text.contains("\"success\":true")
                    || text.contains("\"op\":\"pong\"")
                    || text.contains("\"ret_msg\":\"pong\"")
                {
                    return Ok(vec![]);
                }

                let response = match serde_json::from_str::<BybitTradeResponse>(text) {
                    Ok(r) => r,
                    Err(e) => {
                        debug!("Bybit trade parse skip: {}", e);
                        return Ok(vec![]);
                    }
                };

                if !response.topic.starts_with("publicTrade.") {
                    return Ok(vec![]);
                }

                let mut trades = Vec::with_capacity(response.data.len());
                for entry in response.data {
                    let price = match entry.price.parse::<f64>() {
                        Ok(p) => p,
                        Err(_) => continue,
                    };
                    let qty = match entry.size.parse::<f64>() {
                        Ok(q) => q,
                        Err(_) => continue,
                    };
                    let side = match entry.side.as_str() {
                        "Buy" => TradeSide::Buy,
                        "Sell" => TradeSide::Sell,
                        _ => TradeSide::Unknown,
                    };
                    let exchange_ts = DateTime::from_timestamp_millis(entry.trade_time as i64);

                    trades.push((
                        entry.symbol,
                        TradeData {
                            price,
                            qty,
                            side,
                            exchange_ts_raw: exchange_ts,
                            exchange_ts: None,
                            received_ts: Some(received_ts),
                            received_instant: Some(received_instant),
                            feed_latency_ns: 0,
                        },
                    ));
                }
                Ok(trades)
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
    let feed = Arc::new(BybitTradeFeed::new_perp());
    listen_with_reconnect(
        data,
        symbols,
        feed,
        "bybit_perp_trades",
        ConnectionConfig::default(),
        shutdown,
    )
    .await
}
