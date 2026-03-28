use crate::exchange_fees::{ExchangeFees, FeeSchedule};
use crate::exchanges::connection::{
    ConnectionConfig, ExchangeFeed, WireMessage, listen_with_reconnect,
};
use crate::mappers::{ApexMapper, SymbolMapper};
use crate::market_data::{InstrumentType, MarketData, MarketDataCollection};
use anyhow::Result;
use chrono::{DateTime, Utc};
use futures_util::SinkExt;
use futures_util::stream::SplitSink;
use log::{debug, error};
use ordered_float::OrderedFloat;
use serde::Deserialize;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

pub fn get_fees() -> ExchangeFees {
    ExchangeFees::new(FeeSchedule::new(10.0, 10.0), FeeSchedule::new(5.0, 2.0))
}

/// Local orderbook tracking best bid/ask across snapshot + delta updates.
struct LocalBook {
    bids: BTreeMap<OrderedFloat<f64>, f64>,
    asks: BTreeMap<OrderedFloat<f64>, f64>,
}

impl LocalBook {
    fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    fn apply(&mut self, bids: &[[String; 2]], asks: &[[String; 2]], is_snapshot: bool) {
        if is_snapshot {
            self.bids.clear();
            self.asks.clear();
        }
        for [p, q] in bids {
            if let (Ok(price), Ok(qty)) = (p.parse::<f64>(), q.parse::<f64>()) {
                if qty == 0.0 {
                    self.bids.remove(&OrderedFloat(price));
                } else {
                    self.bids.insert(OrderedFloat(price), qty);
                }
            }
        }
        for [p, q] in asks {
            if let (Ok(price), Ok(qty)) = (p.parse::<f64>(), q.parse::<f64>()) {
                if qty == 0.0 {
                    self.asks.remove(&OrderedFloat(price));
                } else {
                    self.asks.insert(OrderedFloat(price), qty);
                }
            }
        }
    }

    fn best_bid(&self) -> Option<(f64, f64)> {
        self.bids.iter().next_back().map(|(p, q)| (p.0, *q))
    }

    fn best_ask(&self) -> Option<(f64, f64)> {
        self.asks.iter().next().map(|(p, q)| (p.0, *q))
    }
}

struct ApexFeed {
    mapper: ApexMapper,
    books: std::sync::Mutex<rustc_hash::FxHashMap<String, LocalBook>>,
}

impl ApexFeed {
    fn new() -> Self {
        Self {
            mapper: ApexMapper,
            books: std::sync::Mutex::new(rustc_hash::FxHashMap::default()),
        }
    }
}

#[derive(Debug, Deserialize)]
struct ApexOrderbookData {
    s: String,
    #[serde(default)]
    b: Vec<[String; 2]>,
    #[serde(default)]
    a: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
struct ApexOrderbookMsg {
    topic: String,
    #[serde(rename = "type")]
    msg_type: Option<String>,
    data: ApexOrderbookData,
    ts: Option<u64>,
}

#[async_trait::async_trait]
impl ExchangeFeed for ApexFeed {
    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&InstrumentType::Perp)
    }

    fn timestamp_dedup(&self) -> bool {
        false
    }

    fn build_url(&self, _symbols: &[&str]) -> Result<String> {
        let ts = Utc::now().timestamp_millis();
        Ok(format!(
            "wss://quote.omni.apex.exchange/realtime_public?v=2&timestamp={}",
            ts
        ))
    }

    async fn send_subscription(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbols: &[&str],
    ) -> Result<()> {
        let args: Vec<String> = symbols
            .iter()
            .map(|s| {
                let native = self.mapper.denormalize(s, InstrumentType::Perp).unwrap();
                format!("orderBook25.H.{}", native)
            })
            .collect();

        let msg = json!({
            "op": "subscribe",
            "args": args
        });
        write
            .send(Message::Text(msg.to_string().into()))
            .await?;
        Ok(())
    }

    async fn process_other(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        text: &str,
    ) -> Result<()> {
        if text.contains("\"op\":\"ping\"") {
            let ts = Utc::now().timestamp_millis().to_string();
            let pong = json!({"op": "pong", "args": [ts]});
            write.send(Message::Text(pong.to_string().into())).await?;
        }
        Ok(())
    }

    fn parse_message(
        &self,
        msg: WireMessage<'_>,
        received_ts: DateTime<Utc>,
    ) -> Result<Option<(String, MarketData)>> {
        match msg {
            WireMessage::Text(text) => {
                if text.contains("\"success\":true") || text.contains("\"op\":\"pong\"") {
                    return Ok(None);
                }
                if text.contains("\"op\":\"ping\"") {
                    return Ok(None);
                }

                let response: ApexOrderbookMsg = match serde_json::from_str(text) {
                    Ok(r) => r,
                    Err(e) => {
                        if !text.contains("\"op\"") {
                            error!("Apex parse error: {} — {}", e, &text[..text.len().min(200)]);
                        }
                        return Ok(None);
                    }
                };

                if !response.topic.starts_with("orderBook") {
                    debug!("Apex ignoring topic: {}", response.topic);
                    return Ok(None);
                }

                let is_snapshot = response
                    .msg_type
                    .as_deref()
                    .map_or(false, |t| t == "snapshot");

                let symbol = response.data.s.clone();

                // Update local book and extract BBO
                let (bid, ask, bid_qty, ask_qty) = {
                    let mut books = self.books.lock().unwrap();
                    let book = books
                        .entry(symbol.clone())
                        .or_insert_with(LocalBook::new);
                    book.apply(&response.data.b, &response.data.a, is_snapshot);
                    let (bid, bq) = book.best_bid().unzip();
                    let (ask, aq) = book.best_ask().unzip();
                    (bid, ask, bq, aq)
                };

                // ts is microseconds
                let exchange_ts = response
                    .ts
                    .and_then(|us| DateTime::from_timestamp_micros(us as i64));

                Ok(Some((
                    symbol,
                    MarketData {
                        bid,
                        ask,
                        bid_qty,
                        ask_qty,
                        exchange_ts_raw: exchange_ts,
                        exchange_ts: None,
                        received_ts: Some(received_ts),
                    },
                )))
            }
            WireMessage::Binary(_) => Ok(None),
        }
    }

    fn heartbeat_message(&self) -> Option<Message> {
        let ts = Utc::now().timestamp_millis().to_string();
        let ping = json!({"op": "ping", "args": [ts]});
        Some(Message::Text(ping.to_string().into()))
    }
}

pub async fn listen_perp_bbo(
    data: Arc<MarketDataCollection>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let feed = Arc::new(ApexFeed::new());
    let config = ConnectionConfig {
        heartbeat_interval: std::time::Duration::from_secs(15),
        ..ConnectionConfig::default()
    };
    listen_with_reconnect(data, symbols, feed, "apex_perp", config, shutdown).await
}
