use crate::exchange_fees::{ExchangeFees, FeeSchedule};
use crate::exchanges::connection::{
    ConnectionConfig, ExchangeFeed, WireMessage, listen_with_reconnect,
};
use crate::mappers::{BingxMapper, SymbolMapper};
use crate::market_data::{InstrumentType, MarketData, MarketDataCollection};
use anyhow::Result;
use chrono::{DateTime, Utc};
use futures_util::SinkExt;
use futures_util::stream::SplitSink;
use log::{debug, error};
use serde::Deserialize;
use serde_json::json;
use std::io::Read as IoRead;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

pub fn get_fees() -> ExchangeFees {
    ExchangeFees::new(FeeSchedule::new(10.0, 10.0), FeeSchedule::new(5.0, 2.0))
}

struct BingxFeed {
    url: &'static str,
    itype: InstrumentType,
    mapper: BingxMapper,
    got_ping: std::sync::atomic::AtomicBool,
}

impl BingxFeed {
    fn new_spot() -> Self {
        Self {
            url: "wss://open-api-ws.bingx.com/market",
            itype: InstrumentType::Spot,
            mapper: BingxMapper,
            got_ping: std::sync::atomic::AtomicBool::new(false),
        }
    }
    fn new_perp() -> Self {
        Self {
            url: "wss://open-api-swap.bingx.com/swap-market",
            itype: InstrumentType::Perp,
            mapper: BingxMapper,
            got_ping: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

/// BingX bookTicker uses short field names:
///   s = symbol, b = best bid price, B = best bid qty,
///   a = best ask price, A = best ask qty, E = update time (ms)
#[derive(Debug, Deserialize)]
struct BingxBookTickerData {
    s: Option<String>,
    b: Option<String>,
    #[serde(rename = "B")]
    bid_qty: Option<serde_json::Value>,
    a: Option<String>,
    #[serde(rename = "A")]
    ask_qty: Option<serde_json::Value>,
    #[serde(rename = "E")]
    event_time: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct BingxMessage {
    #[serde(rename = "dataType")]
    data_type: Option<String>,
    data: Option<serde_json::Value>,
}

fn decompress_gzip(data: &[u8]) -> Result<String> {
    let mut decoder = flate2::read::GzDecoder::new(data);
    let mut text = String::new();
    decoder.read_to_string(&mut text)?;
    Ok(text)
}

fn parse_str_or_num(v: &Option<serde_json::Value>) -> Option<f64> {
    match v.as_ref()? {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn parse_bingx_text(text: &str, received_ts: DateTime<Utc>, received_instant: std::time::Instant) -> Result<Vec<(String, MarketData)>> {
    if text == "Ping" || text == "Pong" || text == "ping" || text == "pong" {
        return Ok(vec![]);
    }

    let envelope: BingxMessage = match serde_json::from_str(text) {
        Ok(m) => m,
        Err(e) => {
            error!("BingX parse error: {} — {}", e, text);
            return Ok(vec![]);
        }
    };

    // Subscription confirmations or errors
    if envelope.data_type.is_none() {
        debug!("BingX non-data message: {}", text);
        return Ok(vec![]);
    }

    let data_type = envelope.data_type.as_deref().unwrap_or("");
    if !data_type.ends_with("@bookTicker") {
        debug!("BingX ignoring dataType: {}", data_type);
        return Ok(vec![]);
    }

    let data_val = match &envelope.data {
        Some(d) => d,
        None => return Ok(vec![]),
    };

    let ticker: BingxBookTickerData = match data_val {
        serde_json::Value::String(s) => serde_json::from_str(s)?,
        other => serde_json::from_value(other.clone())?,
    };

    let symbol = ticker
        .s
        .or_else(|| {
            data_type.strip_suffix("@bookTicker").map(|s| s.to_string())
        })
        .unwrap_or_default();

    let bid = ticker.b.as_deref().and_then(|s| s.parse::<f64>().ok());
    let ask = ticker.a.as_deref().and_then(|s| s.parse::<f64>().ok());
    let bid_qty = parse_str_or_num(&ticker.bid_qty);
    let ask_qty = parse_str_or_num(&ticker.ask_qty);
    let exchange_ts = ticker
        .event_time
        .and_then(|ms| DateTime::from_timestamp_millis(ms as i64));

    Ok(vec![(
        symbol,
        MarketData {
            bid,
            ask,
            bid_qty,
            ask_qty,
            exchange_ts_raw: exchange_ts,
            exchange_ts: None,
            received_ts: Some(received_ts),
            received_instant: Some(received_instant),
                    feed_latency_ns: 0,
        },
    )])
}

#[async_trait::async_trait]
impl ExchangeFeed for BingxFeed {
    type Item = MarketData;

    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }

    fn build_url(&self, _symbols: &[&str]) -> Result<String> {
        Ok(self.url.to_string())
    }

    async fn send_subscription(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbols: &[&str],
    ) -> Result<()> {
        for symbol in symbols {
            let native = self.mapper.denormalize(symbol, self.itype)?;
            let msg = json!({
                "id": Utc::now().timestamp_millis().to_string(),
                "reqType": "sub",
                "dataType": format!("{}@bookTicker", native)
            });
            write
                .send(Message::Text(msg.to_string().into()))
                .await?;
        }
        Ok(())
    }

    async fn process_other(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        text: &str,
    ) -> Result<()> {
        if text == "Ping" || text == "ping" {
            write.send(Message::Text("Pong".into())).await?;
        }
        Ok(())
    }

    fn parse_message(
        &self,
        msg: WireMessage<'_>,
        received_ts: DateTime<Utc>,
        received_instant: std::time::Instant,
    ) -> Result<Vec<(String, MarketData)>> {
        match msg {
            WireMessage::Text(text) => parse_bingx_text(text, received_ts, received_instant),
            WireMessage::Binary(data) => {
                match decompress_gzip(data) {
                    Ok(text) => {
                        if text == "Ping" || text == "ping" {
                            self.got_ping.store(true, std::sync::atomic::Ordering::Relaxed);
                            return Ok(vec![]);
                        }
                        parse_bingx_text(&text, received_ts, received_instant)
                    }
                    Err(e) => {
                        error!("BingX gzip decompress error: {}", e);
                        Ok(vec![])
                    }
                }
            }
        }
    }

    fn heartbeat_message(&self) -> Option<Message> {
        if self.got_ping.swap(false, std::sync::atomic::Ordering::Relaxed) {
            return Some(Message::Text("Pong".into()));
        }
        Some(Message::Text("Pong".into()))
    }
}

pub async fn listen_spot_bbo(
    data: Arc<MarketDataCollection>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let feed = Arc::new(BingxFeed::new_spot());
    listen_with_reconnect(
        data,
        symbols,
        feed,
        "bingx_spot",
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
    let feed = Arc::new(BingxFeed::new_perp());
    listen_with_reconnect(
        data,
        symbols,
        feed,
        "bingx_perp",
        ConnectionConfig::default(),
        shutdown,
    )
    .await
}
