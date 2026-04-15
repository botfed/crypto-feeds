use crate::exchange_fees::{ExchangeFees, FeeSchedule};
use crate::exchanges::connection::{
    ConnectionConfig, ExchangeFeed, WireMessage, listen_with_reconnect,
};
use crate::mappers::{KucoinMapper, SymbolMapper};
use crate::market_data::{InstrumentType, MarketData, MarketDataCollection};
use anyhow::Result;
use chrono::{DateTime, Utc};
use futures_util::SinkExt;
use futures_util::stream::SplitSink;
use log::{debug, error, info};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

pub fn get_fees() -> ExchangeFees {
    ExchangeFees::new(FeeSchedule::new(10.0, 10.0), FeeSchedule::new(6.0, 2.0))
}

#[derive(Clone)]
struct KucoinFeed {
    bullet_url: &'static str,
    itype: InstrumentType,
    mapper: KucoinMapper,
    /// Native symbol → (canonical symbol, qty multiplier). Precomputed at startup for perp.
    perp_symbol_map: HashMap<String, (String, f64)>,
}

impl KucoinFeed {
    fn new_spot() -> Self {
        Self {
            bullet_url: "https://api.kucoin.com/api/v1/bullet-public",
            itype: InstrumentType::Spot,
            mapper: KucoinMapper,
            perp_symbol_map: HashMap::new(),
        }
    }
    fn new_perp(symbols: &[&str]) -> Result<Self> {
        let mapper = KucoinMapper;
        let multipliers = fetch_contract_multipliers(symbols)?;
        let mut perp_symbol_map = HashMap::new();
        for &sym in symbols {
            if let Ok(native) = mapper.denormalize(sym, InstrumentType::Perp) {
                let mult = multipliers.get(&native).copied().unwrap_or(1.0);
                perp_symbol_map.insert(native, (sym.to_string(), mult));
            }
        }
        Ok(Self {
            bullet_url: "https://api-futures.kucoin.com/api/v1/bullet-public",
            itype: InstrumentType::Perp,
            mapper,
            perp_symbol_map,
        })
    }
}

#[derive(Debug, Deserialize)]
struct BulletServer {
    endpoint: String,
}

#[derive(Debug, Deserialize)]
struct BulletData {
    token: String,
    #[serde(rename = "instanceServers")]
    instance_servers: Vec<BulletServer>,
}

#[derive(Debug, Deserialize)]
struct BulletResponse {
    code: String,
    data: BulletData,
}

/// Spot ticker data from `/market/ticker:{symbol}`
#[derive(Debug, Deserialize)]
struct KucoinSpotTickerData {
    #[serde(rename = "bestBid")]
    best_bid: Option<String>,
    #[serde(rename = "bestAsk")]
    best_ask: Option<String>,
    #[serde(rename = "bestBidSize")]
    best_bid_size: Option<String>,
    #[serde(rename = "bestAskSize")]
    best_ask_size: Option<String>,
    time: Option<u64>,
}

/// Futures ticker data from `/contractMarket/tickerV2:{symbol}`
#[derive(Debug, Deserialize)]
struct KucoinFuturesTickerData {
    symbol: Option<String>,
    #[serde(rename = "bestBidPrice")]
    best_bid_price: Option<serde_json::Value>,
    #[serde(rename = "bestAskPrice")]
    best_ask_price: Option<serde_json::Value>,
    #[serde(rename = "bestBidSize")]
    best_bid_size: Option<serde_json::Value>,
    #[serde(rename = "bestAskSize")]
    best_ask_size: Option<serde_json::Value>,
    ts: Option<u64>,
}

fn parse_f64(v: &Option<serde_json::Value>) -> Option<f64> {
    match v.as_ref()? {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

#[derive(Debug, Deserialize)]
struct KucoinMessage {
    #[serde(rename = "type")]
    msg_type: String,
    topic: Option<String>,
    data: Option<serde_json::Value>,
}

fn fetch_bullet_token(bullet_url: &str) -> Result<String> {
    let handle = tokio::runtime::Handle::current();
    tokio::task::block_in_place(|| {
        handle.block_on(async {
            let client = reqwest::Client::new();
            let resp: BulletResponse = client.post(bullet_url).send().await?.json().await?;
            if resp.code != "200000" {
                anyhow::bail!("KuCoin bullet-public returned code: {}", resp.code);
            }
            let server = resp
                .data
                .instance_servers
                .first()
                .ok_or_else(|| anyhow::anyhow!("No instance servers returned"))?;
            let ts = Utc::now().timestamp_millis();
            let url = format!(
                "{}?token={}&connectId={}",
                server.endpoint, resp.data.token, ts
            );
            info!("KuCoin WS endpoint: {}", server.endpoint);
            Ok(url)
        })
    })
}

#[derive(Debug, Deserialize)]
struct KucoinContractInfo {
    symbol: String,
    multiplier: f64,
}

#[derive(Debug, Deserialize)]
struct KucoinContractsResponse {
    code: String,
    data: Vec<KucoinContractInfo>,
}

/// Fetch contract multipliers from Kucoin futures REST API.
/// Returns a map of native symbol (e.g. "XBTUSDTM") → multiplier (base units per contract).
fn fetch_contract_multipliers(symbols: &[&str]) -> Result<HashMap<String, f64>> {
    let mapper = KucoinMapper;
    // Convert canonical symbols to native Kucoin format for filtering
    let native_symbols: Vec<String> = symbols
        .iter()
        .filter_map(|s| mapper.denormalize(s, InstrumentType::Perp).ok())
        .collect();

    let handle = tokio::runtime::Handle::current();
    tokio::task::block_in_place(|| {
        handle.block_on(async {
            let client = reqwest::Client::new();
            let resp: KucoinContractsResponse = client
                .get("https://api-futures.kucoin.com/api/v1/contracts/active")
                .send()
                .await?
                .json()
                .await?;
            if resp.code != "200000" {
                anyhow::bail!("KuCoin contracts API returned code: {}", resp.code);
            }
            let mut multipliers = HashMap::new();
            for contract in &resp.data {
                if native_symbols.contains(&contract.symbol) {
                    info!(
                        "KuCoin contract {}: multiplier={}",
                        contract.symbol, contract.multiplier
                    );
                    multipliers.insert(contract.symbol.clone(), contract.multiplier);
                }
            }
            Ok(multipliers)
        })
    })
}

#[async_trait::async_trait]
impl ExchangeFeed for KucoinFeed {
    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }

    fn build_url(&self, _symbols: &[&str]) -> Result<String> {
        fetch_bullet_token(self.bullet_url)
    }

    async fn send_subscription(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbols: &[&str],
    ) -> Result<()> {
        match self.itype {
            InstrumentType::Spot => {
                let native_symbols: Vec<String> = symbols
                    .iter()
                    .map(|s| self.mapper.denormalize(s, self.itype).unwrap())
                    .collect();
                let topic = format!("/market/ticker:{}", native_symbols.join(","));
                let msg = json!({
                    "id": Utc::now().timestamp_millis().to_string(),
                    "type": "subscribe",
                    "topic": topic,
                    "response": true
                });
                write
                    .send(Message::Text(msg.to_string().into()))
                    .await?;
            }
            InstrumentType::Perp => {
                let native_symbols: Vec<String> = symbols
                    .iter()
                    .map(|s| self.mapper.denormalize(s, self.itype).unwrap())
                    .collect();
                let topic = format!(
                    "/contractMarket/tickerV2:{}",
                    native_symbols.join(",")
                );
                let msg = json!({
                    "id": Utc::now().timestamp_millis().to_string(),
                    "type": "subscribe",
                    "topic": topic,
                    "response": true
                });
                write
                    .send(Message::Text(msg.to_string().into()))
                    .await?;
            }
            _ => {}
        }
        Ok(())
    }

    fn parse_message(
        &self,
        msg: WireMessage<'_>,
        received_ts: DateTime<Utc>,
        received_instant: std::time::Instant,
    ) -> Result<Option<(String, MarketData)>> {
        match msg {
            WireMessage::Text(text) => {
                let envelope: KucoinMessage = match serde_json::from_str(text) {
                    Ok(m) => m,
                    Err(e) => {
                        error!("KuCoin parse error: {} — {}", e, text);
                        return Ok(None);
                    }
                };

                match envelope.msg_type.as_str() {
                    "pong" | "welcome" | "ack" => return Ok(None),
                    "message" => {}
                    other => {
                        debug!("KuCoin unknown type: {}", other);
                        return Ok(None);
                    }
                }

                let topic = match &envelope.topic {
                    Some(t) => t,
                    None => return Ok(None),
                };
                let data = match &envelope.data {
                    Some(d) => d,
                    None => return Ok(None),
                };

                if topic.starts_with("/market/ticker:") {
                    // Spot ticker
                    let ticker: KucoinSpotTickerData = serde_json::from_value(data.clone())?;
                    let symbol = topic
                        .strip_prefix("/market/ticker:")
                        .unwrap_or("")
                        .to_string();

                    let bid = ticker.best_bid.as_deref().and_then(|s| s.parse::<f64>().ok());
                    let ask = ticker.best_ask.as_deref().and_then(|s| s.parse::<f64>().ok());
                    let bid_qty = ticker.best_bid_size.as_deref().and_then(|s| s.parse::<f64>().ok());
                    let ask_qty = ticker.best_ask_size.as_deref().and_then(|s| s.parse::<f64>().ok());
                    let exchange_ts = ticker
                        .time
                        .and_then(|ms| DateTime::from_timestamp_millis(ms as i64));

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
                            received_instant: Some(received_instant),
                        },
                    )))
                } else if topic.starts_with("/contractMarket/tickerV2:") {
                    // Futures ticker — native symbol e.g. "XBTUSDTM"
                    // Normalize back to canonical form e.g. "BTC-USDT"
                    let ticker: KucoinFuturesTickerData = serde_json::from_value(data.clone())?;
                    let native = ticker
                        .symbol
                        .or_else(|| {
                            topic
                                .strip_prefix("/contractMarket/tickerV2:")
                                .map(|s| s.to_string())
                        })
                        .unwrap_or_default();
                    let (symbol, mult) = match self.perp_symbol_map.get(&native) {
                        Some((sym, m)) => (sym.clone(), *m),
                        None => return Ok(None),
                    };

                    let bid = parse_f64(&ticker.best_bid_price);
                    let ask = parse_f64(&ticker.best_ask_price);
                    let bid_qty = parse_f64(&ticker.best_bid_size).map(|q| q * mult);
                    let ask_qty = parse_f64(&ticker.best_ask_size).map(|q| q * mult);
                    // ts is in nanoseconds
                    let exchange_ts = ticker
                        .ts
                        .and_then(|ns| DateTime::from_timestamp_nanos(ns as i64).into());

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
                            received_instant: Some(received_instant),
                        },
                    )))
                } else {
                    debug!("KuCoin unknown topic: {}", topic);
                    Ok(None)
                }
            }
            WireMessage::Binary(_) => Ok(None),
        }
    }

    fn heartbeat_message(&self) -> Option<Message> {
        let msg = json!({
            "id": chrono::Utc::now().timestamp_millis().to_string(),
            "type": "ping"
        });
        Some(Message::Text(msg.to_string().into()))
    }
}

pub async fn listen_spot_bbo(
    data: Arc<MarketDataCollection>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let feed = Arc::new(KucoinFeed::new_spot());
    listen_with_reconnect(
        data,
        symbols,
        feed,
        "kucoin_spot",
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
    let feed = Arc::new(KucoinFeed::new_perp(symbols)?);
    listen_with_reconnect(
        data,
        symbols,
        feed,
        "kucoin_perp",
        ConnectionConfig::default(),
        shutdown,
    )
    .await
}
