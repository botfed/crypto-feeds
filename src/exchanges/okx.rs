use crate::exchange_fees::{ExchangeFees, FeeSchedule};
use crate::exchanges::connection::{
    ConnectionConfig, ExchangeFeed, WireMessage, listen_with_reconnect,
};
use crate::mappers::{OkxMapper, SymbolMapper};
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
    ExchangeFees::new(FeeSchedule::new(10.0, 8.0), FeeSchedule::new(5.0, 2.0))
}

#[derive(Clone)]
struct OkxFeed {
    itype: InstrumentType,
    mapper: OkxMapper,
    /// instId → (canonical symbol, ctVal). Precomputed at startup for perp.
    perp_symbol_map: HashMap<String, (String, f64)>,
}

impl OkxFeed {
    fn new_spot() -> Self {
        Self {
            itype: InstrumentType::Spot,
            mapper: OkxMapper,
            perp_symbol_map: HashMap::new(),
        }
    }
    fn new_perp(symbols: &[&str]) -> Result<Self> {
        let mapper = OkxMapper;
        let ct_vals = fetch_contract_values(symbols)?;
        let mut perp_symbol_map = HashMap::new();
        for &sym in symbols {
            if let Ok(native) = mapper.denormalize(sym, InstrumentType::Perp) {
                // native = "BTC-USDT-SWAP"
                let ct_val = ct_vals.get(&native).copied().unwrap_or(1.0);
                perp_symbol_map.insert(native, (sym.to_string(), ct_val));
            }
        }
        Ok(Self {
            itype: InstrumentType::Perp,
            mapper,
            perp_symbol_map,
        })
    }
}

#[derive(Debug, Deserialize)]
struct OkxArg {
    channel: String,
    #[serde(rename = "instId")]
    inst_id: String,
}

#[derive(Debug, Deserialize)]
struct OkxBboData {
    asks: Vec<Vec<String>>,
    bids: Vec<Vec<String>>,
    ts: String,
}

#[derive(Debug, Deserialize)]
struct OkxBboResponse {
    arg: OkxArg,
    data: Vec<OkxBboData>,
}

#[derive(Debug, Deserialize)]
struct OkxInstrument {
    #[serde(rename = "instId")]
    inst_id: String,
    #[serde(rename = "ctVal")]
    ct_val: String,
}

#[derive(Debug, Deserialize)]
struct OkxInstrumentsResponse {
    code: String,
    data: Vec<OkxInstrument>,
}

/// Fetch contract values from OKX REST API.
/// Returns a map of instId (e.g. "BTC-USDT-SWAP") → ctVal (base units per contract).
fn fetch_contract_values(symbols: &[&str]) -> Result<HashMap<String, f64>> {
    let mapper = OkxMapper;
    let native_symbols: Vec<String> = symbols
        .iter()
        .filter_map(|s| mapper.denormalize(s, InstrumentType::Perp).ok())
        .collect();

    let handle = tokio::runtime::Handle::current();
    tokio::task::block_in_place(|| {
        handle.block_on(async {
            let client = reqwest::Client::new();
            let resp: OkxInstrumentsResponse = client
                .get("https://www.okx.com/api/v5/public/instruments?instType=SWAP")
                .send()
                .await?
                .json()
                .await?;
            if resp.code != "0" {
                anyhow::bail!("OKX instruments API returned code: {}", resp.code);
            }
            let mut ct_vals = HashMap::new();
            for inst in &resp.data {
                if native_symbols.contains(&inst.inst_id) {
                    let val: f64 = inst.ct_val.parse().unwrap_or(1.0);
                    info!("OKX contract {}: ctVal={}", inst.inst_id, val);
                    ct_vals.insert(inst.inst_id.clone(), val);
                }
            }
            Ok(ct_vals)
        })
    })
}

#[async_trait::async_trait]
impl ExchangeFeed for OkxFeed {
    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }

    fn build_url(&self, _symbols: &[&str]) -> Result<String> {
        Ok("wss://ws.okx.com:8443/ws/v5/public".to_string())
    }

    async fn send_subscription(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbols: &[&str],
    ) -> Result<()> {
        let args: Vec<serde_json::Value> = symbols
            .iter()
            .map(|symbol| {
                let inst_id = self.mapper.denormalize(symbol, self.itype).unwrap();
                json!({
                    "channel": "bbo-tbt",
                    "instId": inst_id
                })
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

    fn parse_message(
        &self,
        msg: WireMessage<'_>,
        received_ts: DateTime<Utc>,
        received_instant: std::time::Instant,
    ) -> Result<Option<(String, MarketData)>> {
        match msg {
            WireMessage::Text(text) => {
                // OKX sends "pong" as a text response to "ping"
                if text == "pong" {
                    return Ok(None);
                }

                // Subscription confirmations
                if text.contains("\"event\"") {
                    debug!("OKX event: {}", text);
                    return Ok(None);
                }

                match serde_json::from_str::<OkxBboResponse>(text) {
                    Ok(response) => {
                        if response.arg.channel != "bbo-tbt" {
                            return Ok(None);
                        }

                        let entry = match response.data.first() {
                            Some(d) => d,
                            None => return Ok(None),
                        };

                        let bid = entry
                            .bids
                            .first()
                            .and_then(|v| v.first())
                            .and_then(|p| p.parse::<f64>().ok());
                        let ask = entry
                            .asks
                            .first()
                            .and_then(|v| v.first())
                            .and_then(|p| p.parse::<f64>().ok());
                        let mut bid_qty = entry
                            .bids
                            .first()
                            .and_then(|v| v.get(1))
                            .and_then(|q| q.parse::<f64>().ok());
                        let mut ask_qty = entry
                            .asks
                            .first()
                            .and_then(|v| v.get(1))
                            .and_then(|q| q.parse::<f64>().ok());

                        // For perp: lookup canonical symbol + apply contract value
                        let symbol = if let Some((sym, ct_val)) = self.perp_symbol_map.get(&response.arg.inst_id) {
                            bid_qty = bid_qty.map(|q| q * ct_val);
                            ask_qty = ask_qty.map(|q| q * ct_val);
                            sym.clone()
                        } else {
                            // Spot or unknown — strip "-SWAP" suffix for compat
                            response.arg.inst_id
                                .strip_suffix("-SWAP")
                                .unwrap_or(&response.arg.inst_id)
                                .to_string()
                        };

                        let exchange_ts = entry
                            .ts
                            .parse::<i64>()
                            .ok()
                            .and_then(DateTime::from_timestamp_millis);

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

                        Ok(Some((symbol, market_data)))
                    }
                    Err(e) => {
                        error!("Got error parsing OKX message: {} \n {}", e, text);
                        Ok(None)
                    }
                }
            }
            WireMessage::Binary(_) => Ok(None),
        }
    }

    fn heartbeat_message(&self) -> Option<Message> {
        Some(Message::Text("ping".into()))
    }
}

pub async fn listen_spot_bbo(
    data: Arc<MarketDataCollection>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let feed = Arc::new(OkxFeed::new_spot());
    listen_with_reconnect(
        data,
        symbols,
        feed,
        "okx_spot",
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
    let feed = Arc::new(OkxFeed::new_perp(symbols)?);
    listen_with_reconnect(
        data,
        symbols,
        feed,
        "okx_perp",
        ConnectionConfig::default(),
        shutdown,
    )
    .await
}
