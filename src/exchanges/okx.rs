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
use log::{debug, error};
use serde::Deserialize;
use serde_json::json;
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
}

impl OkxFeed {
    fn new_spot() -> Self {
        Self {
            itype: InstrumentType::Spot,
            mapper: OkxMapper,
        }
    }
    fn new_perp() -> Self {
        Self {
            itype: InstrumentType::Perp,
            mapper: OkxMapper,
        }
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
                        let bid_qty = entry
                            .bids
                            .first()
                            .and_then(|v| v.get(1))
                            .and_then(|q| q.parse::<f64>().ok());
                        let ask_qty = entry
                            .asks
                            .first()
                            .and_then(|v| v.get(1))
                            .and_then(|q| q.parse::<f64>().ok());

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
                            exchange_ts,
                            received_ts: Some(received_ts),
                        };

                        // Strip "-SWAP" suffix so the symbol matches registry aliases
                        // e.g. "BTC-USDT-SWAP" -> "BTC-USDT"
                        let symbol = response
                            .arg
                            .inst_id
                            .strip_suffix("-SWAP")
                            .unwrap_or(&response.arg.inst_id)
                            .to_string();

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
    let feed = Arc::new(OkxFeed::new_perp());
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
