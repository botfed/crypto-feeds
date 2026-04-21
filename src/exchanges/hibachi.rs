use crate::exchange_fees::{ExchangeFees, FeeSchedule};
use crate::exchanges::connection::{
    ConnectionConfig, ExchangeFeed, WireMessage, listen_with_reconnect,
};
use crate::market_data::{InstrumentType, MarketData, MarketDataCollection};
use anyhow::Result;
use chrono::{DateTime, Utc};
use futures_util::SinkExt;
use futures_util::stream::SplitSink;
use log::debug;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

pub fn get_fees() -> ExchangeFees {
    // Hibachi fee schedule (bps): maker, taker
    ExchangeFees::new(FeeSchedule::new(2.0, 5.0), FeeSchedule::new(2.0, 5.0))
}

#[derive(Clone)]
struct HibachiFeed {
    itype: InstrumentType,
    /// Maps hibachi native symbol ("BTC/USDT-P") → registry-compatible symbol ("BTCUSDT")
    native_to_registry: HashMap<String, String>,
}

impl HibachiFeed {
    fn new(symbols: &[&str], itype: InstrumentType) -> Self {
        let mut native_to_registry = HashMap::new();
        for sym in symbols {
            let parts: Vec<&str> = sym.split('_').collect();
            let (base, quote) = if parts.len() == 3 {
                (parts[1], parts[2])
            } else if parts.len() == 2 {
                (parts[0], parts[1])
            } else {
                continue;
            };
            let native = match itype {
                InstrumentType::Perp => format!("{}/{}-P", base.to_uppercase(), quote.to_uppercase()),
                _ => format!("{}/{}", base.to_uppercase(), quote.to_uppercase()),
            };
            let registry = format!("{}{}", base.to_uppercase(), quote.to_uppercase());
            native_to_registry.insert(native, registry);
        }
        Self {
            itype,
            native_to_registry,
        }
    }

    /// Convert config symbol ("PERP_BTC_USDT" or "BTC_USDT") to hibachi native ("BTC/USDT-P")
    fn to_native(sym: &str, itype: InstrumentType) -> String {
        let parts: Vec<&str> = sym.split('_').collect();
        let (base, quote) = if parts.len() == 3 {
            (parts[1], parts[2])
        } else if parts.len() == 2 {
            (parts[0], parts[1])
        } else {
            return sym.to_string();
        };
        match itype {
            InstrumentType::Perp => format!("{}/{}-P", base.to_uppercase(), quote.to_uppercase()),
            _ => format!("{}/{}", base.to_uppercase(), quote.to_uppercase()),
        }
    }
}

#[derive(Debug, Deserialize)]
struct HibachiAskBidPrice {
    symbol: String,
    #[serde(rename = "askPrice")]
    ask_price: String,
    #[serde(rename = "bidPrice")]
    bid_price: String,
}

#[async_trait::async_trait]
impl ExchangeFeed for HibachiFeed {
    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }

    fn build_url(&self, _symbols: &[&str]) -> Result<String> {
        Ok("wss://data-api.hibachi.xyz/ws/market".to_string())
    }

    async fn send_subscription(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbols: &[&str],
    ) -> Result<()> {
        let subscriptions: Vec<serde_json::Value> = symbols
            .iter()
            .map(|sym| {
                let native = Self::to_native(sym, self.itype);
                json!({"symbol": native, "topic": "ask_bid_price"})
            })
            .collect();

        let sub_msg = json!({
            "method": "subscribe",
            "parameters": {
                "subscriptions": subscriptions
            }
        });
        write
            .send(Message::Text(sub_msg.to_string().into()))
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
                // Quick filter: only parse ask_bid_price messages
                if !text.contains("\"ask_bid_price\"") {
                    debug!("Hibachi non-bbo message");
                    return Ok(None);
                }

                let msg: HibachiAskBidPrice = serde_json::from_str(text)?;

                let registry_sym = match self.native_to_registry.get(&msg.symbol) {
                    Some(s) => s.clone(),
                    None => {
                        debug!("Unknown symbol from Hibachi: {}", msg.symbol);
                        return Ok(None);
                    }
                };

                let bid = msg.bid_price.parse::<f64>().ok();
                let ask = msg.ask_price.parse::<f64>().ok();

                let market_data = MarketData {
                    bid,
                    ask,
                    bid_qty: None,
                    ask_qty: None,
                    exchange_ts_raw: None,
                    exchange_ts: None,
                    received_ts: Some(received_ts),
                    received_instant: Some(received_instant),
                    feed_latency_ns: 0,
                };

                Ok(Some((registry_sym, market_data)))
            }
            WireMessage::Binary(_) => Ok(None),
        }
    }

    fn timestamp_dedup(&self) -> bool {
        // ask_bid_price messages may not have timestamps, disable dedup
        false
    }
}

pub async fn listen_perp_bbo(
    data: Arc<MarketDataCollection>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let feed = Arc::new(HibachiFeed::new(symbols, InstrumentType::Perp));
    listen_with_reconnect(
        data,
        symbols,
        feed,
        "hibachi_perp",
        ConnectionConfig::default(),
        shutdown,
    )
    .await
}

pub async fn listen_spot_bbo(
    data: Arc<MarketDataCollection>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let feed = Arc::new(HibachiFeed::new(symbols, InstrumentType::Spot));
    listen_with_reconnect(
        data,
        symbols,
        feed,
        "hibachi_spot",
        ConnectionConfig::default(),
        shutdown,
    )
    .await
}
