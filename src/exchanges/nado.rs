use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use futures_util::SinkExt;
use futures_util::stream::SplitSink;
use log::{debug, error};
use reqwest::Client;
use reqwest::header::ACCEPT_ENCODING;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

use crate::exchange_fees::{ExchangeFees, FeeSchedule};
use crate::exchanges::connection::{
    ConnectionConfig, ExchangeFeed, WireMessage, listen_with_reconnect,
};
use crate::mappers::{NadoMapper, SymbolMapper};
use crate::market_data::{InstrumentType, MarketData, MarketDataCollection};

const X18: f64 = 1e18;

pub fn get_fees() -> ExchangeFees {
    // Nado: 2.5 bps taker, 0 bps maker (standard perp-dex fees)
    ExchangeFees::new(FeeSchedule::new(0.0, 0.0), FeeSchedule::new(2.5, 0.0))
}

#[derive(Debug, Deserialize)]
struct PairRow {
    product_id: u32,
    base: String,
}

async fn fetch_pairs(client: &Client) -> Result<Vec<PairRow>> {
    let url = "https://gateway.prod.nado.xyz/v2/pairs";
    let resp = client
        .get(url)
        .header(ACCEPT_ENCODING, "gzip")
        .send()
        .await
        .with_context(|| format!("GET {url} failed"))?;

    let status = resp.status();
    let body = resp.text().await.context("read response body")?;

    if !status.is_success() {
        anyhow::bail!("GET {url} -> {status}; body: {body}");
    }

    let rows: Vec<PairRow> = serde_json::from_str(&body).context("decode pairs JSON")?;
    Ok(rows)
}

struct NadoFeed {
    /// product_id -> config symbol (e.g. "BTC_USDT")
    id_to_config: HashMap<u32, String>,
    /// config symbol -> product_id
    config_to_id: HashMap<String, u32>,
    itype: InstrumentType,
}

impl NadoFeed {
    async fn new_perp(normalized_symbols: &[&str]) -> Result<Self> {
        let client = Client::new();
        let rows = fetch_pairs(&client).await?;
        let itype = InstrumentType::Perp;
        let mapper = NadoMapper;

        // base (e.g. "BTC-PERP") -> product_id
        let api_map: HashMap<String, u32> = rows
            .into_iter()
            .map(|r| (r.base, r.product_id))
            .collect();

        let mut id_to_config = HashMap::new();
        let mut config_to_id = HashMap::new();

        for &sym in normalized_symbols {
            let native_base = mapper.denormalize(sym, itype)?;
            let pid = *api_map
                .get(&native_base)
                .ok_or_else(|| anyhow!("Nado symbol '{}' not found in pairs", native_base))?;
            id_to_config.insert(pid, sym.to_string());
            config_to_id.insert(sym.to_string(), pid);
        }

        Ok(Self {
            id_to_config,
            config_to_id,
            itype,
        })
    }
}

#[derive(Debug, Deserialize)]
struct BboEvent {
    #[serde(default)]
    r#type: Option<String>,
    #[serde(default)]
    product_id: Option<u32>,
    #[serde(default)]
    bid_price: Option<String>,
    #[serde(default)]
    bid_qty: Option<String>,
    #[serde(default)]
    ask_price: Option<String>,
    #[serde(default)]
    ask_qty: Option<String>,
    #[serde(default)]
    timestamp: Option<String>,
}

fn parse_x18(s: &str) -> Option<f64> {
    s.parse::<f64>().ok().map(|v| v / X18)
}

#[async_trait::async_trait]
impl ExchangeFeed for NadoFeed {
    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }

    fn extra_headers(&self) -> Vec<(&str, &str)> {
        vec![
            ("Origin", "https://app.nado.xyz"),
            ("User-Agent", "Mozilla/5.0"),
        ]
    }

    fn build_url(&self, _symbols: &[&str]) -> Result<String> {
        Ok("wss://gateway.prod.nado.xyz/v1/subscribe".to_string())
    }

    fn heartbeat_message(&self) -> Option<Message> {
        Some(Message::Ping(vec![].into()))
    }

    async fn send_subscription(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbols: &[&str],
    ) -> Result<()> {
        for (i, sym) in symbols.iter().enumerate() {
            let pid = self.config_to_id.get(*sym)
                .ok_or_else(|| anyhow!("No product_id for {}", sym))?;

            let msg = json!({
                "method": "subscribe",
                "stream": {
                    "type": "best_bid_offer",
                    "product_id": pid,
                },
                "id": i + 1,
            });
            debug!("Nado sub: {}", msg);
            write
                .send(Message::Text(msg.to_string().into()))
                .await
                .with_context(|| format!("failed to subscribe to Nado product {}", pid))?;
        }
        Ok(())
    }

    fn parse_message(
        &self,
        msg: WireMessage<'_>,
        received_ts: DateTime<Utc>,
    ) -> Result<Option<(String, MarketData)>> {
        let WireMessage::Text(text) = msg else {
            return Ok(None);
        };

        // Skip subscription acks and non-BBO messages
        if !text.contains("best_bid_offer") {
            return Ok(None);
        }

        let evt: BboEvent = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };

        if evt.r#type.as_deref() != Some("best_bid_offer") {
            return Ok(None);
        }

        let pid = match evt.product_id {
            Some(id) => id,
            None => return Ok(None),
        };

        let config_sym = match self.id_to_config.get(&pid) {
            Some(s) => s.as_str(),
            None => return Ok(None),
        };

        let bid = evt.bid_price.as_deref().and_then(parse_x18);
        let ask = evt.ask_price.as_deref().and_then(parse_x18);
        let bid_qty = evt.bid_qty.as_deref().and_then(parse_x18);
        let ask_qty = evt.ask_qty.as_deref().and_then(parse_x18);

        let (Some(b), Some(a)) = (bid, ask) else {
            return Ok(None);
        };

        if b <= 0.0 || a <= 0.0 || b >= a {
            return Ok(None);
        }

        let exchange_ts = evt.timestamp.as_deref()
            .and_then(|s| s.parse::<i64>().ok())
            .and_then(|ns| DateTime::from_timestamp(ns / 1_000_000_000, (ns % 1_000_000_000) as u32));

        let md = MarketData {
            bid,
            ask,
            bid_qty,
            ask_qty,
            exchange_ts,
            received_ts: Some(received_ts),
        };

        Ok(Some((config_sym.to_string(), md)))
    }
}

pub async fn listen_perp_bbo(
    data: Arc<MarketDataCollection>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let feed = Arc::new(NadoFeed::new_perp(symbols).await?);

    let mut config = ConnectionConfig::default();
    // Nado requires ping every 30s
    config.heartbeat_interval = std::time::Duration::from_secs(15);

    listen_with_reconnect(data, symbols, feed, "nado_perp", config, shutdown).await
}
