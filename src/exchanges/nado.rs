use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use log::{debug, error, info, warn};
use ratchet_deflate::DeflateExtProvider;
use ratchet_rs::{Message, WebSocketConfig};
use reqwest::Client;
use reqwest::header::ACCEPT_ENCODING;
use serde::Deserialize;
use serde_json::json;
use bytes::BytesMut;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;

use crate::exchange_fees::{ExchangeFees, FeeSchedule};
use crate::exchanges::connection::{ConnectionConfig, calculate_backoff};
use crate::mappers::{NadoMapper, SymbolMapper};
use crate::market_data::{InstrumentType, MarketData, MarketDataCollection};
use crate::symbol_registry::REGISTRY;

const X18: f64 = 1e18;
const FEED_NAME: &str = "nado_perp";
const WS_URL: &str = "wss://gateway.prod.nado.xyz/v1/subscribe";

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

fn parse_bbo(feed: &NadoFeed, text: &str, received_ts: DateTime<Utc>) -> Option<(String, MarketData)> {
    if !text.contains("best_bid_offer") {
        return None;
    }

    let evt: BboEvent = serde_json::from_str(text).ok()?;

    if evt.r#type.as_deref() != Some("best_bid_offer") {
        return None;
    }

    let pid = evt.product_id?;
    let config_sym = feed.id_to_config.get(&pid)?;

    let bid = evt.bid_price.as_deref().and_then(parse_x18);
    let ask = evt.ask_price.as_deref().and_then(parse_x18);
    let bid_qty = evt.bid_qty.as_deref().and_then(parse_x18);
    let ask_qty = evt.ask_qty.as_deref().and_then(parse_x18);

    let (b, a) = (bid?, ask?);
    if b <= 0.0 || a <= 0.0 || b >= a {
        return None;
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

    Some((config_sym.to_string(), md))
}

/// Connect via ratchet_rs with permessage-deflate support.
async fn connect_ratchet(
) -> Result<ratchet_rs::WebSocket<tokio_native_tls::TlsStream<TcpStream>, ratchet_deflate::Deflate>>
{
    let host = "gateway.prod.nado.xyz";
    let port = 443u16;

    let tcp = TcpStream::connect((host, port))
        .await
        .context("TCP connect")?;

    let tls_cx = tokio_native_tls::native_tls::TlsConnector::new().context("TLS connector")?;
    let tls_cx = tokio_native_tls::TlsConnector::from(tls_cx);
    let tls_stream = tls_cx
        .connect(host, tcp)
        .await
        .context("TLS handshake")?;

    let request = http::Request::builder()
        .uri(WS_URL)
        .header("Host", host)
        .header("Origin", "https://app.nado.xyz")
        .header("User-Agent", "Mozilla/5.0")
        .body(())
        .context("build HTTP request")?;

    let config = WebSocketConfig::default();
    let deflate = DeflateExtProvider::default();
    let subprotos = ratchet_rs::SubprotocolRegistry::default();

    let upgraded = ratchet_rs::subscribe_with(config, tls_stream, request, deflate, subprotos)
        .await
        .map_err(|e| anyhow!("ratchet subscribe: {e}"))?;

    Ok(upgraded.into_websocket())
}

async fn connect_and_stream(
    data: &Arc<MarketDataCollection>,
    feed: &NadoFeed,
    symbols: &[&str],
    config: &ConnectionConfig,
) -> Result<bool> {
    let upgraded = match tokio::time::timeout(
        config.message_timeout,
        connect_ratchet(),
    )
    .await
    {
        Ok(Ok(u)) => u,
        Ok(Err(e)) => {
            error!("Failed to connect to {}: {}", FEED_NAME, e);
            return Ok(false);
        }
        Err(_) => {
            error!("Connect timed out for {}", FEED_NAME);
            return Ok(false);
        }
    };

    info!("Connected to {}", FEED_NAME);

    let mut ws = upgraded;

    // Send subscriptions
    for (i, sym) in symbols.iter().enumerate() {
        let pid = feed.config_to_id.get(*sym)
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
        ws.write_text(msg.to_string())
            .await
            .map_err(|e| anyhow!("subscribe send: {e}"))?;
    }

    let mut heartbeat = tokio::time::interval(config.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_message_time = Utc::now();
    let mut buf = BytesMut::new();

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                let elapsed = Utc::now() - last_message_time;
                if elapsed > chrono::Duration::from_std(config.message_timeout)? {
                    warn!("No messages for {:?} on {}, reconnecting", config.message_timeout, FEED_NAME);
                    return Ok(false);
                }
                if let Err(e) = ws.write_ping(&[]).await {
                    error!("Failed heartbeat on {}: {}", FEED_NAME, e);
                    return Ok(false);
                }
            }

            msg = ws.read(&mut buf) => {
                let received_ts = Utc::now();
                last_message_time = received_ts;

                match msg {
                    Ok(Message::Text) => {
                        let text = std::str::from_utf8(&buf).unwrap_or("");
                        if let Some((sym, md)) = parse_bbo(feed, text, received_ts) {
                            if let Some(&id) = REGISTRY.lookup(&sym, &feed.itype) {
                                data.push(&id, md);
                            }
                        }
                        buf.clear();
                    }
                    Ok(Message::Binary) => {
                        buf.clear();
                    }
                    Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {
                        buf.clear();
                    }
                    Ok(Message::Close(_)) => {
                        warn!("{} socket closed.", FEED_NAME);
                        return Ok(false);
                    }
                    Err(e) => {
                        error!("{} socket error: {}", FEED_NAME, e);
                        return Ok(false);
                    }
                }
            }
        }
    }
}

pub async fn listen_perp_bbo(
    data: Arc<MarketDataCollection>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let feed = NadoFeed::new_perp(symbols).await?;

    let mut config = ConnectionConfig::default();
    config.heartbeat_interval = Duration::from_secs(15);

    let mut retry_count: u32 = 0;
    let last_success = Utc::now();

    loop {
        debug!("Connecting feed {} attempt {}", FEED_NAME, retry_count + 1);

        tokio::select! {
            _ = shutdown.notified() => {
                info!("Shutdown received for feed {}", FEED_NAME);
                break;
            }

            res = connect_and_stream(&data, &feed, symbols, &config) => match res {
                Ok(true) => break, // clean shutdown
                Ok(false) => {
                    retry_count += 1;
                    let backoff = calculate_backoff(
                        retry_count,
                        config.initial_backoff,
                        config.max_retry_delay,
                    );
                    error!("{} disconnected. Reconnecting in {:?}", FEED_NAME, backoff);

                    if Utc::now() - last_success > chrono::Duration::seconds(300) {
                        retry_count = 0;
                    }

                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        _ = shutdown.notified() => {
                            info!("Shutdown during backoff for {}", FEED_NAME);
                            break;
                        }
                    }
                }
                Err(e) => {
                    retry_count += 1;
                    let backoff = calculate_backoff(
                        retry_count,
                        config.initial_backoff,
                        config.max_retry_delay,
                    );
                    error!("{} error: {}. Reconnecting in {:?}", FEED_NAME, e, backoff);

                    if Utc::now() - last_success > chrono::Duration::seconds(300) {
                        retry_count = 0;
                    }

                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        _ = shutdown.notified() => {
                            info!("Shutdown during backoff for {}", FEED_NAME);
                            break;
                        }
                    }
                }
            }
        }
    }

    info!("Stopped {}", FEED_NAME);
    Ok(())
}
