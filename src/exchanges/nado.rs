use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use nado_ws::Message;
use reqwest::Client;
use reqwest::header::ACCEPT_ENCODING;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::exchange_fees::{ExchangeFees, FeeSchedule};
use crate::exchanges::connection::{ConnectionConfig, calculate_backoff};
use crate::mappers::{NadoMapper, SymbolMapper};
use crate::market_data::{InstrumentType, MarketData, MarketDataCollection};
use crate::symbol_registry::REGISTRY;

const X18: f64 = 1e18;
const FEED_NAME: &str = "nado_perp";
const WS_URL: &str = "wss://gateway.prod.nado.xyz/v1/subscribe";

pub fn get_fees() -> ExchangeFees {
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
    id_to_config: HashMap<u32, String>,
    config_to_id: HashMap<String, u32>,
    itype: InstrumentType,
}

impl NadoFeed {
    async fn new_perp(normalized_symbols: &[&str]) -> Result<Self> {
        let client = Client::new();
        let rows = fetch_pairs(&client).await?;
        let itype = InstrumentType::Perp;
        let mapper = NadoMapper;

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
        exchange_ts_raw: exchange_ts,
        exchange_ts: None,
        received_ts: Some(received_ts),
    };

    Some((config_sym.to_string(), md))
}

async fn connect_and_stream(
    data: &Arc<MarketDataCollection>,
    feed: &NadoFeed,
    symbols: &[&str],
    config: &ConnectionConfig,
) -> Result<bool> {
    let ws_stream = match tokio::time::timeout(
        config.message_timeout,
        nado_ws::connect(WS_URL),
    )
    .await
    {
        Ok(Ok(s)) => s,
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

    let (mut write, mut read) = ws_stream.split();

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
        if let Err(e) = write
            .send(Message::Text(msg.to_string().into()))
            .await
        {
            error!("Failed to subscribe to Nado product {}: {}", pid, e);
            close_nado(write, read).await;
            return Ok(false);
        }
    }

    let mut heartbeat = tokio::time::interval(config.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_message_time = Utc::now();
    let mut last_exchange_ts: HashMap<crate::symbol_registry::SymbolId, DateTime<Utc>> = HashMap::new();

    let result = loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                let elapsed = Utc::now() - last_message_time;
                if elapsed > chrono::Duration::from_std(config.message_timeout)? {
                    warn!("No messages for {:?} on {}, reconnecting", config.message_timeout, FEED_NAME);
                    break false;
                }
                if let Err(e) = write.send(Message::Ping(vec![].into())).await {
                    error!("Failed heartbeat on {}: {}", FEED_NAME, e);
                    break false;
                }
            }

            msg = read.next() => {
                let received_ts = Utc::now();
                last_message_time = received_ts;

                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Some((sym, md)) = parse_bbo(feed, text.as_str(), received_ts) {
                            if let Some(&id) = REGISTRY.lookup(&sym, &feed.itype) {
                                let stale = md.exchange_ts_raw.map_or(false, |ts| {
                                    last_exchange_ts.get(&id).map_or(false, |&last| ts < last)
                                });
                                if !stale {
                                    if let Some(ts) = md.exchange_ts_raw {
                                        last_exchange_ts.insert(id, ts);
                                    }
                                    data.push(&id, md);
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Binary(_))) => {}
                    Some(Ok(Message::Ping(payload))) => {
                        let _ = write.send(Message::Pong(payload)).await;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) => {
                        warn!("{} socket closed.", FEED_NAME);
                        break false;
                    }
                    Some(Err(e)) => {
                        error!("{} socket error: {}", FEED_NAME, e);
                        break false;
                    }
                    None => {
                        warn!("{} stream ended.", FEED_NAME);
                        break false;
                    }
                    _ => {}
                }
            }
        }
    };

    close_nado(write, read).await;
    Ok(result)
}

async fn close_nado(
    write: futures_util::stream::SplitSink<nado_ws::WsStream, Message>,
    read: futures_util::stream::SplitStream<nado_ws::WsStream>,
) {
    // Move the drop into spawn_blocking so TLS shutdown on a dead socket
    // cannot block async worker threads (same rationale as close_stream).
    let _ = tokio::task::spawn_blocking(move || {
        drop(read);
        drop(write);
    })
    .await;
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

    loop {
        debug!("Connecting feed {} attempt {}", FEED_NAME, retry_count + 1);
        let attempt_start = std::time::Instant::now();

        tokio::select! {
            _ = shutdown.notified() => {
                info!("Shutdown received for feed {}", FEED_NAME);
                break;
            }

            res = connect_and_stream(&data, &feed, symbols, &config) => {
                // Reset backoff only if the connection was stable for >60s
                let was_long_lived = attempt_start.elapsed() > Duration::from_secs(60);

                match res {
                    Ok(true) => break,
                    Ok(false) => {
                        if was_long_lived {
                            retry_count = 0;
                        } else {
                            retry_count += 1;
                        }

                        let backoff = calculate_backoff(
                            retry_count,
                            config.initial_backoff,
                            config.max_retry_delay,
                        );
                        error!("{} disconnected. Reconnecting in {:?}", FEED_NAME, backoff);

                        tokio::select! {
                            _ = tokio::time::sleep(backoff) => {}
                            _ = shutdown.notified() => {
                                info!("Shutdown during backoff for {}", FEED_NAME);
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        if was_long_lived {
                            retry_count = 0;
                        } else {
                            retry_count += 1;
                        }

                        let backoff = calculate_backoff(
                            retry_count,
                            config.initial_backoff,
                            config.max_retry_delay,
                        );
                        error!("{} error: {}. Reconnecting in {:?}", FEED_NAME, e, backoff);

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
    }

    info!("Stopped {}", FEED_NAME);
    Ok(())
}
