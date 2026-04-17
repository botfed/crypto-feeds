use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use log::{debug, error, info, warn};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::interval;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config, tungstenite::Message, tungstenite::client::IntoClientRequest, tungstenite::http};

use crate::market_data::InstrumentType;
use crate::symbol_registry::{REGISTRY, SymbolId};
use crate::{MarketDataCollection, market_data::MarketData};
use std::collections::HashMap;

#[derive(Clone)]
pub struct ConnectionConfig {
    pub max_retry_delay: Duration,
    pub heartbeat_interval: Duration,
    pub message_timeout: Duration,
    pub initial_backoff: Duration,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            max_retry_delay: Duration::from_secs(60),
            heartbeat_interval: Duration::from_secs(10),
            message_timeout: Duration::from_secs(90),
            initial_backoff: Duration::from_secs(1),
        }
    }
}

pub enum ConnectionResult {
    Shutdown,
    Reconnect,
    InvalidConfig,
}

pub fn calculate_backoff(retry_count: u32, initial: Duration, max: Duration) -> Duration {
    let exponential = initial * 2_u32.saturating_pow(retry_count.min(10));
    std::cmp::min(exponential, max)
}

pub enum WireMessage<'a> {
    Text(&'a str),
    Binary(&'a [u8]),
}

#[async_trait]
pub trait ExchangeFeed {

    fn get_itype(&self) -> Result<&InstrumentType>;

    async fn send_subscription(
        &self,
        _write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        _symbols: &[&str],
    ) -> Result<()> {
        Ok(())
    }
    async fn process_other(
        &self,
        _write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        _text: &str,
    ) -> Result<()> {
        Ok(())
    }

    fn heartbeat_message(&self) -> Option<Message> {
        return None;
    }

    /// Whether the generic connection loop should drop messages with
    /// exchange_ts < last seen exchange_ts for that symbol.
    /// Override to `false` for incremental depth feeds.
    fn timestamp_dedup(&self) -> bool {
        true
    }

    /// Optional extra HTTP headers added to the WebSocket upgrade request.
    fn extra_headers(&self) -> Vec<(&str, &str)> {
        vec![]
    }

    /// Optional WebSocket config (e.g. to enable permessage-deflate).
    fn ws_config(&self) -> Option<tokio_tungstenite::tungstenite::protocol::WebSocketConfig> {
        None
    }

    fn build_url(&self, symbols: &[&str]) -> Result<String>;

    /// Return:
    /// - Ok(Some((symbol, MarketData))) for a usable update
    /// - Ok(None) to ignore the message (heartbeat, sub ack, etc.)
    /// - Err(_) for parse/decode failures you want logged
    fn parse_message(
        &self,
        msg: WireMessage<'_>,
        received_ts: chrono::DateTime<Utc>,
        received_instant: std::time::Instant,
    ) -> Result<Option<(String, MarketData)>>;
}

pub async fn listen_with_reconnect<F: ExchangeFeed + Send + Sync>(
    data: Arc<MarketDataCollection>,
    symbols: &[&str],
    feed: Arc<F>,
    feed_name: &str,
    config: ConnectionConfig,
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let mut retry_count: u32 = 0;

    loop {
        debug!("Connecting feed {} attempt {}", feed_name, retry_count + 1);
        let attempt_start = std::time::Instant::now();

        tokio::select! {
            _ = shutdown.notified() => {
                info!("Shutdown received for feed {}", feed_name);
                break;
            }

            res = connect_and_stream(&data, &feed, feed_name, symbols, &config) => {
                // Reset backoff only if the connection was stable for >60s
                let was_long_lived = attempt_start.elapsed() > Duration::from_secs(60);

                match res {
                    Ok(ConnectionResult::Shutdown | ConnectionResult::InvalidConfig) => break,

                    Ok(ConnectionResult::Reconnect) => {
                        if was_long_lived {
                            retry_count = 0;
                        } else {
                            retry_count += 1;
                        }

                        let backoff = calculate_backoff(
                            retry_count,
                            config.initial_backoff,
                            config.max_retry_delay
                        );

                        warn!("{} disconnected. Reconnecting in {:?}", feed_name, backoff);

                        tokio::select! {
                            _ = tokio::time::sleep(backoff) => {}
                            _ = shutdown.notified() => {
                                info!("Shutdown during backoff for {}", feed_name);
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
                            config.max_retry_delay
                        );

                        error!("{} error: {}. Reconnecting in {:?}", feed_name, e, backoff);

                        tokio::select! {
                            _ = tokio::time::sleep(backoff) => {}
                            _ = shutdown.notified() => {
                                info!("Shutdown during backoff for {}", feed_name);
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    info!("Stopped {}", feed_name);
    Ok(())
}

async fn connect_and_stream<F: ExchangeFeed + Sync + Send>(
    data: &Arc<MarketDataCollection>,
    feed: &Arc<F>,
    feed_name: &str,
    symbols: &[&str],
    config: &ConnectionConfig,
) -> Result<ConnectionResult> {
    let itype = feed.get_itype()?;
    let url = match feed.build_url(symbols) {
        Ok(v) => v,
        Err(e) => {
            error!("Could not build connection url {}: {}", feed_name, e);
            return Ok(ConnectionResult::InvalidConfig);
        }
    };

    let mut request = url.into_client_request()?;
    for (key, value) in feed.extra_headers() {
        request.headers_mut().insert(
            http::header::HeaderName::from_bytes(key.as_bytes())?,
            http::header::HeaderValue::from_str(value)?,
        );
    }

    let ws_config = feed.ws_config();
    let connect_fut = if let Some(ws_cfg) = ws_config {
        connect_async_with_config(request, Some(ws_cfg), false)
    } else {
        connect_async_with_config(request, None, false)
    };

    let ws_stream = match tokio::time::timeout(
        Duration::from_secs(config.message_timeout.as_secs()),
        connect_fut,
    )
    .await
    {
        Ok(Ok((stream, _))) => stream,
        Ok(Err(e)) => {
            error!("Failed to connect to {} {}", feed_name, e);
            return Ok(ConnectionResult::Reconnect);
        }
        Err(e) => {
            error!("Connect timed out for {} {}", feed_name, e);
            return Ok(ConnectionResult::Reconnect);
        }
    };

    info!("Connected to {}", feed_name);

    let (mut write, mut read) = ws_stream.split();

    if let Err(e) = feed.send_subscription(&mut write, symbols).await {
        error!("Error subscribing to {}: {}", feed_name, e);
        close_stream(write, read, feed_name).await;
        return Ok(ConnectionResult::Reconnect);
    }

    let mut heartbeat = interval(config.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut last_message_time = Utc::now();
    let do_ts_dedup = feed.timestamp_dedup();
    let mut last_exchange_ts: HashMap<SymbolId, chrono::DateTime<Utc>> = HashMap::new();

    let result = loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                let elapsed = Utc::now() - last_message_time;
                if elapsed > chrono::Duration::from_std(config.message_timeout)? {
                    warn!("No messages for {:?} on {}, reconnecting", config.message_timeout, feed_name);
                    break ConnectionResult::Reconnect;
                }

                if let Some(msg) = feed.heartbeat_message() {
                    if let Err(e) = write.send(msg).await {
                        error!("Failed heartbeat on {}: {}", feed_name, e);
                        break ConnectionResult::Reconnect;
                    }
                } else {
                // Keepalive ping (many servers ignore it; some require it)
                if let Err(e) = write.send(Message::Ping(vec![].into())).await {
                    error!("Failed to send ping on {}: {}", feed_name, e);
                    break ConnectionResult::Reconnect;
                }
                }

            }

            msg = read.next() => {
                let received_instant = std::time::Instant::now();
                let received_ts = Utc::now();
                last_message_time = received_ts;

                match msg {
                    Some(Ok(Message::Text(text))) => {
                        match feed.parse_message(WireMessage::Text(text.as_str()), received_ts, received_instant) {
                            Ok(Some((sym, mut md))) => {
                                if let Some(&id) = REGISTRY.lookup(&sym, &itype) {
                                    let stale = do_ts_dedup && md.exchange_ts_raw.map_or(false, |ts| {
                                        last_exchange_ts.get(&id).map_or(false, |&last| ts < last)
                                    });
                                    if !stale {
                                        if do_ts_dedup {
                                            if let Some(ts) = md.exchange_ts_raw {
                                                last_exchange_ts.insert(id, ts);
                                            }
                                        }
                                        md.feed_latency_ns = received_instant.elapsed().as_nanos() as u64;
                                        data.push(&id, md);
                                    }
                                }
                            }
                            Ok(None) => {
                                // intentionally ignored (heartbeats, sub acks, etc.)
                                if let Err(e) = feed.process_other(&mut write, &text).await {
                                    error!("Error processing other: {}", e);
                                    break ConnectionResult::Reconnect;
                                }
                            }
                            Err(e) => {
                                let preview = if text.len() > 120 { &text[..120] } else { &text };
                                error!("{} parse error: {}  {}", feed_name, preview, e);
                            }
                        }
                    }

                    Some(Ok(Message::Binary(bytes))) => {
                        match feed.parse_message(WireMessage::Binary(&bytes), received_ts, received_instant) {
                            Ok(Some((sym, mut md))) => {
                                if let Some(&id) = REGISTRY.lookup(&sym, &itype) {
                                    let stale = do_ts_dedup && md.exchange_ts_raw.map_or(false, |ts| {
                                        last_exchange_ts.get(&id).map_or(false, |&last| ts < last)
                                    });
                                    if !stale {
                                        if do_ts_dedup {
                                            if let Some(ts) = md.exchange_ts_raw {
                                                last_exchange_ts.insert(id, ts);
                                            }
                                        }
                                        md.feed_latency_ns = received_instant.elapsed().as_nanos() as u64;
                                        data.push(&id, md);
                                    }
                                }
                            }
                            Ok(None) => {
                                // intentionally ignored
                            }
                            Err(e) => {
                                error!("{} parse error (binary): {}", feed_name, e);
                            }
                        }
                    }

                    Some(Ok(Message::Ping(payload))) => {
                        // Respond to server ping (helps with some exchanges)
                        let _ = write.send(Message::Pong(payload)).await;
                    }

                    Some(Ok(Message::Pong(_))) => {
                        // keepalive response; nothing to do
                    }

                    Some(Ok(Message::Close(_))) => {
                        warn!("{} socket closed.", feed_name);
                        break ConnectionResult::Reconnect;
                    }

                    Some(Err(e)) => {
                        error!("{} socket error: {}", feed_name, e);
                        break ConnectionResult::Reconnect;
                    }

                    None => {
                        warn!("{} stream ended.", feed_name);
                        break ConnectionResult::Reconnect;
                    }

                    _ => {}
                }
            }
        }
    };

    close_stream(write, read, feed_name).await;
    Ok(result)
}

/// Shut down the WebSocket connection without blocking async worker threads.
///
/// Dropping a TLS WebSocket stream calls `SSLClose()` (native-tls / SecureTransport
/// on macOS) which attempts a TLS shutdown handshake.  On a dead socket this blocks
/// until the OS timeout fires — starving the tokio thread pool if multiple feeds
/// disconnect at once.  We move the entire drop sequence into `spawn_blocking` so
/// it can never block async tasks.
async fn close_stream(
    write: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    read: futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    feed_name: &str,
) {
    let name = feed_name.to_string();
    let _ = tokio::task::spawn_blocking(move || {
        // Skip sending a Close frame — the connection is already dead or
        // about to be replaced. Just drop both halves; the TLS/TCP cleanup
        // happens here, off the async runtime.
        drop(read);
        drop(write);
        debug!("Dropped stream for {}", name);
    })
    .await;
}
