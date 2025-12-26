use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use log::{debug, error, info, warn};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::interval;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::market_data::InstrumentType;
use crate::symbol_registry::REGISTRY;
use crate::{MarketDataCollection, market_data::MarketData};

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

    fn build_url(&self, symbols: &[&str]) -> Result<String>;

    /// Return:
    /// - Ok(Some((symbol, MarketData))) for a usable update
    /// - Ok(None) to ignore the message (heartbeat, sub ack, etc.)
    /// - Err(_) for parse/decode failures you want logged
    fn parse_message(
        &self,
        msg: WireMessage<'_>,
        received_ts: chrono::DateTime<Utc>,
    ) -> Result<Option<(String, MarketData)>>;
}

pub async fn listen_with_reconnect<F: ExchangeFeed + Send + Sync>(
    data: Arc<Mutex<MarketDataCollection>>,
    symbols: &[&str],
    feed: Arc<F>,
    feed_name: &str,
    config: ConnectionConfig,
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let mut retry_count: u32 = 0;
    let mut last_success = Utc::now();

    loop {
        debug!("Connecting feed {} attempt {}", feed_name, retry_count + 1);

        tokio::select! {
            _ = shutdown.notified() => {
                info!("Shutdown received for feed {}", feed_name);
                break;
            }

            res = connect_and_stream(&data, &feed, feed_name, symbols, &config) => match res {
                Ok(ConnectionResult::Shutdown | ConnectionResult::InvalidConfig) => break,

                Ok(ConnectionResult::Reconnect) => {
                    // Successful run that ended in a reconnect condition:
                    retry_count = 0;
                    last_success = Utc::now();
                }

                Err(e) => {
                    retry_count += 1;

                    let backoff = calculate_backoff(
                        retry_count,
                        config.initial_backoff,
                        config.max_retry_delay
                    );

                    error!("{} error: {}. Reconnecting in {:?}", feed_name, e, backoff);

                    // Reset retry count after a successful long run
                    if Utc::now() - last_success > chrono::Duration::seconds(300) {
                        retry_count = 0;
                    }

                    // Sleep with ability to interrupt on shutdown
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

    info!("Stopped {}", feed_name);
    Ok(())
}

async fn connect_and_stream<F: ExchangeFeed + Sync + Send>(
    data: &Arc<Mutex<MarketDataCollection>>,
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

    let ws_stream = match tokio::time::timeout(
        Duration::from_secs(config.message_timeout.as_secs()),
        connect_async(url),
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
        return Ok(ConnectionResult::Reconnect);
    }

    let mut heartbeat = interval(config.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut last_message_time = Utc::now();

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                let elapsed = Utc::now() - last_message_time;
                if elapsed > chrono::Duration::from_std(config.message_timeout)? {
                    warn!("No messages for {:?} on {}, reconnecting", config.message_timeout, feed_name);
                    return Ok(ConnectionResult::Reconnect);
                }

                if let Some(msg) = feed.heartbeat_message() {
                    if let Err(e) = write.send(msg).await {
                        error!("Failed heartbeat on {}: {}", feed_name, e);
                        return Ok(ConnectionResult::Reconnect);
                    }
                } else {
                // Keepalive ping (many servers ignore it; some require it)
                if let Err(e) = write.send(Message::Ping(vec![].into())).await {
                    error!("Failed to send ping on {}: {}", feed_name, e);
                    return Ok(ConnectionResult::Reconnect);
                }
                }

            }

            msg = read.next() => {
                let received_ts = Utc::now();
                last_message_time = received_ts;

                match msg {
                    Some(Ok(Message::Text(text))) => {
                        match feed.parse_message(WireMessage::Text(text.as_str()), received_ts) {
                            Ok(Some((sym, md))) => {
                                let mut collection = data.lock().unwrap();
                                if let Some(&id) = REGISTRY.lookup(&sym, &itype) {
                                    collection.data[id] = Some(md);
                                }
                            }
                            Ok(None) => {
                                // intentionally ignored (heartbeats, sub acks, etc.)
                                if let Err(e) = feed.process_other(&mut write, &text).await {
                                    error!("Error processing other{}: {}", text, e);
                                    return Ok(ConnectionResult::Reconnect);
                                }
                            }
                            Err(e) => {
                                error!("{} parse error (text): {}  {}", feed_name, &text, e);
                            }
                        }
                    }

                    Some(Ok(Message::Binary(bytes))) => {
                        match feed.parse_message(WireMessage::Binary(&bytes), received_ts) {
                            Ok(Some((sym, md))) => {
                                let mut collection = data.lock().unwrap();
                                if let Some(id) = REGISTRY.lookup(&sym, &itype) {
                                    collection.data[*id] = Some(md);
                                };
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
                        return Ok(ConnectionResult::Reconnect);
                    }

                    Some(Err(e)) => {
                        error!("{} socket error: {}", feed_name, e);
                        return Ok(ConnectionResult::Reconnect);
                    }

                    None => {
                        warn!("{} stream ended.", feed_name);
                        return Ok(ConnectionResult::Reconnect);
                    }

                    _ => {}
                }
            }
        }
    }
}
