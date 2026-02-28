use anyhow::Result;
use chrono::{DateTime, Utc};
use futures_util::SinkExt;
use futures_util::stream::SplitSink;
use log::{debug, warn};
use serde::Deserialize;
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

use crate::exchange_fees::{ExchangeFees, FeeSchedule};
use crate::exchanges::connection::{
    ConnectionConfig, ExchangeFeed, WireMessage, listen_with_reconnect,
};
use crate::mappers::{CoinbaseMapper, SymbolMapper};
use crate::market_data::{InstrumentType, MarketData, MarketDataCollection};

pub fn get_fees() -> ExchangeFees {
    ExchangeFees::new(FeeSchedule::new(60.0, 40.0), FeeSchedule::new(60.0, 40.0))
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum CoinbaseMessage {
    #[serde(rename = "ticker")]
    Ticker(CoinbaseTicker),
    #[serde(rename = "subscriptions")]
    Subscriptions(serde_json::Value),
    #[serde(rename = "heartbeat")]
    Heartbeat(serde_json::Value),
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
struct CoinbaseTicker {
    product_id: String,
    best_bid: String,
    best_ask: String,
    best_bid_size: String,
    best_ask_size: String,
    time: String,
    sequence: u64,
}

#[derive(Clone)]
struct CoinbaseFeed {
    url: &'static str,
    itype: InstrumentType,
    mapper: CoinbaseMapper,
}

impl CoinbaseFeed {
    fn new_spot() -> Self {
        // Keep your existing URL; if Coinbase changes domains, update here.
        Self {
            url: "wss://ws-feed.exchange.coinbase.com",
            itype: InstrumentType::Spot,
            mapper: CoinbaseMapper,
        }
    }
}

#[async_trait::async_trait]
impl ExchangeFeed for CoinbaseFeed {

    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }
    fn build_url(&self, _symbols: &[&str]) -> Result<String> {
        // Coinbase uses a fixed URL; subscription carries symbols.
        Ok(self.url.to_string())
    }

    async fn send_subscription(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbols: &[&str],
    ) -> Result<()> {
        let pairs: Vec<String> = symbols
            .iter()
            .map(|s| self.mapper.denormalize(s, self.itype))
            .collect::<Result<Vec<_>, _>>()?;
        let subscribe_msg = json!({
            "type": "subscribe",
            "product_ids": pairs,
            "channels": ["ticker"]
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
        // Coinbase sends multiple message types; we parse them all and filter later.
        match msg {
            WireMessage::Text(text) => {
                let msg = serde_json::from_str::<CoinbaseMessage>(&text)?;
                match msg {
                    CoinbaseMessage::Ticker(ticker) => {
                        let bid = ticker.best_bid.parse::<f64>().ok();
                        let ask = ticker.best_ask.parse::<f64>().ok();
                        let bid_qty = ticker.best_bid_size.parse::<f64>().ok();
                        let ask_qty = ticker.best_ask_size.parse::<f64>().ok();

                        // Optional sanity check
                        if let (Some(b), Some(a)) = (bid, ask) {
                            if b >= a {
                                warn!(
                                    "Invalid Coinbase quote for {}: bid={} >= ask={}",
                                    ticker.product_id, b, a
                                );
                                return Ok(None);
                            }
                        }

                        let exchange_ts = DateTime::parse_from_rfc3339(&ticker.time)
                            .ok()
                            .map(|dt| dt.with_timezone(&Utc));

                        let market_data = MarketData {
                            bid,
                            ask,
                            bid_qty,
                            ask_qty,
                            exchange_ts,
                            received_ts: Some(received_ts),
                        };

                        return Ok(Some((ticker.product_id, market_data)));
                    }
                    CoinbaseMessage::Heartbeat(beat) => {
                        debug!("Got heartbeat {}", beat);
                        return Ok(None);
                    }
                    CoinbaseMessage::Subscriptions(_) => {
                        return Ok(None);
                    }
                    CoinbaseMessage::Other => {
                        return Ok(None);
                    }
                }
            }
            WireMessage::Binary(_) => Ok(None),
        }
    }
}

pub async fn listen_spot_bbo(
    data: Arc<Mutex<MarketDataCollection>>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let feed = Arc::new(CoinbaseFeed::new_spot());
    listen_with_reconnect(
        data,
        symbols,
        feed,
        "coinbase_spot",
        ConnectionConfig::default(),
        shutdown,
    )
    .await
}

// --- Coinbase Advanced Trade WebSocket (perps) ---

#[derive(Debug, Deserialize)]
struct AdvancedTradeMessage {
    channel: String,
    timestamp: String,
    #[serde(default)]
    events: Vec<AdvancedTradeEvent>,
}

#[derive(Debug, Deserialize)]
struct AdvancedTradeEvent {
    #[serde(default)]
    tickers: Vec<AdvancedTradeTicker>,
}

#[derive(Debug, Deserialize)]
struct AdvancedTradeTicker {
    product_id: String,
    best_bid: String,
    best_bid_quantity: String,
    best_ask: String,
    best_ask_quantity: String,
}

#[derive(Clone)]
struct CoinbaseAdvancedFeed {
    itype: InstrumentType,
    mapper: CoinbaseMapper,
}

impl CoinbaseAdvancedFeed {
    fn new_perp() -> Self {
        Self {
            itype: InstrumentType::Perp,
            mapper: CoinbaseMapper,
        }
    }
}

#[async_trait::async_trait]
impl ExchangeFeed for CoinbaseAdvancedFeed {
    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }

    fn build_url(&self, _symbols: &[&str]) -> Result<String> {
        Ok("wss://advanced-trade-ws.coinbase.com".to_string())
    }

    async fn send_subscription(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbols: &[&str],
    ) -> Result<()> {
        let product_ids: Vec<String> = symbols
            .iter()
            .map(|s| self.mapper.denormalize(s, self.itype))
            .collect::<Result<Vec<_>, _>>()?;

        let subscribe_msg = json!({
            "type": "subscribe",
            "product_ids": product_ids,
            "channel": "ticker"
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
                let msg = serde_json::from_str::<AdvancedTradeMessage>(text)?;

                if msg.channel != "ticker" {
                    return Ok(None);
                }

                for event in &msg.events {
                    for ticker in &event.tickers {
                        let bid = ticker.best_bid.parse::<f64>().ok();
                        let ask = ticker.best_ask.parse::<f64>().ok();
                        let bid_qty = ticker.best_bid_quantity.parse::<f64>().ok();
                        let ask_qty = ticker.best_ask_quantity.parse::<f64>().ok();

                        if let (Some(b), Some(a)) = (bid, ask) {
                            if b >= a {
                                warn!(
                                    "Invalid Coinbase AT quote for {}: bid={} >= ask={}",
                                    ticker.product_id, b, a
                                );
                                continue;
                            }
                        }

                        let exchange_ts = DateTime::parse_from_rfc3339(&msg.timestamp)
                            .ok()
                            .map(|dt| dt.with_timezone(&Utc));

                        // Convert BTC-PERP-INTX -> BTCUSD for registry lookup
                        let (base, quote) = self.mapper.parse(&ticker.product_id, self.itype)?;
                        let sym = format!("{}{}", base, quote);

                        let market_data = MarketData {
                            bid,
                            ask,
                            bid_qty,
                            ask_qty,
                            exchange_ts,
                            received_ts: Some(received_ts),
                        };

                        return Ok(Some((sym, market_data)));
                    }
                }

                Ok(None)
            }
            WireMessage::Binary(_) => Ok(None),
        }
    }
}

pub async fn listen_perp_bbo(
    data: Arc<Mutex<MarketDataCollection>>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let feed = Arc::new(CoinbaseAdvancedFeed::new_perp());
    listen_with_reconnect(
        data,
        symbols,
        feed,
        "coinbase_perp",
        ConnectionConfig::default(),
        shutdown,
    )
    .await
}
