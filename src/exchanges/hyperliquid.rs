use crate::exchange_fees::{ExchangeFees, FeeSchedule};
use crate::exchanges::connection::{
    ConnectionConfig, ExchangeFeed, WireMessage, listen_with_reconnect,
};
use crate::market_data::{InstrumentType, MarketData, MarketDataCollection};
use crate::trade_data::{TradeData, TradeDataCollection, TradeSide};
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
    ExchangeFees::new(FeeSchedule::new(3.5, 1.0), FeeSchedule::new(3.5, 1.0))
}

#[derive(Clone)]
struct HyperliquidFeed {
    itype: InstrumentType,
    /// Maps coin name ("BTC") → registry-compatible symbol ("BTCUSDT")
    coin_to_symbol: HashMap<String, String>,
}

impl HyperliquidFeed {
    fn new(symbols: &[&str]) -> Self {
        let mut coin_to_symbol = HashMap::new();
        for sym in symbols {
            let parts: Vec<&str> = sym.split('_').collect();
            let (base, quote) = if parts.len() == 3 {
                (parts[1], parts[2])
            } else if parts.len() == 2 {
                (parts[0], parts[1])
            } else {
                continue;
            };
            // Hyperliquid uses bare coin names; map back to concatenated format
            // so the registry can look it up (e.g. "BTC" → "BTCUSDT")
            coin_to_symbol.insert(
                base.to_uppercase(),
                format!("{}{}", base.to_uppercase(), quote.to_uppercase()),
            );
        }
        Self {
            itype: InstrumentType::Perp,
            coin_to_symbol,
        }
    }

    fn coin_from_config(sym: &str) -> String {
        let parts: Vec<&str> = sym.split('_').collect();
        if parts.len() == 3 {
            parts[1].to_uppercase()
        } else if parts.len() == 2 {
            parts[0].to_uppercase()
        } else {
            sym.to_uppercase()
        }
    }
}

#[async_trait::async_trait]
impl ExchangeFeed for HyperliquidFeed {
    type Item = MarketData;

    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }

    fn heartbeat_message(&self) -> Option<Message> {
        Some(Message::Text(r#"{"method":"ping"}"#.into()))
    }

    fn build_url(&self, _symbols: &[&str]) -> Result<String> {
        Ok("wss://api.hyperliquid.xyz/ws".to_string())
    }

    async fn send_subscription(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbols: &[&str],
    ) -> Result<()> {
        for sym in symbols {
            let coin = Self::coin_from_config(sym);
            let sub_msg = json!({
                "method": "subscribe",
                "subscription": {
                    "type": "l2Book",
                    "coin": coin
                }
            });
            write
                .send(Message::Text(sub_msg.to_string().into()))
                .await?;
        }
        Ok(())
    }

    fn parse_message(
        &self,
        msg: WireMessage<'_>,
        received_ts: DateTime<Utc>,
        received_instant: std::time::Instant,
    ) -> Result<Vec<(String, MarketData)>> {
        match msg {
            WireMessage::Text(text) => {
                if text.contains("\"channel\":\"subscriptionResponse\"")
                    || text.contains("\"channel\":\"pong\"")
                {
                    debug!("Hyperliquid control message");
                    return Ok(vec![]);
                }

                if !text.contains("\"channel\":\"l2Book\"") {
                    return Ok(vec![]);
                }

                let response: HyperliquidL2Book = serde_json::from_str(text)?;
                let coin = &response.data.coin;

                let registry_sym = match self.coin_to_symbol.get(coin) {
                    Some(s) => s.clone(),
                    None => {
                        debug!("Unknown coin from Hyperliquid: {}", coin);
                        return Ok(vec![]);
                    }
                };

                let bids = &response.data.levels[0];
                let asks = &response.data.levels[1];

                let bid = bids.first().and_then(|l| l.px.parse::<f64>().ok());
                let ask = asks.first().and_then(|l| l.px.parse::<f64>().ok());
                let bid_qty = bids.first().and_then(|l| l.sz.parse::<f64>().ok());
                let ask_qty = asks.first().and_then(|l| l.sz.parse::<f64>().ok());

                let exchange_ts =
                    DateTime::from_timestamp_millis(response.data.time as i64)
                        .map(|dt| dt.with_timezone(&Utc));

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

                Ok(vec![(registry_sym, market_data)])
            }
            WireMessage::Binary(_) => Ok(vec![]),
        }
    }
}

#[derive(Debug, Deserialize)]
struct HyperliquidL2Book {
    data: HyperliquidL2Data,
}

#[derive(Debug, Deserialize)]
struct HyperliquidL2Data {
    coin: String,
    time: u64,
    levels: Vec<Vec<HyperliquidLevel>>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct HyperliquidLevel {
    px: String,
    sz: String,
    n: u64,
}

pub async fn listen_perp_bbo(
    data: Arc<MarketDataCollection>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let feed = Arc::new(HyperliquidFeed::new(symbols));
    listen_with_reconnect(
        data,
        symbols,
        feed,
        "hyperliquid_perp",
        ConnectionConfig::default(),
        shutdown,
    )
    .await
}

// ---------------------------------------------------------------------------
// Trade feed
// ---------------------------------------------------------------------------

// Wire: {"channel":"trades","data":[{"coin":"BTC","side":"B","px":"77000","sz":"0.1","hash":"...","time":1676151190656,"tid":123}]}

#[derive(Debug, Deserialize)]
struct HyperliquidTradesMsg {
    channel: String,
    data: Vec<HyperliquidTrade>,
}

#[derive(Debug, Deserialize)]
struct HyperliquidTrade {
    coin: String,
    side: String,
    px: String,
    sz: String,
    time: u64,
}

struct HyperliquidTradeFeed {
    itype: InstrumentType,
    coin_to_symbol: HashMap<String, String>,
}

impl HyperliquidTradeFeed {
    fn new(symbols: &[&str]) -> Self {
        let mut coin_to_symbol = HashMap::new();
        for sym in symbols {
            let parts: Vec<&str> = sym.split('_').collect();
            let (base, quote) = if parts.len() == 3 {
                (parts[1], parts[2])
            } else if parts.len() == 2 {
                (parts[0], parts[1])
            } else {
                continue;
            };
            coin_to_symbol.insert(
                base.to_uppercase(),
                format!("{}{}", base.to_uppercase(), quote.to_uppercase()),
            );
        }
        Self {
            itype: InstrumentType::Perp,
            coin_to_symbol,
        }
    }
}

#[async_trait::async_trait]
impl ExchangeFeed for HyperliquidTradeFeed {
    type Item = TradeData;

    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }

    fn heartbeat_message(&self) -> Option<Message> {
        Some(Message::Text(r#"{"method":"ping"}"#.into()))
    }

    fn build_url(&self, _symbols: &[&str]) -> Result<String> {
        Ok("wss://api.hyperliquid.xyz/ws".to_string())
    }

    async fn send_subscription(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbols: &[&str],
    ) -> Result<()> {
        for sym in symbols {
            let coin = HyperliquidFeed::coin_from_config(sym);
            let sub_msg = json!({
                "method": "subscribe",
                "subscription": {
                    "type": "trades",
                    "coin": coin
                }
            });
            write
                .send(Message::Text(sub_msg.to_string().into()))
                .await?;
        }
        Ok(())
    }

    fn timestamp_dedup(&self) -> bool {
        false
    }

    fn parse_message(
        &self,
        msg: WireMessage<'_>,
        received_ts: DateTime<Utc>,
        received_instant: std::time::Instant,
    ) -> Result<Vec<(String, TradeData)>> {
        match msg {
            WireMessage::Text(text) => {
                if !text.contains("\"channel\":\"trades\"") {
                    return Ok(vec![]);
                }

                let msg: HyperliquidTradesMsg = match serde_json::from_str(text) {
                    Ok(v) => v,
                    Err(_) => return Ok(vec![]),
                };

                if msg.channel != "trades" {
                    return Ok(vec![]);
                }

                let mut results = Vec::with_capacity(msg.data.len());
                for t in &msg.data {
                    let registry_sym = match self.coin_to_symbol.get(&t.coin) {
                        Some(s) => s.clone(),
                        None => continue,
                    };
                    let price = t.px.parse::<f64>()?;
                    let qty = t.sz.parse::<f64>()?;
                    let side = match t.side.as_str() {
                        "B" => TradeSide::Buy,
                        "A" => TradeSide::Sell,
                        _ => TradeSide::Unknown,
                    };
                    let exchange_ts = DateTime::from_timestamp_millis(t.time as i64);

                    results.push((registry_sym, TradeData {
                        price,
                        qty,
                        side,
                        exchange_ts_raw: exchange_ts,
                        exchange_ts: None,
                        received_ts: Some(received_ts),
                        received_instant: Some(received_instant),
                        feed_latency_ns: 0,
                    }));
                }

                Ok(results)
            }
            WireMessage::Binary(_) => Ok(vec![]),
        }
    }
}

pub async fn listen_perp_trades(
    data: Arc<TradeDataCollection>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let feed = Arc::new(HyperliquidTradeFeed::new(symbols));
    listen_with_reconnect(
        data,
        symbols,
        feed,
        "hyperliquid_perp_trades",
        ConnectionConfig::default(),
        shutdown,
    )
    .await
}
