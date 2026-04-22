use crate::exchange_fees::{ExchangeFees, FeeSchedule};
use crate::exchanges::connection::{
    ConnectionConfig, ExchangeFeed, WireMessage, listen_with_reconnect,
};
use crate::market_data::{InstrumentType, MarketData, MarketDataCollection};
use crate::orderbook::SyncBook;
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
    ExchangeFees::new(FeeSchedule::new(2.0, 5.0), FeeSchedule::new(2.0, 5.0))
}

struct HibachiFeed {
    itype: InstrumentType,
    /// Maps hibachi native symbol ("BTC/USDT-P") → registry-compatible symbol ("BTCUSDT")
    native_to_registry: HashMap<String, String>,
    /// Per-symbol orderbooks (single-writer: one WS task)
    books: HashMap<String, SyncBook>,
}

impl HibachiFeed {
    fn new(symbols: &[&str], itype: InstrumentType) -> Self {
        let mut native_to_registry = HashMap::new();
        let mut books = HashMap::new();
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
            native_to_registry.insert(native.clone(), registry);
            books.insert(native, SyncBook::new());
        }
        Self {
            itype,
            native_to_registry,
            books,
        }
    }

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

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct HibachiOrderbookMsg {
    symbol: String,
    #[serde(rename = "messageType")]
    message_type: String,
    #[serde(default)]
    timestamp_ms: Option<u64>,
    data: HibachiOrderbookData,
}

#[derive(Debug, Deserialize)]
struct HibachiOrderbookData {
    bid: HibachiSide,
    ask: HibachiSide,
}

#[derive(Debug, Deserialize)]
struct HibachiSide {
    levels: Vec<HibachiLevel>,
}

#[derive(Debug, Deserialize)]
struct HibachiLevel {
    price: String,
    quantity: String,
}

#[async_trait::async_trait]
impl ExchangeFeed for HibachiFeed {
    type Item = MarketData;

    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }

    fn timestamp_dedup(&self) -> bool {
        false // incremental depth feed
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
                json!({"symbol": native, "topic": "orderbook"})
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
    ) -> Result<Vec<(String, MarketData)>> {
        let WireMessage::Text(text) = msg else {
            return Ok(vec![]);
        };

        if !text.contains("\"orderbook\"") {
            return Ok(vec![]);
        }

        let ob: HibachiOrderbookMsg = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(_) => return Ok(vec![]),
        };

        let registry_sym = match self.native_to_registry.get(&ob.symbol) {
            Some(s) => s.clone(),
            None => {
                debug!("Unknown symbol from Hibachi: {}", ob.symbol);
                return Ok(vec![]);
            }
        };

        let Some(book_cell) = self.books.get(&ob.symbol) else {
            return Ok(vec![]);
        };

        // SAFETY: single writer — one WS task per feed.
        let book = unsafe { book_cell.get_mut() };

        let is_snapshot = ob.message_type == "Snapshot";
        if is_snapshot {
            book.bids.clear();
            book.asks.clear();
        }

        if !ob.data.bid.levels.is_empty() {
            book.update_bids(
                ob.data.bid.levels.iter()
                    .map(|l| (l.price.clone(), l.quantity.parse::<f64>().unwrap_or(0.0)))
                    .collect(),
            );
        }
        if !ob.data.ask.levels.is_empty() {
            book.update_asks(
                ob.data.ask.levels.iter()
                    .map(|l| (l.price.clone(), l.quantity.parse::<f64>().unwrap_or(0.0)))
                    .collect(),
            );
        }

        let (bid, bid_qty) = book.best_bid()
            .map(|(p, s)| (Some(p), Some(s)))
            .unwrap_or((None, None));
        let (ask, ask_qty) = book.best_ask()
            .map(|(p, s)| (Some(p), Some(s)))
            .unwrap_or((None, None));

        if bid.is_none() || ask.is_none() {
            return Ok(vec![]);
        }

        if let (Some(b), Some(a)) = (bid, ask) {
            if b >= a {
                return Ok(vec![]);
            }
        }

        let exchange_ts = ob.timestamp_ms
            .and_then(|ms| DateTime::from_timestamp_millis(ms as i64));

        let md = MarketData {
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

        Ok(vec![(registry_sym, md)])
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
