use crate::exchange_fees::{ExchangeFees, FeeSchedule};
use crate::exchanges::connection::{
    ConnectionConfig, ExchangeFeed, WireMessage, listen_with_reconnect,
};
use crate::market_data::{InstrumentType, MarketData, MarketDataCollection};
use crate::trade_data::{TradeData, TradeDataCollection, TradeSide};
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

// ---------------------------------------------------------------------------
// Trade feed
// ---------------------------------------------------------------------------

// Wire format:
// {"data":{"trade":{"price":"77070.89","quantity":"0.003","takerSide":"Buy","timestamp":1777330560}},"symbol":"BTC/USDT-P","topic":"trades"}

#[derive(Debug, Deserialize)]
struct HibachiTradeMsg {
    symbol: String,
    data: HibachiTradeWrapper,
}

#[derive(Debug, Deserialize)]
struct HibachiTradeWrapper {
    trade: HibachiTradeData,
}

#[derive(Debug, Deserialize)]
struct HibachiTradeData {
    price: String,
    quantity: String,
    #[serde(rename = "takerSide")]
    taker_side: String,
    timestamp: u64,
}

struct HibachiTradeFeed {
    itype: InstrumentType,
    native_to_registry: HashMap<String, String>,
}

impl HibachiTradeFeed {
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
        Self { itype, native_to_registry }
    }
}

#[async_trait::async_trait]
impl ExchangeFeed for HibachiTradeFeed {
    type Item = TradeData;

    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }

    fn timestamp_dedup(&self) -> bool {
        false
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
                let native = HibachiFeed::to_native(sym, self.itype);
                json!({"symbol": native, "topic": "trades"})
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
    ) -> Result<Vec<(String, TradeData)>> {
        let WireMessage::Text(text) = msg else {
            return Ok(vec![]);
        };

        if !text.contains("\"trades\"") {
            return Ok(vec![]);
        }

        let trade_msg: HibachiTradeMsg = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(_) => return Ok(vec![]),
        };

        let registry_sym = match self.native_to_registry.get(&trade_msg.symbol) {
            Some(s) => s.clone(),
            None => {
                debug!("Unknown symbol from Hibachi trades: {}", trade_msg.symbol);
                return Ok(vec![]);
            }
        };

        let t = &trade_msg.data.trade;
        let price = t.price.parse::<f64>()?;
        let qty = t.quantity.parse::<f64>()?;
        let side = match t.taker_side.as_str() {
            "Buy" => TradeSide::Buy,
            "Sell" => TradeSide::Sell,
            _ => TradeSide::Unknown,
        };
        let exchange_ts = DateTime::from_timestamp_millis(t.timestamp as i64 * 1000);

        let trade = TradeData {
            price,
            qty,
            side,
            exchange_ts_raw: exchange_ts,
            exchange_ts: None,
            received_ts: Some(received_ts),
            received_instant: Some(received_instant),
            feed_latency_ns: 0,
        };

        Ok(vec![(registry_sym, trade)])
    }
}

pub async fn listen_perp_trades(
    data: Arc<TradeDataCollection>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let feed = Arc::new(HibachiTradeFeed::new(symbols, InstrumentType::Perp));
    listen_with_reconnect(
        data,
        symbols,
        feed,
        "hibachi_perp_trades",
        ConnectionConfig::default(),
        shutdown,
    )
    .await
}
