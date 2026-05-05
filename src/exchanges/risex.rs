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

// ---------------------------------------------------------------------------
// Symbol mapping: config "BTC_USDC" → RiseX market_id, registry "BTCUSDC"
// ---------------------------------------------------------------------------

/// Map from config symbol (e.g. "BTC_USDC") to (market_id, registry_symbol).
fn build_symbol_maps(symbols: &[&str]) -> (HashMap<u32, String>, Vec<u32>) {
    // Known market IDs from RiseX mainnet
    let known: &[(&str, u32)] = &[
        ("BTC", 1), ("ETH", 2), ("BNB", 3), ("SOL", 4),
        ("HYPE", 5), ("XRP", 6), ("TAO", 7), ("ZEC", 8),
    ];

    let mut id_to_registry = HashMap::new();
    let mut market_ids = Vec::new();

    for sym in symbols {
        let parts: Vec<&str> = sym.split('_').collect();
        let (base, quote) = if parts.len() == 3 {
            (parts[1], parts[2])
        } else if parts.len() == 2 {
            (parts[0], parts[1])
        } else {
            continue;
        };

        let base_upper = base.to_uppercase();
        if let Some(&(_, mid)) = known.iter().find(|(b, _)| *b == base_upper) {
            let registry = format!("{}{}", base_upper, quote.to_uppercase());
            id_to_registry.insert(mid, registry);
            market_ids.push(mid);
        }
    }

    (id_to_registry, market_ids)
}

// ---------------------------------------------------------------------------
// BBO Feed (orderbook channel)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct RiseObMsg {
    #[serde(default)]
    channel: String,
    #[serde(default)]
    r#type: String,
    #[serde(default)]
    market_id: serde_json::Value, // can be string or number
    #[serde(default)]
    data: Option<RiseObData>,
    #[serde(default)]
    timestamp: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RiseObData {
    #[serde(default)]
    market_id: Option<u32>,
    #[serde(default)]
    bids: Vec<RiseObLevel>,
    #[serde(default)]
    asks: Vec<RiseObLevel>,
}

#[derive(Debug, Deserialize)]
struct RiseObLevel {
    price: String,
    quantity: String,
    #[serde(default)]
    order_count: Option<u32>,
}

struct RiseXBboFeed {
    itype: InstrumentType,
    id_to_registry: HashMap<u32, String>,
    market_ids: Vec<u32>,
    books: HashMap<u32, SyncBook>,
}

impl RiseXBboFeed {
    fn new(symbols: &[&str], itype: InstrumentType) -> Self {
        let (id_to_registry, market_ids) = build_symbol_maps(symbols);
        let mut books = HashMap::new();
        for &mid in &market_ids {
            books.insert(mid, SyncBook::new());
        }
        Self { itype, id_to_registry, market_ids, books }
    }
}

#[async_trait::async_trait]
impl ExchangeFeed for RiseXBboFeed {
    type Item = MarketData;

    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }

    fn build_url(&self, _symbols: &[&str]) -> Result<String> {
        Ok("wss://ws.rise.trade/ws".to_string())
    }

    fn timestamp_dedup(&self) -> bool {
        false
    }

    fn heartbeat_message(&self) -> Option<Message> {
        Some(Message::Text("{\"op\":\"ping\"}".to_string().into()))
    }

    async fn send_subscription(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        _symbols: &[&str],
    ) -> Result<()> {
        let sub = json!({
            "method": "subscribe",
            "params": {
                "channel": "orderbook",
                "market_ids": self.market_ids,
            }
        });
        write.send(Message::Text(sub.to_string().into())).await?;
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

        let ob: RiseObMsg = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(_) => return Ok(vec![]),
        };

        if ob.channel != "orderbook" {
            return Ok(vec![]);
        }

        let Some(data) = ob.data else { return Ok(vec![]); };

        // Resolve market_id from either the data object or the top-level field
        let market_id = data.market_id.unwrap_or_else(|| {
            match &ob.market_id {
                serde_json::Value::Number(n) => n.as_u64().unwrap_or(0) as u32,
                serde_json::Value::String(s) => s.parse().unwrap_or(0),
                _ => 0,
            }
        });

        let registry_sym = match self.id_to_registry.get(&market_id) {
            Some(s) => s.clone(),
            None => return Ok(vec![]),
        };

        let Some(book_cell) = self.books.get(&market_id) else {
            return Ok(vec![]);
        };

        // SAFETY: single writer — one WS task per feed.
        let book = unsafe { book_cell.get_mut() };

        let is_snapshot = ob.r#type == "snapshot";
        if is_snapshot {
            book.bids.clear();
            book.asks.clear();
        }

        if !data.bids.is_empty() {
            book.update_bids(
                data.bids.iter()
                    .map(|l| (l.price.clone(), l.quantity.parse::<f64>().unwrap_or(0.0)))
                    .collect(),
            );
        }
        if !data.asks.is_empty() {
            book.update_asks(
                data.asks.iter()
                    .map(|l| (l.price.clone(), l.quantity.parse::<f64>().unwrap_or(0.0)))
                    .collect(),
            );
        }

        let bid = book.best_bid();
        let ask = book.best_ask();

        let exchange_ts = ob.timestamp.as_ref().and_then(|ts| {
            // Timestamp is nanoseconds
            ts.parse::<i64>().ok().and_then(|ns| {
                DateTime::from_timestamp(ns / 1_000_000_000, (ns % 1_000_000_000) as u32)
            })
        });

        let market_data = MarketData {
            bid: bid.map(|(p, _)| p),
            ask: ask.map(|(p, _)| p),
            bid_qty: bid.map(|(_, q)| q),
            ask_qty: ask.map(|(_, q)| q),
            exchange_ts_raw: exchange_ts,
            exchange_ts,
            received_ts: Some(received_ts),
            received_instant: Some(received_instant),
            feed_latency_ns: 0,
        };

        Ok(vec![(registry_sym, market_data)])
    }
}

pub async fn listen_perp_bbo(
    data: Arc<MarketDataCollection>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let feed = Arc::new(RiseXBboFeed::new(symbols, InstrumentType::Perp));
    listen_with_reconnect(
        data, symbols, feed, "risex_perp",
        ConnectionConfig::default(), shutdown,
    ).await
}

// ---------------------------------------------------------------------------
// Trade Feed
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct RiseTradeMsg {
    #[serde(default)]
    channel: String,
    #[serde(default)]
    r#type: String,
    #[serde(default)]
    market_id: serde_json::Value,
    #[serde(default)]
    data: Option<RiseTradeData>,
    #[serde(default)]
    timestamp: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RiseTradeData {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    maker_side: Option<u32>,
    #[serde(default)]
    price: String,
    #[serde(default)]
    size: String,
}

struct RiseXTradeFeed {
    itype: InstrumentType,
    id_to_registry: HashMap<u32, String>,
    market_ids: Vec<u32>,
}

impl RiseXTradeFeed {
    fn new(symbols: &[&str], itype: InstrumentType) -> Self {
        let (id_to_registry, market_ids) = build_symbol_maps(symbols);
        Self { itype, id_to_registry, market_ids }
    }
}

#[async_trait::async_trait]
impl ExchangeFeed for RiseXTradeFeed {
    type Item = TradeData;

    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }

    fn build_url(&self, _symbols: &[&str]) -> Result<String> {
        Ok("wss://ws.rise.trade/ws".to_string())
    }

    fn timestamp_dedup(&self) -> bool {
        false
    }

    fn heartbeat_message(&self) -> Option<Message> {
        Some(Message::Text("{\"op\":\"ping\"}".to_string().into()))
    }

    async fn send_subscription(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        _symbols: &[&str],
    ) -> Result<()> {
        let sub = json!({
            "method": "subscribe",
            "params": {
                "channel": "trades",
                "market_ids": self.market_ids,
            }
        });
        write.send(Message::Text(sub.to_string().into())).await?;
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

        let trade_msg: RiseTradeMsg = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(_) => return Ok(vec![]),
        };

        if trade_msg.channel != "trades" || trade_msg.r#type != "update" {
            return Ok(vec![]);
        }

        let Some(data) = trade_msg.data else { return Ok(vec![]); };

        let market_id = match &trade_msg.market_id {
            serde_json::Value::Number(n) => n.as_u64().unwrap_or(0) as u32,
            serde_json::Value::String(s) => s.parse().unwrap_or(0),
            _ => 0,
        };

        let registry_sym = match self.id_to_registry.get(&market_id) {
            Some(s) => s.clone(),
            None => return Ok(vec![]),
        };

        let price: f64 = data.price.parse().unwrap_or(0.0);
        let size: f64 = data.size.parse().unwrap_or(0.0);
        // maker_side: 0=Buy, 1=Sell. Taker is opposite.
        let taker_side = match data.maker_side {
            Some(0) => TradeSide::Sell,
            Some(1) => TradeSide::Buy,
            _ => TradeSide::Unknown,
        };

        let exchange_ts = trade_msg.timestamp.as_ref().and_then(|ts| {
            ts.parse::<i64>().ok().and_then(|ns| {
                DateTime::from_timestamp(ns / 1_000_000_000, (ns % 1_000_000_000) as u32)
            })
        });

        let trade = TradeData {
            price,
            qty: size,
            side: taker_side,
            exchange_ts_raw: exchange_ts,
            exchange_ts,
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
    let feed = Arc::new(RiseXTradeFeed::new(symbols, InstrumentType::Perp));
    listen_with_reconnect(
        data, symbols, feed, "risex_trades",
        ConnectionConfig::default(), shutdown,
    ).await
}
