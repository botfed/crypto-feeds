use crate::exchange_fees::{ExchangeFees, FeeSchedule};
use crate::exchanges::connection::{
    ConnectionConfig, ExchangeFeed, WireMessage, listen_with_reconnect,
};
use crate::market_data::{InstrumentType, MarketData, MarketDataCollection};
use crate::orderbook::OrderBook;
use crate::trade_data::{TradeData, TradeDataCollection, TradeSide};
use anyhow::Result;
use chrono::{DateTime, Utc};
use futures_util::SinkExt;
use futures_util::stream::SplitSink;
use log::{debug, warn};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

pub fn get_fees() -> ExchangeFees {
    // Hotstuff fee schedule (bps): maker, taker
    ExchangeFees::new(FeeSchedule::new(2.0, 5.0), FeeSchedule::new(2.0, 5.0))
}

// --- JSON-RPC message structures ---

#[derive(Debug, Deserialize)]
struct HotstuffEvent {
    #[allow(dead_code)]
    jsonrpc: String,
    #[allow(dead_code)]
    method: Option<String>,
    params: Option<HotstuffParams>,
}

#[derive(Debug, Deserialize)]
struct HotstuffParams {
    channel: String,
    data: HotstuffData,
}

#[derive(Debug, Deserialize)]
struct HotstuffData {
    update_type: String,
    books: HotstuffBooks,
}

#[derive(Debug, Deserialize)]
struct HotstuffBooks {
    instrument_name: String,
    bids: Vec<HotstuffLevel>,
    asks: Vec<HotstuffLevel>,
    sequence_number: u64,
    timestamp: u64,
}

#[derive(Debug, Deserialize)]
struct HotstuffLevel {
    price: f64,
    size: f64,
}

// --- Feed implementation ---

struct BookState {
    book: OrderBook,
    last_seq: u64,
}

struct HotstuffFeed {
    itype: InstrumentType,
    /// Maps native symbol ("BTC-PERP") → registry symbol ("BTCUSDT")
    native_to_registry: HashMap<String, String>,
    /// Per-symbol orderbook state
    books: std::sync::Mutex<HashMap<String, BookState>>,
}

impl HotstuffFeed {
    fn new(symbols: &[&str], itype: InstrumentType) -> Self {
        let mut native_to_registry = HashMap::new();
        for sym in symbols {
            let parts: Vec<&str> = sym.split('_').collect();
            let base = if parts.len() == 3 {
                parts[1]
            } else if parts.len() == 2 {
                parts[0]
            } else {
                continue;
            };
            let native = format!("{}-PERP", base.to_uppercase());
            let registry = format!("{}USDT", base.to_uppercase());
            native_to_registry.insert(native, registry);
        }
        Self {
            itype,
            native_to_registry,
            books: std::sync::Mutex::new(HashMap::new()),
        }
    }

    fn to_native(sym: &str) -> String {
        let parts: Vec<&str> = sym.split('_').collect();
        let base = if parts.len() == 3 {
            parts[1]
        } else if parts.len() == 2 {
            parts[0]
        } else {
            return sym.to_string();
        };
        format!("{}-PERP", base.to_uppercase())
    }
}

#[async_trait::async_trait]
impl ExchangeFeed for HotstuffFeed {
    type Item = MarketData;

    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }

    fn build_url(&self, _symbols: &[&str]) -> Result<String> {
        Ok("wss://api.hotstuff.trade/ws/".to_string())
    }

    async fn send_subscription(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbols: &[&str],
    ) -> Result<()> {
        for (i, sym) in symbols.iter().enumerate() {
            let native = Self::to_native(sym);
            let sub_msg = json!({
                "jsonrpc": "2.0",
                "id": (i + 1).to_string(),
                "method": "subscribe",
                "params": {
                    "channel": "orderbook",
                    "symbol": native
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
    ) -> Result<Vec<(String, MarketData)>> {
        match msg {
            WireMessage::Text(text) => {
                // Skip subscription confirmations (they have "result" field)
                if text.contains("\"result\"") {
                    debug!("Hotstuff subscription confirmed");
                    return Ok(vec![]);
                }

                let event: HotstuffEvent = serde_json::from_str(text)?;
                let params = match event.params {
                    Some(p) => p,
                    None => return Ok(vec![]),
                };

                if !params.channel.starts_with("orderbook:") {
                    return Ok(vec![]);
                }

                let books = params.data.books;
                let instrument = &books.instrument_name;

                let registry_sym = match self.native_to_registry.get(instrument) {
                    Some(s) => s.clone(),
                    None => {
                        debug!("Unknown symbol from Hotstuff: {}", instrument);
                        return Ok(vec![]);
                    }
                };

                let levels_bid: Vec<(f64, f64)> =
                    books.bids.iter().map(|l| (l.price, l.size)).collect();
                let levels_ask: Vec<(f64, f64)> =
                    books.asks.iter().map(|l| (l.price, l.size)).collect();

                let mut book_map = self.books.lock().unwrap();
                let state = book_map
                    .entry(instrument.clone())
                    .or_insert_with(|| BookState {
                        book: OrderBook::new(),
                        last_seq: 0,
                    });

                match params.data.update_type.as_str() {
                    "snapshot" => {
                        state.book.clear();
                        state.book.update_bids_f64(&levels_bid);
                        state.book.update_asks_f64(&levels_ask);
                        state.last_seq = books.sequence_number;
                    }
                    "delta" => {
                        let expected = state.last_seq + 1;
                        if books.sequence_number != expected && state.last_seq != 0 {
                            warn!(
                                "Hotstuff {} seq gap: expected {}, got {}",
                                instrument, expected, books.sequence_number
                            );
                        }
                        state.book.update_bids_f64(&levels_bid);
                        state.book.update_asks_f64(&levels_ask);
                        state.last_seq = books.sequence_number;
                    }
                    other => {
                        debug!("Hotstuff unknown update_type: {}", other);
                        return Ok(vec![]);
                    }
                }

                let bid = state.book.best_bid();
                let ask = state.book.best_ask();

                let exchange_ts =
                    DateTime::from_timestamp_millis(books.timestamp as i64);

                let market_data = MarketData {
                    bid: bid.map(|(p, _)| p),
                    ask: ask.map(|(p, _)| p),
                    bid_qty: bid.map(|(_, q)| q),
                    ask_qty: ask.map(|(_, q)| q),
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

pub async fn listen_perp_bbo(
    data: Arc<MarketDataCollection>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let feed = Arc::new(HotstuffFeed::new(symbols, InstrumentType::Perp));
    listen_with_reconnect(
        data,
        symbols,
        feed,
        "hotstuff_perp",
        ConnectionConfig::default(),
        shutdown,
    )
    .await
}

// --- Trade Feed ---

#[derive(Debug, Deserialize)]
struct HotstuffTradeEvent {
    #[allow(dead_code)]
    jsonrpc: Option<String>,
    #[allow(dead_code)]
    method: Option<String>,
    params: Option<HotstuffTradeParams>,
}

#[derive(Debug, Deserialize)]
struct HotstuffTradeParams {
    channel: String,
    data: HotstuffTradeData,
}

#[derive(Debug, Deserialize)]
struct HotstuffTradeData {
    instrument: String,
    side: String,
    price: String,
    size: String,
    timestamp: u64,
}

struct HotstuffTradeFeed {
    itype: InstrumentType,
    native_to_registry: HashMap<String, String>,
}

impl HotstuffTradeFeed {
    fn new(symbols: &[&str], itype: InstrumentType) -> Self {
        let mut native_to_registry = HashMap::new();
        for sym in symbols {
            let parts: Vec<&str> = sym.split('_').collect();
            let base = if parts.len() == 3 { parts[1] } else if parts.len() == 2 { parts[0] } else { continue };
            let native = format!("{}-PERP", base.to_uppercase());
            let registry = format!("{}USDT", base.to_uppercase());
            native_to_registry.insert(native, registry);
        }
        Self { itype, native_to_registry }
    }
}

#[async_trait::async_trait]
impl ExchangeFeed for HotstuffTradeFeed {
    type Item = TradeData;

    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }

    fn build_url(&self, _symbols: &[&str]) -> Result<String> {
        Ok("wss://api.hotstuff.trade/ws/".to_string())
    }

    async fn send_subscription(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbols: &[&str],
    ) -> Result<()> {
        for (i, sym) in symbols.iter().enumerate() {
            let parts: Vec<&str> = sym.split('_').collect();
            let base = if parts.len() == 3 { parts[1] } else if parts.len() == 2 { parts[0] } else { continue };
            let native = format!("{}-PERP", base.to_uppercase());
            let sub_msg = json!({
                "jsonrpc": "2.0",
                "id": (i + 1).to_string(),
                "method": "subscribe",
                "params": {
                    "channel": "trades",
                    "symbol": native
                }
            });
            write.send(Message::Text(sub_msg.to_string().into())).await?;
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
                if text.contains("\"result\"") || !text.contains("\"trades:") {
                    return Ok(vec![]);
                }

                let event: HotstuffTradeEvent = serde_json::from_str(text)?;
                let params = match event.params {
                    Some(p) => p,
                    None => return Ok(vec![]),
                };

                if !params.channel.starts_with("trades:") {
                    return Ok(vec![]);
                }

                let data = params.data;
                let registry_sym = match self.native_to_registry.get(&data.instrument) {
                    Some(s) => s.clone(),
                    None => {
                        debug!("Unknown symbol from Hotstuff trades: {}", data.instrument);
                        return Ok(vec![]);
                    }
                };

                let price = data.price.parse::<f64>()?;
                let qty = data.size.parse::<f64>()?;
                let side = match data.side.as_str() {
                    "b" => TradeSide::Buy,
                    "s" => TradeSide::Sell,
                    _ => TradeSide::Unknown,
                };
                let exchange_ts = DateTime::from_timestamp_millis(data.timestamp as i64);

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
            WireMessage::Binary(_) => Ok(vec![]),
        }
    }
}

pub async fn listen_perp_trades(
    data: Arc<TradeDataCollection>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let feed = Arc::new(HotstuffTradeFeed::new(symbols, InstrumentType::Perp));
    listen_with_reconnect(
        data,
        symbols,
        feed,
        "hotstuff_perp_trades",
        ConnectionConfig::default(),
        shutdown,
    )
    .await
}
