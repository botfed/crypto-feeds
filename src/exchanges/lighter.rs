use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use futures_util::SinkExt;
use futures_util::stream::SplitSink;
use log::{debug, error};
use reqwest::Client;
use reqwest::header::{ACCEPT, USER_AGENT};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;

use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

use crate::exchange_fees::{ExchangeFees, FeeSchedule};
use crate::exchanges::connection::{
    ConnectionConfig, ExchangeFeed, WireMessage, listen_with_reconnect,
};
use crate::mappers::{LighterMapper, SymbolMapper};
use crate::market_data::{InstrumentType, MarketData, MarketDataCollection};
use crate::orderbook::OrderBook;

pub fn get_fees() -> ExchangeFees {
    ExchangeFees::new(FeeSchedule::new(0.0, 0.0), FeeSchedule::new(0.0, 0.0))
}

type Book = Arc<Mutex<OrderBook>>;

#[derive(Debug, Clone, Deserialize)]
struct MarketIndexRow {
    symbol: String,
    market_index: u32,
}

async fn fetch_market_indices(client: &Client) -> Result<Vec<MarketIndexRow>> {
    let url = "https://explorer.elliot.ai/api/markets";

    let resp = client
        .get(url)
        .header(ACCEPT, "application/json")
        .header(USER_AGENT, "curl/8.5.0")
        .send()
        .await
        .with_context(|| format!("GET {url} failed"))?;

    let status = resp.status();
    let body = resp.text().await.context("read response body")?;

    if !status.is_success() {
        bail!("GET {url} -> {status}; body: {body}");
    }

    let rows: Vec<MarketIndexRow> = serde_json::from_str(&body).context("decode markets JSON")?;
    Ok(rows)
}

#[derive(Clone)]
struct LighterFeed {
    /// Per-symbol orderbooks; values are locked individually.
    books: HashMap<String, Book>,
    /// Requested symbol -> market_index (symbols are API symbols like "ETH", "BTC", etc.)
    sym_to_index: HashMap<String, u32>,
    /// Reverse map for decoding inbound messages.
    index_to_sym: HashMap<u32, String>,
    itype: InstrumentType,
    mapper: LighterMapper,
}

impl LighterFeed {
    /// Build the feed by loading the dynamic market index mapping from REST.
    /// `symbols` must be API symbols exactly as returned by the markets endpoint (e.g. "ETH", not "ETH-USD").
    async fn new_perp(normalized_symbols: &[&str]) -> Result<Self> {
        let client = Client::new();
        let rows = fetch_market_indices(&client).await?;
        let itype = InstrumentType::Perp;
        let mapper = LighterMapper;

        // API symbol -> market_index
        let api_map: HashMap<String, u32> = rows
            .into_iter()
            .map(|r| (r.symbol, r.market_index))
            .collect();

        let mut sym_to_index: HashMap<String, u32> = HashMap::new();
        let mut index_to_sym: HashMap<u32, String> = HashMap::new();

        let native_symbols: Vec<String> = normalized_symbols
            .iter()
            .map(|normalized| mapper.denormalize(normalized, itype))
            .collect::<Result<Vec<_>, _>>()?;

        // Fail fast if a configured symbol is not present
        for sym in &native_symbols {
            let idx = *api_map
                .get(sym)
                .ok_or_else(|| anyhow!("Lighter symbol '{sym}' not found in markets endpoint"))?;

            sym_to_index.insert(sym.to_string(), idx);
            index_to_sym.insert(idx, sym.to_string());
        }

        let books = native_symbols
            .iter()
            .map(|s| ((*s).to_string(), Arc::new(Mutex::new(OrderBook::new()))))
            .collect();

        Ok(Self {
            books,
            sym_to_index,
            index_to_sym,
            itype,
            mapper,
        })
    }
}

#[derive(Debug, Deserialize)]
struct LighterOrderBookMsg {
    /// e.g. "order_book:{MARKET_INDEX}"
    channel: String,
    offset: u64,
    #[serde(rename = "order_book")]
    order_book: LighterOrderBookData,
    #[serde(rename = "type")]
    msg_type: String,
}

#[derive(Debug, Deserialize)]
struct LighterOrderBookData {
    code: i32,
    asks: Vec<PriceLevel>,
    bids: Vec<PriceLevel>,
    offset: u64,
    // tolerate optional fields
    #[serde(default)]
    nonce: Option<u64>,
    #[serde(default)]
    timestamp: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct PriceLevel {
    price: String,
    size: String,
}

fn parse_market_index(channel: &str) -> Option<u32> {
    channel.strip_prefix("order_book:")?.parse().ok()
}

#[async_trait::async_trait]
impl ExchangeFeed for LighterFeed {
    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }
    fn build_url(&self, _symbols: &[&str]) -> Result<String> {
        Ok("wss://mainnet.zklighter.elliot.ai/stream".to_string())
    }

    async fn send_subscription(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbols: &[&str],
    ) -> Result<()> {
        println!("Lighter sending sub {:?}", symbols);
        for symbol in symbols {
            let native = self.mapper.denormalize(symbol, self.itype).unwrap();
            let Some(&market_index) = self.sym_to_index.get(&native) else {
                error!("Unknown Lighter symbol {}", symbol);
                continue;
            };

            let subscribe_msg = json!({
                "type": "subscribe",
                "channel": format!("order_book/{}", market_index),
            });
            println!("Send lighter sub {}", subscribe_msg);

            write
                .send(Message::Text(subscribe_msg.to_string().into()))
                .await
                .with_context(|| {
                    format!("failed to subscribe to Lighter order_book/{}", market_index)
                })?;
        }

        Ok(())
    }

    async fn process_other(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        text: &str,
    ) -> Result<()> {
        // Lighter heartbeat: {"type":"ping"}  -> respond with {"type":"pong"}
        // Be tolerant of extra fields and ignore anything we don't recognize.
        #[derive(serde::Deserialize)]
        struct TypeOnly {
            #[serde(rename = "type")]
            msg_type: String,
        }

        let Ok(t) = serde_json::from_str::<TypeOnly>(text) else {
            // Not JSON or not the format we expect; ignore.
            debug!("Lighter got unrecognized message {}", &text);
            return Ok(());
        };

        if t.msg_type == "ping" {
            let pong = serde_json::json!({ "type": "pong" });
            write
                .send(Message::Text(pong.to_string().into()))
                .await
                .context("failed to send pong to Lighter")?;
        }

        Ok(())
    }

    fn parse_message(
        &self,
        msg: WireMessage<'_>,
        received_ts: DateTime<Utc>,
    ) -> Result<Option<(String, MarketData)>> {
        let WireMessage::Text(text) = msg else {
            return Ok(None);
        };

        // Fast-path ignore: if no "order_book" substring, it’s probably not data.
        if !text.contains("order_book") {
            return Ok(None);
        }

        let ob: LighterOrderBookMsg = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };

        // Only handle order book updates/snapshots
        if ob.msg_type != "update/order_book" && ob.msg_type != "subscribed/order_book" {
            return Ok(None);
        }

        let Some(market_index) = parse_market_index(&ob.channel) else {
            return Ok(None);
        };

        // Map market_index back to the configured symbol
        let symbol = match self.index_to_sym.get(&market_index) {
            Some(s) => s.as_str(),
            None => {
                // Unknown index (not one we subscribed to) — ignore.
                return Ok(None);
            }
        };

        let Some(book_arc) = self.books.get(symbol) else {
            // Shouldn't happen if mappings/books were built from the same symbol list.
            return Ok(None);
        };

        let mut book = book_arc.lock().unwrap();

        if !ob.order_book.bids.is_empty() {
            book.update_bids(
                ob.order_book
                    .bids
                    .iter()
                    .map(|l| (l.price.clone(), l.size.parse::<f64>().unwrap_or(0.0)))
                    .collect(),
            );
        }
        if !ob.order_book.asks.is_empty() {
            book.update_asks(
                ob.order_book
                    .asks
                    .iter()
                    .map(|l| (l.price.clone(), l.size.parse::<f64>().unwrap_or(0.0)))
                    .collect(),
            );
        }

        let (bid, bid_qty) = book
            .best_bid()
            .map(|(p, s)| (Some(p), Some(s)))
            .unwrap_or((None, None));
        let (ask, ask_qty) = book
            .best_ask()
            .map(|(p, s)| (Some(p), Some(s)))
            .unwrap_or((None, None));

        // If one side missing, skip emitting
        if bid.is_none() || ask.is_none() {
            return Ok(None);
        }

        // Optional sanity
        if let (Some(b), Some(a)) = (bid, ask) {
            if b >= a {
                return Ok(None);
            }
        }

        let exchange_ts = ob
            .order_book
            .timestamp
            .and_then(|ms| DateTime::from_timestamp_millis(ms as i64));

        let md = MarketData {
            bid,
            ask,
            bid_qty,
            ask_qty,
            exchange_ts,
            received_ts: Some(received_ts),
        };

        Ok(Some((symbol.to_string(), md)))
    }
}

/// Public entry point (perp “BBO” derived from order book best levels)
/// IMPORTANT: `symbols` must be API symbols exactly as returned by the markets endpoint (e.g. ["ETH", "BTC"]).
pub async fn listen_perp_bbo(
    data: Arc<Mutex<MarketDataCollection>>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let feed = Arc::new(LighterFeed::new_perp(symbols).await?);

    listen_with_reconnect(
        data,
        symbols,
        feed,
        "lighter_perp",
        ConnectionConfig::default(),
        shutdown,
    )
    .await
}
