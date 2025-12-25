use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, stream::SplitSink};
use log::error;
use prost::Message as ProstMessage;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

use crate::exchanges::connection::{
    ConnectionConfig, ExchangeFeed, WireMessage, listen_with_reconnect,
};
use crate::mappers::{MexcMapper, SymbolMapper};
use crate::market_data::{InstrumentType, MarketData, MarketDataCollection};
use crate::orderbook::OrderBook;

use crate::exchange_fees::{ExchangeFees, FeeSchedule};

pub fn get_fees() -> ExchangeFees {
    ExchangeFees::new(FeeSchedule::new(5.0, 0.0), FeeSchedule::new(2.0, 0.0))
}

// ---- Spot protobuf (bookTicker) ----

// Include the generated protobuf code (spot bookTicker pb stream)
mod mexc_proto {
    include!(concat!(env!("OUT_DIR"), "/_.rs"));
}
use mexc_proto::MexcWrapper;

// Parse MEXC spot protobuf book ticker -> (symbol, bid, ask, bid_qty, ask_qty)
fn parse_mexc_spot_bookticker_pb(data: &[u8]) -> Option<(String, f64, f64, f64, f64)> {
    let wrapper = MexcWrapper::decode(data).ok()?;
    let book = wrapper.public_aggre_book_ticker?;
    let symbol = wrapper.symbol;

    let bid = book.bid_price.parse::<f64>().ok()?;
    let bid_qty = book.bid_quantity.parse::<f64>().ok()?;
    let ask = book.ask_price.parse::<f64>().ok()?;
    let ask_qty = book.ask_quantity.parse::<f64>().ok()?;

    Some((symbol, bid, ask, bid_qty, ask_qty))
}

// ---- Futures perps depth (order book) ----
// Futures WebSocket depth message shape (per docs) :contentReference[oaicite:3]{index=3}
//
// {
//   "channel":"push.depth",
//   "data":{"asks":[[6859.5,3251,1]],"bids":[],"version":96801927},
//   "symbol":"BTC_USDT",
//   "ts":1587442022003
// }
//

#[derive(Debug, Deserialize)]
struct MexcFuturesDepthMsg {
    channel: String,
    symbol: String,
    data: MexcFuturesDepthData,
    #[serde(default)]
    ts: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct MexcFuturesDepthData {
    #[serde(default)]
    asks: Vec<[f64; 3]>,
    #[serde(default)]
    bids: Vec<[f64; 3]>,
    #[serde(default)]
    version: i64,
}

// Convert futures depth levels into your OrderBook::update_* format: Vec<(String, f64)>
// - price as String (your OrderBook parses it)
// - size = quantity (2nd element)
fn depth_levels_to_updates(levels: &[[f64; 3]]) -> Vec<(String, f64)> {
    levels
        .iter()
        .map(|lvl| (lvl[0].to_string(), lvl[1]))
        .collect()
}

type Book = Arc<Mutex<OrderBook>>;

#[derive(Clone)]
struct MexcFeed {
    // Used for perps depth -> BBO derivation
    books: HashMap<String, Book>,
    market: InstrumentType,
    mapper: MexcMapper,
}

impl MexcFeed {
    fn new_spot(symbols: &[&str]) -> Self {
        let mut books = HashMap::new();
        let mapper = MexcMapper;
        let itype = InstrumentType::Spot;

        for normalized in symbols {
            // Denormalize to get native symbol for book key
            if let Ok(native) = mapper.denormalize(normalized, itype) {
                books.insert(native, Arc::new(Mutex::new(OrderBook::new())));
            }
        }

        Self {
            market: itype,
            books: books,
            mapper: mapper,
        }
    }
    fn new_perp(symbols: &[&str]) -> Self {
        let mut books = HashMap::new();
        let mapper = MexcMapper;
        let itype = InstrumentType::Perp;

        for normalized in symbols {
            // Denormalize to get native symbol for book key
            if let Ok(native) = mapper.denormalize(normalized, itype) {
                books.insert(native, Arc::new(Mutex::new(OrderBook::new())));
            }
        }

        Self {
            market: itype,
            books,
            mapper: mapper,
        }
    }
}

#[async_trait::async_trait]
impl ExchangeFeed for MexcFeed {
    fn build_url(&self, _symbols: &[&str]) -> Result<String> {
        match self.market {
            // Spot WS market streams
            InstrumentType::Spot => Ok("wss://wbs-api.mexc.com/ws".to_string()),
            // Futures WS base url
            InstrumentType::Perp => Ok("wss://contract.mexc.com/edge".to_string()), // updated base url in docs/log :contentReference[oaicite:4]{index=4}
            _ => anyhow::bail!("Unsupported instrument type for MEXC: {:?}", self.market),
        }
    }

    fn heartbeat_message(&self) -> Option<Message> {
        match self.market {
            InstrumentType::Perp => Some(Message::Text(r#"{"method":"ping"}"#.into())),
            _ => None,
        }
    }

    async fn send_subscription(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbols: &[&str],
    ) -> Result<()> {
        match self.market {
            InstrumentType::Spot => {
                // Spot protobuf bookTicker stream (true BBO) :contentReference[oaicite:5]{index=5}
                let params: Vec<String> = symbols
                    .iter()
                    .map(|s| {
                        format!(
                            "spot@public.aggre.bookTicker.v3.api.pb@100ms@{}",
                            self.mapper.denormalize(s, InstrumentType::Spot).unwrap()
                        )
                    })
                    .collect();

                let subscribe_msg = json!({
                    "method": "SUBSCRIPTION",
                    "params": params
                });

                write
                    .send(Message::Text(subscribe_msg.to_string().into()))
                    .await
                    .context("Failed to send MEXC spot subscription message")?;

                Ok(())
            }

            InstrumentType::Perp => {
                // Futures depth stream: sub.depth (updates every ~200ms) :contentReference[oaicite:6]{index=6}
                //
                // MEXC futures depth can be "zipped push by default" per update log; request uncompressed to
                // keep parsing simple. :contentReference[oaicite:7]{index=7}
                for s in symbols {
                    let sub = json!({
                        "method": "sub.depth",
                        "param": {
                            "symbol": self.mapper.denormalize(s, InstrumentType::Perp).unwrap(),
                            "compress": false
                        }
                    });
                    write
                        .send(Message::Text(sub.to_string().into()))
                        .await
                        .with_context(|| {
                            format!("Failed to subscribe MEXC perp depth for {}", s)
                        })?;
                }

                Ok(())
            }
            _ => {
                anyhow::bail!("Unsupported asset class {:?}", self.market)
            }
        }
    }

    fn parse_message(
        &self,
        msg: WireMessage<'_>,
        received_ts: DateTime<Utc>,
    ) -> Result<Option<(String, MarketData)>> {
        match self.market {
            InstrumentType::Spot => match msg {
                WireMessage::Binary(bytes) => {
                    if let Some((symbol, bid, ask, bid_qty, ask_qty)) =
                        parse_mexc_spot_bookticker_pb(bytes)
                    {
                        let md = MarketData {
                            bid: Some(bid),
                            ask: Some(ask),
                            bid_qty: Some(bid_qty),
                            ask_qty: Some(ask_qty),
                            received_ts: Some(received_ts),
                        };
                        Ok(Some((symbol, md)))
                    } else {
                        Ok(None)
                    }
                }
                // spot endpoint also sends text acks/pongs sometimes; ignore
                _ => Ok(None),
            },

            InstrumentType::Perp => {
                // Futures depth is JSON text per docs. :contentReference[oaicite:8]{index=8}
                let WireMessage::Text(text) = msg else {
                    // If you still receive binary here, it’s likely compressed. You can either:
                    // 1) keep compress=false as above, or
                    // 2) add decompression logic with flate2.
                    return Ok(None);
                };
                // Quick parse just for channel
                let v: serde_json::Value = match serde_json::from_str(text) {
                    Ok(v) => v,
                    Err(_) => return Ok(None),
                };

                let Some(channel) = v.get("channel").and_then(|c| c.as_str()) else {
                    return Ok(None);
                };
                if channel != "push.depth" && channel != "push.depth.step" {
                    return Ok(None);
                }
                // Ignore non-depth pushes (ticker, deal, etc.) unless you want them later.
                let depth = match serde_json::from_str::<MexcFuturesDepthMsg>(text) {
                    Ok(v) => v,
                    Err(e) => {
                        return {
                            error!("Mexc couldn't parse message {} {}", &text, e);
                            Ok(None)
                        };
                    }
                };

                // Update order book for this symbol
                let book = match self.books.get(&depth.symbol) {
                    Some(book) => Arc::clone(book),
                    None => return Ok(None), // or insert, depending on your design
                };

                let mut book = book.lock().unwrap();

                book.update_bids(depth_levels_to_updates(&depth.data.bids));
                book.update_asks(depth_levels_to_updates(&depth.data.asks));

                // Derive BBO
                let (bid, bid_qty) = match book.best_bid() {
                    Some(v) => v,
                    None => return Ok(None),
                };
                let (ask, ask_qty) = match book.best_ask() {
                    Some(v) => v,
                    None => return Ok(None),
                };

                // Basic sanity
                if bid >= ask {
                    // Can happen transiently if updates arrive out of order / partial
                    return Ok(None);
                }

                let md = MarketData {
                    bid: Some(bid),
                    ask: Some(ask),
                    bid_qty: Some(bid_qty),
                    ask_qty: Some(ask_qty),
                    received_ts: Some(received_ts),
                };

                Ok(Some((depth.symbol, md)))
            }
            _ => {
                anyhow::bail!("Unsupported asset class {:?}", self.market)
            }
        }
    }
}

pub async fn listen_spot_bbo(
    data: Arc<Mutex<MarketDataCollection>>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let feed = Arc::new(MexcFeed::new_spot(symbols));
    listen_with_reconnect(
        data,
        symbols,
        feed,
        "mexc_spot",
        ConnectionConfig::default(),
        shutdown,
    )
    .await
}

pub async fn listen_perp_bbo(
    data: Arc<Mutex<MarketDataCollection>>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let feed = Arc::new(MexcFeed::new_perp(symbols));
    listen_with_reconnect(
        data,
        symbols,
        feed,
        "mexc_perp",
        ConnectionConfig::default(),
        shutdown,
    )
    .await
}
