use anyhow::{Result, Context};
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

use crate::exchange_fees::{ExchangeFees, FeeSchedule};
use crate::exchanges::connection::{
    ConnectionConfig, ExchangeFeed, WireMessage, listen_with_reconnect,
};
use crate::mappers::{ExtendedMapper, SymbolMapper};
use crate::market_data::{InstrumentType, MarketData, MarketDataCollection};
use crate::orderbook::SyncBook;

pub fn get_fees() -> ExchangeFees {
    // 0 bps maker, 2.5 bps taker
    ExchangeFees::new(FeeSchedule::new(0.0, 0.0), FeeSchedule::new(2.5, 0.0))
}

struct ExtendedFeed {
    /// Per-symbol orderbooks (single-writer: one WS task).
    books: HashMap<String, SyncBook>,
    /// Map from native market name (e.g. "BTC-USD") back to config symbol (e.g. "BTC_USD")
    native_to_config: HashMap<String, String>,
    itype: InstrumentType,
    mapper: ExtendedMapper,
}

impl ExtendedFeed {
    fn new_perp(normalized_symbols: &[&str]) -> Result<Self> {
        let itype = InstrumentType::Perp;
        let mapper = ExtendedMapper;

        let mut native_to_config: HashMap<String, String> = HashMap::new();
        let mut books: HashMap<String, SyncBook> = HashMap::new();

        for &sym in normalized_symbols {
            let native = mapper.denormalize(sym, itype)?;
            native_to_config.insert(native, sym.to_string());
            books.insert(sym.to_string(), SyncBook::new());
        }

        Ok(Self {
            books,
            native_to_config,
            itype,
            mapper,
        })
    }
}

#[derive(Debug, Deserialize)]
struct ExtendedOrderBookMsg {
    #[serde(default)]
    market: Option<String>,
    #[serde(default)]
    data: Option<OrderBookData>,
    // Also handle flat format where bid/ask are at top level
    #[serde(default)]
    bid: Option<Vec<PriceLevel>>,
    #[serde(default)]
    ask: Option<Vec<PriceLevel>>,
}

#[derive(Debug, Deserialize)]
struct OrderBookData {
    #[serde(default)]
    bid: Vec<PriceLevel>,
    #[serde(default)]
    ask: Vec<PriceLevel>,
}

#[derive(Debug, Deserialize)]
struct PriceLevel {
    price: String,
    qty: String,
}

#[async_trait::async_trait]
impl ExchangeFeed for ExtendedFeed {
    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }

    fn extra_headers(&self) -> Vec<(&str, &str)> {
        vec![("User-Agent", "crypto-feeds/0.1")]
    }

    fn build_url(&self, _symbols: &[&str]) -> Result<String> {
        Ok("wss://api.starknet.extended.exchange/stream.extended.exchange/v1".to_string())
    }

    async fn send_subscription(
        &self,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        symbols: &[&str],
    ) -> Result<()> {
        for symbol in symbols {
            let native = self.mapper.denormalize(symbol, self.itype)?;
            let subscribe_msg = json!({
                "type": "subscribe",
                "stream": "orderbook",
                "market": native,
            });
            debug!("Extended sub: {}", subscribe_msg);
            write
                .send(Message::Text(subscribe_msg.to_string().into()))
                .await
                .with_context(|| format!("failed to subscribe to Extended {}", native))?;
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

        // Fast-path: skip messages that don't look like orderbook data
        if !text.contains("bid") && !text.contains("ask") {
            return Ok(None);
        }

        let ob: ExtendedOrderBookMsg = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };

        let market = match &ob.market {
            Some(m) => m.as_str(),
            None => return Ok(None),
        };

        let config_sym = match self.native_to_config.get(market) {
            Some(s) => s.as_str(),
            None => return Ok(None),
        };

        // Get bid/ask arrays from either nested `data` or flat top-level
        let (bids, asks) = match &ob.data {
            Some(data) => (&data.bid, &data.ask),
            None => match (&ob.bid, &ob.ask) {
                (Some(b), Some(a)) => (b, a),
                _ => return Ok(None),
            },
        };

        let Some(book_cell) = self.books.get(config_sym) else {
            return Ok(None);
        };

        // SAFETY: single writer - one WS task per feed.
        let book = unsafe { book_cell.get_mut() };

        if !bids.is_empty() {
            book.update_bids(
                bids.iter()
                    .map(|l| (l.price.clone(), l.qty.parse::<f64>().unwrap_or(0.0)))
                    .collect(),
            );
        }
        if !asks.is_empty() {
            book.update_asks(
                asks.iter()
                    .map(|l| (l.price.clone(), l.qty.parse::<f64>().unwrap_or(0.0)))
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

        if bid.is_none() || ask.is_none() {
            return Ok(None);
        }

        if let (Some(b), Some(a)) = (bid, ask) {
            if b >= a {
                return Ok(None);
            }
        }

        let md = MarketData {
            bid,
            ask,
            bid_qty,
            ask_qty,
            exchange_ts: None,
            received_ts: Some(received_ts),
        };

        Ok(Some((config_sym.to_string(), md)))
    }
}

pub async fn listen_perp_bbo(
    data: Arc<MarketDataCollection>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let feed = Arc::new(ExtendedFeed::new_perp(symbols)?);

    listen_with_reconnect(
        data,
        symbols,
        feed,
        "extended_perp",
        ConnectionConfig::default(),
        shutdown,
    )
    .await
}
