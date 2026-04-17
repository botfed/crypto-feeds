use anyhow::Result;
use chrono::{DateTime, Utc};
use log::error;
use serde::Deserialize;
use std::sync::Arc;

use crate::exchange_fees::{ExchangeFees, FeeSchedule};
use crate::exchanges::connection::{
    ConnectionConfig, ExchangeFeed, WireMessage, listen_with_reconnect,
};
use crate::mappers::{ExtendedMapper, SymbolMapper};
use crate::market_data::{InstrumentType, MarketData, MarketDataCollection};

pub fn get_fees() -> ExchangeFees {
    // 0 bps maker, 2.5 bps taker
    ExchangeFees::new(FeeSchedule::new(0.0, 0.0), FeeSchedule::new(2.5, 0.0))
}

/// One feed per market — Extended uses path-based routing (one WS per symbol).
struct ExtendedBboFeed {
    /// The native market name, e.g. "BTC-USD"
    native_market: String,
    /// The config symbol we return to the registry, e.g. "BTC_USD"
    config_sym: String,
    itype: InstrumentType,
}

// Wire format:
// {"type":"SNAPSHOT","data":{"t":"SNAPSHOT","m":"BTC-USD","b":[{"q":"17.7","p":"65219"}],"a":[{"q":"0.3","p":"65220"}],"d":"1"},"ts":...,"seq":...}

#[derive(Debug, Deserialize)]
struct BboMsg {
    data: BboData,
    ts: u64,
}

#[derive(Debug, Deserialize)]
struct BboData {
    #[serde(rename = "b")]
    bids: Vec<PriceLevel>,
    #[serde(rename = "a")]
    asks: Vec<PriceLevel>,
}

#[derive(Debug, Deserialize)]
struct PriceLevel {
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    qty: String,
}

#[async_trait::async_trait]
impl ExchangeFeed for ExtendedBboFeed {
    fn get_itype(&self) -> Result<&InstrumentType> {
        Ok(&self.itype)
    }

    fn extra_headers(&self) -> Vec<(&str, &str)> {
        vec![("User-Agent", "crypto-feeds/0.1")]
    }

    fn build_url(&self, _symbols: &[&str]) -> Result<String> {
        Ok(format!(
            "wss://api.starknet.extended.exchange/stream.extended.exchange/v1/orderbooks/{}?depth=1",
            self.native_market
        ))
    }

    fn parse_message(
        &self,
        msg: WireMessage<'_>,
        received_ts: DateTime<Utc>,
        received_instant: std::time::Instant,
    ) -> Result<Option<(String, MarketData)>> {
        let WireMessage::Text(text) = msg else {
            return Ok(None);
        };

        let ob: BboMsg = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };

        let best_bid = ob.data.bids.first();
        let best_ask = ob.data.asks.first();

        let (Some(bl), Some(al)) = (best_bid, best_ask) else {
            return Ok(None);
        };

        let (Ok(bid), Ok(bid_qty)) = (bl.price.parse::<f64>(), bl.qty.parse::<f64>()) else {
            return Ok(None);
        };
        let (Ok(ask), Ok(ask_qty)) = (al.price.parse::<f64>(), al.qty.parse::<f64>()) else {
            return Ok(None);
        };

        if bid >= ask {
            return Ok(None);
        }

        let md = MarketData {
            bid: Some(bid),
            ask: Some(ask),
            bid_qty: Some(bid_qty),
            ask_qty: Some(ask_qty),
            exchange_ts_raw: DateTime::from_timestamp_millis(ob.ts as i64),
            exchange_ts: None,
            received_ts: Some(received_ts),
            received_instant: Some(received_instant),
                    feed_latency_ns: 0,
        };

        Ok(Some((self.config_sym.clone(), md)))
    }
}

/// Spawns one WebSocket connection per symbol (Extended uses path-based routing).
pub async fn listen_perp_bbo(
    data: Arc<MarketDataCollection>,
    symbols: &[&str],
    shutdown: Arc<tokio::sync::Notify>,
) -> Result<()> {
    let mapper = ExtendedMapper;
    let itype = InstrumentType::Perp;

    let mut handles = Vec::with_capacity(symbols.len());

    for &sym in symbols {
        let native_market = mapper.denormalize(sym, itype)?;
        let config_sym = sym.to_string();

        let feed = Arc::new(ExtendedBboFeed {
            native_market: native_market.clone(),
            config_sym,
            itype,
        });

        let data = Arc::clone(&data);
        let shutdown = Arc::clone(&shutdown);
        let feed_name = format!("extended_perp_{}", native_market);
        let sym_owned = sym.to_string();

        handles.push(tokio::spawn(async move {
            let sym_refs: Vec<&str> = vec![sym_owned.as_str()];
            if let Err(e) = listen_with_reconnect(
                data,
                &sym_refs,
                feed,
                &feed_name,
                ConnectionConfig::default(),
                shutdown,
            )
            .await
            {
                error!("Extended {} listener error: {:?}", feed_name, e);
            }
        }));
    }

    // Wait for all per-symbol tasks
    for h in handles {
        let _ = h.await;
    }

    Ok(())
}
