use crate::symbol_registry::{MAX_SYMBOLS, SymbolId};
use chrono::{DateTime, Utc};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[repr(C)]
pub struct MarketDataAtomic {
    bid: AtomicU64,
    ask: AtomicU64,
    bid_qty: AtomicU64,
    ask_qty: AtomicU64,
    exchange_ts: AtomicU64,
    received_ts: AtomicU64,
}

#[derive(Debug, Copy, Clone)]
pub struct MarketData {
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub bid_qty: Option<f64>,
    pub ask_qty: Option<f64>,
    pub exchange_ts: Option<DateTime<Utc>>,
    pub received_ts: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy, Debug)]
pub enum InstrumentType {
    Spot,
    Perp,
    Option,
    Futures,
}

impl InstrumentType {
    pub fn as_str(&self) -> &'static str {
        match self {
            InstrumentType::Spot => "SPOT",
            InstrumentType::Perp => "PERP",
            InstrumentType::Option => "OPTION",
            InstrumentType::Futures => "FUT",
        }
    }
}

impl MarketData {
    pub fn midquote(&self) -> Option<f64> {
        return Some((self.bid? + self.ask?) / 2.0);
    }
}

#[derive(Debug)]
pub struct MarketDataCollection {
    // pub data: HashMap<String, MarketData>,
    pub data: [Option<MarketData>; MAX_SYMBOLS],
}

#[derive(Debug)]
pub struct AllMarketData {
    pub binance: Arc<Mutex<MarketDataCollection>>,
    pub coinbase: Arc<Mutex<MarketDataCollection>>,
    pub bybit: Arc<Mutex<MarketDataCollection>>,
    pub kraken: Arc<Mutex<MarketDataCollection>>,
    pub lighter: Arc<Mutex<MarketDataCollection>>,
    pub mexc: Arc<Mutex<MarketDataCollection>>,
}

pub enum Exchange {
    Binance,
    Coinbase,
    Bybit,
    Kraken,
    Lighter,
    Mexc,
}

impl AllMarketData {
    pub fn iter(&self) -> impl Iterator<Item = (Exchange, &Arc<Mutex<MarketDataCollection>>)> {
        use Exchange::*;
        [
            (Binance, &self.binance),
            (Coinbase, &self.coinbase),
            (Bybit, &self.bybit),
            (Kraken, &self.kraken),
            (Lighter, &self.lighter),
            (Mexc, &self.mexc),
        ]
        .into_iter()
    }
}

impl AllMarketData {
    pub fn new() -> Self {
        let new_coll = || Arc::new(Mutex::new(MarketDataCollection::new()));
        Self {
            binance: new_coll(),
            bybit: new_coll(),
            coinbase: new_coll(),
            kraken: new_coll(),
            lighter: new_coll(),
            mexc: new_coll(),
        }
    }
}

impl MarketDataCollection {
    pub fn new() -> Self {
        Self {
            data: [None; MAX_SYMBOLS],
        }
    }

    pub fn insert(&mut self, id: &SymbolId, market_data: MarketData) {
        self.data[*id] = Some(market_data);
    }

    pub fn get(&self, id: &SymbolId) -> Option<&MarketData> {
        self.data[*id].as_ref()
    }

    pub fn get_midquote(&self, id: &SymbolId) -> Option<f64> {
        if let Some(md) = self.get(id) {
            let bid = md.bid?;
            let ask = md.ask?;
            return Some((bid + ask) / 2.0);
        }
        None
    }
    pub fn get_midquote_w_timestamps(
        &self,
        id: &SymbolId,
    ) -> Option<(f64, Option<DateTime<Utc>>, Option<DateTime<Utc>>)> {
        if let Some(md) = self.get(id) {
            let mid = (md.bid? + md.ask?) / 2.0;
            return Some((mid, md.received_ts, md.exchange_ts));
        }
        None
    }
}
