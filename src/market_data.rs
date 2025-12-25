use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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

#[derive(Debug)]
pub struct MarketData {
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub bid_qty: Option<f64>,
    pub ask_qty: Option<f64>,
    pub received_ts: Option<DateTime<Utc>>,
}

impl MarketData {
    pub fn midquote(&self) -> Option<f64> {
        return Some((self.bid? + self.ask?) / 2.0);
    }
}

#[derive(Debug, Default)]
pub struct MarketDataCollection {
    pub data: HashMap<String, MarketData>,
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
            data: HashMap::new(),
        }
    }

    pub fn insert(&mut self, symbol: String, market_data: MarketData) {
        self.data.insert(symbol, market_data);
    }

    pub fn get(&self, symbol: &str) -> Option<&MarketData> {
        self.data.get(symbol)
    }

    pub fn get_midquote(&self, symbol: &str) -> Option<f64> {
        let market_data = self.data.get(symbol)?;
        let bid = market_data.bid?;
        let ask = market_data.ask?;
        Some((bid + ask) / 2.0)
    }
    pub fn get_midquote_w_timestamp(&self, symbol: &str) -> Option<(f64, DateTime<Utc>)> {
        let mid = self.get_midquote(symbol)?;
        let received_ts = self.data.get(symbol)?.received_ts?;
        Some((mid, received_ts))
    }
}
