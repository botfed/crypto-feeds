use crate::ring_buffer::RingBuffer;
use crate::symbol_registry::{MAX_SYMBOLS, SymbolId};
use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::sync::OnceLock;

#[derive(Debug, Default, Copy, Clone)]
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

pub struct MarketDataCollection {
    buffers: Box<[OnceLock<Box<RingBuffer<MarketData>>>]>,
}

// Debug impl since OnceLock<Box<RingBuffer>> doesn't derive Debug
impl std::fmt::Debug for MarketDataCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MarketDataCollection")
            .field("capacity", &self.buffers.len())
            .finish()
    }
}

pub struct AllMarketData {
    pub binance: Arc<MarketDataCollection>,
    pub coinbase: Arc<MarketDataCollection>,
    pub bybit: Arc<MarketDataCollection>,
    pub kraken: Arc<MarketDataCollection>,
    pub lighter: Arc<MarketDataCollection>,
    pub mexc: Arc<MarketDataCollection>,
    pub extended: Arc<MarketDataCollection>,
    pub nado: Arc<MarketDataCollection>,
    pub okx: Arc<MarketDataCollection>,
    pub kucoin: Arc<MarketDataCollection>,
    pub bingx: Arc<MarketDataCollection>,
    pub apex: Arc<MarketDataCollection>,
    pub hyperliquid: Arc<MarketDataCollection>,
    pub aerodrome: Arc<MarketDataCollection>,
    pub uniswap: Arc<MarketDataCollection>,
}

// Debug impl
impl std::fmt::Debug for AllMarketData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AllMarketData").finish()
    }
}

#[derive(Clone, Copy)]
pub enum Exchange {
    Binance,
    Coinbase,
    Bybit,
    Kraken,
    Lighter,
    Mexc,
    Extended,
    Nado,
    Okx,
    Kucoin,
    Bingx,
    Apex,
    Hyperliquid,
    Aerodrome,
    Uniswap,
}

impl Exchange {
    pub fn as_str(&self) -> &'static str {
        match self {
            Exchange::Binance => "binance",
            Exchange::Coinbase => "coinbase",
            Exchange::Bybit => "bybit",
            Exchange::Kraken => "kraken",
            Exchange::Lighter => "lighter",
            Exchange::Mexc => "mexc",
            Exchange::Extended => "extended",
            Exchange::Nado => "nado",
            Exchange::Okx => "okx",
            Exchange::Kucoin => "kucoin",
            Exchange::Bingx => "bingx",
            Exchange::Apex => "apex",
            Exchange::Hyperliquid => "hyperliquid",
            Exchange::Aerodrome => "aerodrome",
            Exchange::Uniswap => "uniswap",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "binance" => Some(Exchange::Binance),
            "coinbase" => Some(Exchange::Coinbase),
            "bybit" => Some(Exchange::Bybit),
            "kraken" => Some(Exchange::Kraken),
            "lighter" => Some(Exchange::Lighter),
            "mexc" => Some(Exchange::Mexc),
            "extended" => Some(Exchange::Extended),
            "nado" => Some(Exchange::Nado),
            "okx" => Some(Exchange::Okx),
            "kucoin" => Some(Exchange::Kucoin),
            "bingx" => Some(Exchange::Bingx),
            "apex" => Some(Exchange::Apex),
            "hyperliquid" => Some(Exchange::Hyperliquid),
            "aerodrome" => Some(Exchange::Aerodrome),
            "uniswap" => Some(Exchange::Uniswap),
            _ => None,
        }
    }
}

impl AllMarketData {
    pub fn iter(&self) -> impl Iterator<Item = (Exchange, &Arc<MarketDataCollection>)> {
        use Exchange::*;
        [
            (Binance, &self.binance),
            (Coinbase, &self.coinbase),
            (Bybit, &self.bybit),
            (Kraken, &self.kraken),
            (Lighter, &self.lighter),
            (Mexc, &self.mexc),
            (Extended, &self.extended),
            (Nado, &self.nado),
            (Okx, &self.okx),
            (Kucoin, &self.kucoin),
            (Bingx, &self.bingx),
            (Apex, &self.apex),
            (Hyperliquid, &self.hyperliquid),
            (Aerodrome, &self.aerodrome),
            (Uniswap, &self.uniswap),
        ]
        .into_iter()
    }

    pub fn get_collection(&self, exchange: &Exchange) -> &Arc<MarketDataCollection> {
        match exchange {
            Exchange::Binance => &self.binance,
            Exchange::Coinbase => &self.coinbase,
            Exchange::Bybit => &self.bybit,
            Exchange::Kraken => &self.kraken,
            Exchange::Lighter => &self.lighter,
            Exchange::Mexc => &self.mexc,
            Exchange::Extended => &self.extended,
            Exchange::Nado => &self.nado,
            Exchange::Okx => &self.okx,
            Exchange::Kucoin => &self.kucoin,
            Exchange::Bingx => &self.bingx,
            Exchange::Apex => &self.apex,
            Exchange::Hyperliquid => &self.hyperliquid,
            Exchange::Aerodrome => &self.aerodrome,
            Exchange::Uniswap => &self.uniswap,
        }
    }
}

impl AllMarketData {
    pub fn new() -> Self {
        let new_coll = || Arc::new(MarketDataCollection::new());
        Self {
            binance: new_coll(),
            bybit: new_coll(),
            coinbase: new_coll(),
            kraken: new_coll(),
            lighter: new_coll(),
            mexc: new_coll(),
            extended: new_coll(),
            nado: new_coll(),
            okx: new_coll(),
            kucoin: new_coll(),
            bingx: new_coll(),
            apex: new_coll(),
            hyperliquid: new_coll(),
            aerodrome: new_coll(),
            uniswap: new_coll(),
        }
    }
}

impl MarketDataCollection {
    pub fn new() -> Self {
        let mut buffers = Vec::with_capacity(MAX_SYMBOLS);
        for _ in 0..MAX_SYMBOLS {
            buffers.push(OnceLock::new());
        }
        Self {
            buffers: buffers.into_boxed_slice(),
        }
    }

    /// Push a new tick for the given symbol (interior-mutable, no &mut self needed).
    pub fn push(&self, id: &SymbolId, market_data: MarketData) {
        let ring = self.buffers[*id].get_or_init(|| Box::new(RingBuffer::new()));
        ring.push(market_data);
    }

    /// Get the latest tick for a symbol (owned copy via seqlock read).
    pub fn latest(&self, id: &SymbolId) -> Option<MarketData> {
        self.buffers[*id].get()?.latest()
    }

    /// Direct access to the underlying ring buffer for a symbol.
    pub fn get_buffer(&self, id: &SymbolId) -> Option<&RingBuffer<MarketData>> {
        self.buffers[*id].get().map(|b| b.as_ref())
    }

    pub fn get_midquote(&self, id: &SymbolId) -> Option<f64> {
        let md = self.latest(id)?;
        let bid = md.bid?;
        let ask = md.ask?;
        Some((bid + ask) / 2.0)
    }

    pub fn get_midquote_w_timestamps(
        &self,
        id: &SymbolId,
    ) -> Option<(f64, Option<DateTime<Utc>>, Option<DateTime<Utc>>)> {
        let md = self.latest(id)?;
        let mid = (md.bid? + md.ask?) / 2.0;
        Some((mid, md.received_ts, md.exchange_ts))
    }
}
