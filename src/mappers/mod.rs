mod binance;
mod mexc;
mod symbol_mapper;
mod coinbase;
mod bybit;
mod kraken;
mod lighter;

// Re-export the trait
pub use symbol_mapper::{SymbolMapper, parse_normalized};

// Re-export implementations
pub use binance::BinanceMapper;
pub use mexc::MexcMapper;
pub use coinbase::CoinbaseMapper;
pub use bybit::BybitMapper;
pub use kraken::KrakenMapper;
pub use lighter::LighterMapper;

use anyhow::Result;

/// Factory function to create a mapper for a given exchange
pub fn get_mapper(exchange: &str) -> Result<Box<dyn SymbolMapper>> {
    match exchange.to_lowercase().as_str() {
        "mexc" => Ok(Box::new(MexcMapper)),
        "binance" => Ok(Box::new(BinanceMapper)),
        "coinbase" => Ok(Box::new(CoinbaseMapper)),
        "bybit" => Ok(Box::new(BybitMapper)),
        "kraken" => Ok(Box::new(KrakenMapper)),
        "lighter" => Ok(Box::new(LighterMapper)),
        _ => anyhow::bail!("Unsupported exchange: {}", exchange),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_mapper() {
        assert!(get_mapper("mexc").is_ok());
        assert!(get_mapper("binance").is_ok());
        assert!(get_mapper("invalid").is_err());
    }
}
