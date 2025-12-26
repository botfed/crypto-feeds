use crate::market_data::InstrumentType;
use anyhow::Result;
use once_cell::sync::Lazy;
use rustc_hash::FxHashMap;

/// Converts between exchange-native and normalized symbol formats
pub trait SymbolMapper: Send + Sync {
    fn normalize(&self, native: &str, itype: InstrumentType) -> Result<String>;
    fn denormalize(&self, normalized: &str, itype: InstrumentType) -> Result<String>;
    fn parse(&self, native: &str, itype: InstrumentType) -> Result<(String, String)>;
    fn exchange(&self) -> &str;
}

/// Helper to parse normalized symbols
pub fn parse_normalized(normalized: &str) -> Result<(String, String)> {
    let parts: Vec<&str> = normalized.split('_').collect();

    match parts.as_slice() {
        [_itype, base, quote] => Ok((base.to_string(), quote.to_string())),
        [base, quote] => Ok((base.to_string(), quote.to_string())),
        _ => anyhow::bail!("Invalid normalized symbol format: {}", normalized),
    }
}
