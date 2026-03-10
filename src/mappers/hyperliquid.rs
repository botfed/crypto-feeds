use crate::mappers::symbol_mapper::SymbolMapper;
use crate::market_data::InstrumentType;
use anyhow::Result;

#[derive(Clone)]
pub struct HyperliquidMapper;

impl SymbolMapper for HyperliquidMapper {
    fn normalize(&self, native: &str, itype: InstrumentType) -> Result<String> {
        // Hyperliquid native symbols are just coin names: "BTC", "ETH"
        // Normalize to "PERP_BTC_USD" (Hyperliquid perps are USD-denominated)
        Ok(format!("{}_{}_USD", itype.as_str(), native.to_uppercase()))
    }

    fn denormalize(&self, normalized: &str, _itype: InstrumentType) -> Result<String> {
        // "BTC_USDT" → "BTC" (just the base coin)
        let parts: Vec<&str> = normalized.split('_').collect();
        let base = if parts.len() == 3 {
            parts[1] // PERP_BTC_USDT
        } else if parts.len() == 2 {
            parts[0] // BTC_USDT
        } else {
            anyhow::bail!("Invalid normalized symbol: {}", normalized);
        };
        Ok(base.to_uppercase())
    }

    fn parse(&self, native: &str, _itype: InstrumentType) -> Result<(String, String)> {
        // Native is just the coin name, quote is implicitly USD
        Ok((native.to_uppercase(), "USD".to_string()))
    }

    fn exchange(&self) -> &str {
        "hyperliquid"
    }
}
