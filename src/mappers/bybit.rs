use crate::mappers::symbol_mapper::SymbolMapper; // Import from sibling module
use crate::market_data::InstrumentType;
use anyhow::Result;

#[derive(Clone)]
pub struct BybitMapper;

impl SymbolMapper for BybitMapper {
    fn normalize(&self, native: &str, itype: InstrumentType) -> Result<String> {
        let (base, quote) = self.parse(native, itype)?;
        Ok(format!("{}_{}_{}", itype.as_str(), base, quote))
    }
    fn denormalize(&self, normalized: &str, itype: InstrumentType) -> Result<String> {
        let parts: Vec<&str> = normalized.split('_').collect();
        if parts.len() < 2 {
            anyhow::bail!("Invalid normalized symbol: {}", normalized);
        }
        // Assume already stripped of SPOT_ prefix, or handle it:
        let (base, quote) = if parts.len() == 3 {
            (parts[1], parts[2]) // SPOT_BTC_USDT
        } else {
            (parts[0], parts[1]) // BTC_USDT
        };
        match itype {
            InstrumentType::Spot => Ok(format!("{}{}", base, quote).to_uppercase()),
            InstrumentType::Perp => Ok(format!("{}{}", base, quote).to_uppercase()),
            _ => anyhow::bail!("Type not implemented {:?}", itype),
        }
    }
    fn parse(&self, native: &str, itype: InstrumentType) -> Result<(String, String)> {
        match itype {
            InstrumentType::Spot | InstrumentType::Perp => {
                // Known quote currencies in priority order (longest first)
                const QUOTES: &[&str] = &["USDT", "USDC", "BUSD", "USD", "BTC", "ETH", "BNB"];

                let upper = native.to_uppercase();

                for quote in QUOTES {
                    if let Some(base) = upper.strip_suffix(quote) {
                        if !base.is_empty() {
                            return Ok((base.to_string(), quote.to_string()));
                        }
                    }
                }
                anyhow::bail!("Could not parse binance symbol: {}", native)
            }
            _ => {
                anyhow::bail!("Unsupported itype {:?}", itype)
            }
        }
    }
    fn exchange(&self) -> &str {
        "binance"
    }
}
