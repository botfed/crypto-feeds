use crate::mappers::symbol_mapper::SymbolMapper; // Import from sibling module
use crate::market_data::InstrumentType;
use anyhow::Result;

#[derive(Clone)]
pub struct KrakenMapper;

impl SymbolMapper for KrakenMapper {
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
            InstrumentType::Spot => Ok(format!("{}/{}", base, quote).to_uppercase()),
            InstrumentType::Perp => Ok(format!("PI_{}{}", base, quote).to_uppercase()),
            _ => anyhow::bail!("Type not implemented {:?}", itype),
        }
    }
    fn parse(&self, native: &str, itype: InstrumentType) -> Result<(String, String)> {
        match itype {
            InstrumentType::Spot => {
                let parts: Vec<&str> = native.split('/').collect();
                // parts = ["ETH", "USDT"]
                let base = parts[0]; // "ETH"
                let quote = parts[1]; // "USDT"
                return Ok((base.to_string(), quote.to_string()));
            }

            _ => {
                anyhow::bail!(
                    "Unknown asset class {}, could not parse Binance symbol: {}",
                    itype.as_str(),
                    native
                )
            }
        }
    }
    fn exchange(&self) -> &str {
        "kraken"
    }
}
