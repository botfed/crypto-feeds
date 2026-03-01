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
        let (base, quote) = if parts.len() == 3 {
            (parts[1], parts[2]) // SPOT_BTC_USDT
        } else {
            (parts[0], parts[1]) // BTC_USDT
        };
        match itype {
            InstrumentType::Spot => Ok(format!("{}/{}", base, quote).to_uppercase()),
            InstrumentType::Perp => {
                // Kraken Futures uses XBT instead of BTC
                let base = if base.eq_ignore_ascii_case("BTC") { "XBT" } else { base };
                Ok(format!("PI_{}{}", base, quote).to_uppercase())
            }
            _ => anyhow::bail!("Type not implemented {:?}", itype),
        }
    }
    fn parse(&self, native: &str, itype: InstrumentType) -> Result<(String, String)> {
        match itype {
            InstrumentType::Spot => {
                let parts: Vec<&str> = native.split('/').collect();
                let base = parts[0];
                let quote = parts[1];
                Ok((base.to_string(), quote.to_string()))
            }
            InstrumentType::Perp => {
                // PI_XBTUSD -> ("BTC", "USD")
                let stripped = native
                    .strip_prefix("PI_")
                    .unwrap_or(native);
                for quote in &["USDT", "USDC", "USD"] {
                    if let Some(base) = stripped.strip_suffix(quote) {
                        // XBT -> BTC for registry compatibility
                        let base = if base == "XBT" { "BTC" } else { base };
                        return Ok((base.to_string(), quote.to_string()));
                    }
                }
                anyhow::bail!("Could not parse Kraken futures symbol: {}", native)
            }
            _ => {
                anyhow::bail!(
                    "Unknown asset class {}, could not parse Kraken symbol: {}",
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
