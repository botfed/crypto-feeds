use crate::mappers::symbol_mapper::SymbolMapper;
use crate::market_data::InstrumentType;
use anyhow::Result;

#[derive(Clone)]
pub struct NadoMapper;

impl SymbolMapper for NadoMapper {
    fn normalize(&self, native: &str, itype: InstrumentType) -> Result<String> {
        let (base, quote) = self.parse(native, itype)?;
        Ok(format!("{}_{}_{}", itype.as_str(), base, quote))
    }

    fn denormalize(&self, normalized: &str, itype: InstrumentType) -> Result<String> {
        let parts: Vec<&str> = normalized.split('_').collect();
        let (base, _quote) = if parts.len() == 3 {
            (parts[1], parts[2]) // PERP_BTC_USDT
        } else if parts.len() == 2 {
            (parts[0], parts[1]) // BTC_USDT
        } else {
            anyhow::bail!("Invalid normalized symbol: {}", normalized);
        };
        match itype {
            // Nado uses "BTC-PERP" as the base in their pairs
            InstrumentType::Perp => Ok(format!("{}-PERP", base).to_uppercase()),
            _ => anyhow::bail!("Nado only supports perp, got {:?}", itype),
        }
    }

    fn parse(&self, native: &str, itype: InstrumentType) -> Result<(String, String)> {
        match itype {
            InstrumentType::Perp => {
                // Native ticker_id: "BTC-PERP_USDT0"
                // Or just base: "BTC-PERP"
                let ticker = native.strip_suffix("_USDT0").unwrap_or(native);
                let base = ticker.strip_suffix("-PERP")
                    .ok_or_else(|| anyhow::anyhow!("Could not parse Nado perp symbol: {}", native))?;
                // All Nado perps settle in USDT0, map to USDT
                Ok((base.to_uppercase(), "USDT".to_string()))
            }
            _ => anyhow::bail!("Nado only supports perp, got {:?}", itype),
        }
    }

    fn exchange(&self) -> &str {
        "nado"
    }
}
