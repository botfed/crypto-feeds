use crate::mappers::symbol_mapper::SymbolMapper;
use crate::market_data::InstrumentType;
use anyhow::Result;

#[derive(Clone)]
pub struct HotstuffMapper;

impl SymbolMapper for HotstuffMapper {
    fn normalize(&self, native: &str, itype: InstrumentType) -> Result<String> {
        let (base, quote) = self.parse(native, itype)?;
        Ok(format!("{}_{}_{}", itype.as_str(), base, quote))
    }

    fn denormalize(&self, normalized: &str, itype: InstrumentType) -> Result<String> {
        let parts: Vec<&str> = normalized.split('_').collect();
        let base = if parts.len() == 3 {
            parts[1]
        } else if parts.len() == 2 {
            parts[0]
        } else {
            anyhow::bail!("Invalid normalized symbol: {}", normalized);
        };
        match itype {
            InstrumentType::Perp => Ok(format!("{}-PERP", base.to_uppercase())),
            _ => anyhow::bail!("Hotstuff only supports perp, got {:?}", itype),
        }
    }

    fn parse(&self, native: &str, _itype: InstrumentType) -> Result<(String, String)> {
        // BTC-PERP -> (BTC, USDT)
        let base = native
            .strip_suffix("-PERP")
            .ok_or_else(|| anyhow::anyhow!("Could not parse hotstuff symbol: {}", native))?;
        Ok((base.to_uppercase(), "USDT".to_string()))
    }

    fn exchange(&self) -> &str {
        "hotstuff"
    }
}
