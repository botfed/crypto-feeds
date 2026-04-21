use crate::mappers::symbol_mapper::SymbolMapper;
use crate::market_data::InstrumentType;
use anyhow::Result;

#[derive(Clone)]
pub struct HibachiMapper;

impl SymbolMapper for HibachiMapper {
    fn normalize(&self, native: &str, itype: InstrumentType) -> Result<String> {
        let (base, quote) = self.parse(native, itype)?;
        Ok(format!("{}_{}_{}", itype.as_str(), base, quote))
    }

    fn denormalize(&self, normalized: &str, itype: InstrumentType) -> Result<String> {
        let parts: Vec<&str> = normalized.split('_').collect();
        let (base, quote) = if parts.len() == 3 {
            (parts[1], parts[2])
        } else if parts.len() == 2 {
            (parts[0], parts[1])
        } else {
            anyhow::bail!("Invalid normalized symbol: {}", normalized);
        };
        match itype {
            InstrumentType::Perp => Ok(format!("{}/{}-P", base, quote).to_uppercase()),
            InstrumentType::Spot => Ok(format!("{}/{}", base, quote).to_uppercase()),
            _ => anyhow::bail!("Unsupported itype {:?}", itype),
        }
    }

    fn parse(&self, native: &str, itype: InstrumentType) -> Result<(String, String)> {
        // Strip -P suffix for perps
        let stripped = match itype {
            InstrumentType::Perp => native.strip_suffix("-P").unwrap_or(native),
            _ => native,
        };
        // Split on /
        let parts: Vec<&str> = stripped.split('/').collect();
        if parts.len() != 2 {
            anyhow::bail!("Could not parse hibachi symbol: {}", native);
        }
        Ok((parts[0].to_uppercase(), parts[1].to_uppercase()))
    }

    fn exchange(&self) -> &str {
        "hibachi"
    }
}
