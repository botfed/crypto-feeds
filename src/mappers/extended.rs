use crate::mappers::symbol_mapper::SymbolMapper;
use crate::market_data::InstrumentType;
use anyhow::Result;

#[derive(Clone)]
pub struct ExtendedMapper;

impl SymbolMapper for ExtendedMapper {
    fn normalize(&self, native: &str, itype: InstrumentType) -> Result<String> {
        let (base, quote) = self.parse(native, itype)?;
        Ok(format!("{}_{}_{}", itype.as_str(), base, quote))
    }

    fn denormalize(&self, normalized: &str, itype: InstrumentType) -> Result<String> {
        let parts: Vec<&str> = normalized.split('_').collect();
        let (base, quote) = if parts.len() == 3 {
            (parts[1], parts[2]) // PERP_BTC_USD
        } else if parts.len() == 2 {
            (parts[0], parts[1]) // BTC_USD
        } else {
            anyhow::bail!("Invalid normalized symbol: {}", normalized);
        };
        match itype {
            InstrumentType::Perp => Ok(format!("{}-{}", base, quote).to_uppercase()),
            _ => anyhow::bail!("Extended only supports perp, got {:?}", itype),
        }
    }

    fn parse(&self, native: &str, _itype: InstrumentType) -> Result<(String, String)> {
        // Native format: "BTC-USD"
        let parts: Vec<&str> = native.split('-').collect();
        if parts.len() != 2 {
            anyhow::bail!("Could not parse Extended symbol: {}", native);
        }
        Ok((parts[0].to_uppercase(), parts[1].to_uppercase()))
    }

    fn exchange(&self) -> &str {
        "extended"
    }
}
