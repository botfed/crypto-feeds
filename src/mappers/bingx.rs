use crate::mappers::symbol_mapper::SymbolMapper;
use crate::market_data::InstrumentType;
use anyhow::Result;

#[derive(Clone)]
pub struct BingxMapper;

impl SymbolMapper for BingxMapper {
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
            (parts[1], parts[2])
        } else {
            (parts[0], parts[1])
        };
        match itype {
            InstrumentType::Spot | InstrumentType::Perp => {
                Ok(format!("{}-{}", base, quote).to_uppercase())
            }
            _ => anyhow::bail!("Type not implemented {:?}", itype),
        }
    }
    fn parse(&self, native: &str, itype: InstrumentType) -> Result<(String, String)> {
        match itype {
            InstrumentType::Spot | InstrumentType::Perp => {
                // BingX: "BTC-USDT"
                let parts: Vec<&str> = native.split('-').collect();
                if parts.len() == 2 {
                    Ok((parts[0].to_uppercase(), parts[1].to_uppercase()))
                } else {
                    anyhow::bail!("Could not parse BingX symbol: {}", native)
                }
            }
            _ => anyhow::bail!("Unsupported itype {:?}", itype),
        }
    }
    fn exchange(&self) -> &str {
        "bingx"
    }
}
