use crate::mappers::symbol_mapper::SymbolMapper;
use crate::market_data::InstrumentType;
use anyhow::Result;

#[derive(Clone)]
pub struct OkxMapper;

impl SymbolMapper for OkxMapper {
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
            InstrumentType::Spot => Ok(format!("{}-{}", base, quote).to_uppercase()),
            InstrumentType::Perp => Ok(format!("{}-{}-SWAP", base, quote).to_uppercase()),
            _ => anyhow::bail!("Type not implemented {:?}", itype),
        }
    }
    fn parse(&self, native: &str, itype: InstrumentType) -> Result<(String, String)> {
        match itype {
            InstrumentType::Spot => {
                // OKX spot: "BTC-USDT"
                let parts: Vec<&str> = native.split('-').collect();
                if parts.len() == 2 {
                    Ok((parts[0].to_uppercase(), parts[1].to_uppercase()))
                } else {
                    anyhow::bail!("Could not parse OKX spot symbol: {}", native)
                }
            }
            InstrumentType::Perp => {
                // OKX perp: "BTC-USDT-SWAP"
                let parts: Vec<&str> = native.split('-').collect();
                if parts.len() == 3 && parts[2].eq_ignore_ascii_case("SWAP") {
                    Ok((parts[0].to_uppercase(), parts[1].to_uppercase()))
                } else {
                    anyhow::bail!("Could not parse OKX perp symbol: {}", native)
                }
            }
            _ => anyhow::bail!("Unsupported itype {:?}", itype),
        }
    }
    fn exchange(&self) -> &str {
        "okx"
    }
}
