use crate::mappers::symbol_mapper::SymbolMapper;
use crate::market_data::InstrumentType;
use anyhow::Result;

#[derive(Clone)]
pub struct ZeroOneMapper;

impl SymbolMapper for ZeroOneMapper {
    fn normalize(&self, native: &str, itype: InstrumentType) -> Result<String> {
        let (base, quote) = self.parse(native, itype)?;
        Ok(format!("{}_{}_{}", itype.as_str(), base, quote))
    }

    fn denormalize(&self, normalized: &str, _itype: InstrumentType) -> Result<String> {
        // BTC_USDT or PERP_BTC_USDT -> BTCUSD
        let parts: Vec<&str> = normalized.split('_').collect();
        let base = if parts.len() == 3 {
            parts[1]
        } else if parts.len() == 2 {
            parts[0]
        } else {
            anyhow::bail!("Invalid normalized symbol: {}", normalized);
        };
        Ok(format!("{}USD", base.to_uppercase()))
    }

    fn parse(&self, native: &str, _itype: InstrumentType) -> Result<(String, String)> {
        // BTCUSD -> (BTC, USDT)
        // Map USD quote to USDT for registry compatibility
        let base = native
            .strip_suffix("USD")
            .ok_or_else(|| anyhow::anyhow!("Could not parse zeroone symbol: {}", native))?;
        Ok((base.to_uppercase(), "USDT".to_string()))
    }

    fn exchange(&self) -> &str {
        "zeroone"
    }
}
