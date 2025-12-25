use std::collections::HashMap;


#[derive(Debug, Clone, Copy)]
pub struct FeeSchedule {
    pub taker_fees_bps: f64,
    pub maker_fees_bps: f64,
}

impl FeeSchedule{
    pub fn new (taker_bps: f64, maker_bps: f64) -> Self {
        Self {
            taker_fees_bps: taker_bps,
            maker_fees_bps: maker_bps,
        }
    }
}


pub struct ExchangeFees 
{
    spot_default: FeeSchedule,
    perp_default: FeeSchedule,

    spot_overrides: HashMap<&'static str, FeeSchedule>,
    perp_overrides: HashMap<&'static str, FeeSchedule>,

}

impl ExchangeFees {
    pub fn new(spot_default: FeeSchedule, perp_default: FeeSchedule) -> Self {
        Self {
            spot_default,
            perp_default,
            spot_overrides: HashMap::new(),
            perp_overrides: HashMap::new(),
        }
    }
    
    pub fn set_spot_override(mut self, symbol: &'static str, fees: FeeSchedule) -> Self {
        self.spot_overrides.insert(symbol, fees);
        self
    }
    
    pub fn set_perp_override(mut self, symbol: &'static str, fees: FeeSchedule) -> Self {
        self.perp_overrides.insert(symbol, fees);
        self
    }
    
    pub fn get_spot_fees(&self, symbol: &str) -> &FeeSchedule {
        self.spot_overrides.get(symbol).unwrap_or(&self.spot_default)
    }
    
    pub fn get_perp_fees(&self, symbol: &str) -> &FeeSchedule {
        self.perp_overrides.get(symbol).unwrap_or(&self.perp_default)
    }
}