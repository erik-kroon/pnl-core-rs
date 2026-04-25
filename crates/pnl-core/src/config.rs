use crate::types::{AccountingMethod, CurrencyId, RoundingMode, ACCOUNT_MONEY_SCALE};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineConfig {
    pub base_currency: CurrencyId,
    pub account_money_scale: u8,
    pub rounding_mode: RoundingMode,
    pub accounting_method: AccountingMethod,
    pub cash_authoritative: bool,
    pub allow_short: bool,
    pub allow_position_flip: bool,
    pub expected_start_seq: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            base_currency: CurrencyId::usd(),
            account_money_scale: ACCOUNT_MONEY_SCALE,
            rounding_mode: RoundingMode::HalfEven,
            accounting_method: AccountingMethod::AverageCost,
            cash_authoritative: true,
            allow_short: true,
            allow_position_flip: true,
            expected_start_seq: 1,
        }
    }
}
