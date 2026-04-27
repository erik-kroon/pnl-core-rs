use crate::types::{AccountId, CurrencyId, Money};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountState {
    pub account_id: AccountId,
    pub base_currency: CurrencyId,
    pub initial_cash: Money,
    pub cash: Money,
    pub net_external_cash_flows: Money,
    pub trading_realized_pnl: Money,
    pub interest_pnl: Money,
    pub borrow_pnl: Money,
    pub funding_pnl: Money,
    pub financing_pnl: Money,
    pub total_financing_pnl: Money,
    pub realized_pnl: Money,
    pub peak_equity: Money,
    pub current_drawdown: Money,
    pub max_drawdown: Money,
    pub initial_cash_set: bool,
}
