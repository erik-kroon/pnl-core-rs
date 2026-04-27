use crate::position::PositionKey;
use crate::state_hash::StateHash;
use crate::types::{AccountId, CurrencyId, Money, Ratio};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyReceipt {
    pub sequence: u64,
    pub changed_accounts: Vec<AccountId>,
    pub changed_positions: Vec<PositionKey>,
    pub realized_pnl_delta: Money,
    pub cash_delta: Money,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplainedApplyReceipt {
    pub receipt: ApplyReceipt,
    pub account_changes: Vec<AccountChangeExplanation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountChangeExplanation {
    pub account_id: AccountId,
    pub base_currency: CurrencyId,
    pub before_cash: Money,
    pub after_cash: Money,
    pub cash_delta: Money,
    pub before_position_market_value: Money,
    pub after_position_market_value: Money,
    pub position_market_value_delta: Money,
    pub before_equity: Money,
    pub after_equity: Money,
    pub equity_delta: Money,
    pub before_realized_pnl: Money,
    pub after_realized_pnl: Money,
    pub realized_pnl_delta: Money,
    pub before_unrealized_pnl: Money,
    pub after_unrealized_pnl: Money,
    pub unrealized_pnl_delta: Money,
    pub before_total_pnl: Money,
    pub after_total_pnl: Money,
    pub total_pnl_delta: Money,
    pub before_net_external_cash_flows: Money,
    pub after_net_external_cash_flows: Money,
    pub net_external_cash_flows_delta: Money,
    pub before_pnl_reconciliation_delta: Money,
    pub after_pnl_reconciliation_delta: Money,
    pub pnl_reconciliation_delta_change: Money,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayReport {
    pub applied: u64,
    pub last_sequence: u64,
    pub state_hash: StateHash,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountSummary {
    pub account_id: AccountId,
    pub base_currency: CurrencyId,
    pub cash: Money,
    pub position_market_value: Money,
    pub equity: Money,
    pub trading_realized_pnl: Money,
    pub interest_pnl: Money,
    pub borrow_pnl: Money,
    pub funding_pnl: Money,
    pub financing_pnl: Money,
    pub total_financing_pnl: Money,
    pub realized_pnl: Money,
    pub unrealized_pnl: Money,
    pub total_pnl: Money,
    pub gross_exposure: Money,
    pub net_exposure: Money,
    pub leverage: Option<Ratio>,
    pub peak_equity: Money,
    pub current_drawdown: Money,
    pub max_drawdown: Money,
    pub open_positions: u32,
    pub net_external_cash_flows: Money,
    pub pnl_reconciliation_delta: Money,
    pub state_hash: StateHash,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountReconciliation {
    pub account_id: AccountId,
    pub base_currency: CurrencyId,
    pub initial_cash: Money,
    pub cash: Money,
    pub net_external_cash_flows: Money,
    pub position_market_value: Money,
    pub equity: Money,
    pub realized_pnl: Money,
    pub unrealized_pnl: Money,
    pub total_pnl: Money,
    pub expected_pnl_from_equity: Money,
    pub pnl_reconciliation_delta: Money,
    pub state_hash: StateHash,
}
