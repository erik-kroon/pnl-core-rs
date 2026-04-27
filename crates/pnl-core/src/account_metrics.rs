use crate::engine::Engine;
use crate::error::Result;
use crate::state_hash::StateHash;
use crate::summary::{AccountChangeExplanation, AccountReconciliation, AccountSummary};
use crate::types::*;
use crate::valuation;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AccountMetrics {
    account_id: AccountId,
    base_currency: CurrencyId,
    cash: Money,
    position_market_value: Money,
    equity: Money,
    trading_realized_pnl: Money,
    interest_pnl: Money,
    borrow_pnl: Money,
    funding_pnl: Money,
    financing_pnl: Money,
    total_financing_pnl: Money,
    realized_pnl: Money,
    unrealized_pnl: Money,
    total_pnl: Money,
    gross_exposure: Money,
    net_exposure: Money,
    leverage: Option<Ratio>,
    peak_equity: Money,
    current_drawdown: Money,
    max_drawdown: Money,
    open_positions: u32,
    net_external_cash_flows: Money,
    expected_pnl_from_equity: Money,
    pnl_reconciliation_delta: Money,
}

impl AccountMetrics {
    pub(crate) fn compute(engine: &Engine, account_id: AccountId) -> Result<Self> {
        engine.registry.ensure_account(account_id)?;
        let account = engine.accounts.get(&account_id).unwrap();
        let totals = valuation::account_position_totals(
            engine
                .positions
                .values()
                .filter(|p| p.key.account_id == account_id),
            account.base_currency,
            engine.config.account_money_scale,
        )?;

        let equity = account.cash.checked_add(totals.position_market_value)?;
        let total_pnl = account.realized_pnl.checked_add(totals.unrealized_pnl)?;
        let leverage = if equity.amount > 0 {
            Some(Ratio::from_fraction(
                totals.gross_exposure.amount,
                equity.amount,
                ACCOUNT_RATIO_SCALE,
                engine.config.rounding_mode,
            )?)
        } else {
            None
        };
        let expected_pnl_from_equity = equity
            .checked_sub(account.initial_cash)?
            .checked_sub(account.net_external_cash_flows)?;
        let pnl_reconciliation_delta = expected_pnl_from_equity.checked_sub(total_pnl)?;

        Ok(Self {
            account_id,
            base_currency: account.base_currency,
            cash: account.cash,
            position_market_value: totals.position_market_value,
            equity,
            trading_realized_pnl: account.trading_realized_pnl,
            interest_pnl: account.interest_pnl,
            borrow_pnl: account.borrow_pnl,
            funding_pnl: account.funding_pnl,
            financing_pnl: account.financing_pnl,
            total_financing_pnl: account.total_financing_pnl,
            realized_pnl: account.realized_pnl,
            unrealized_pnl: totals.unrealized_pnl,
            total_pnl,
            gross_exposure: totals.gross_exposure,
            net_exposure: totals.net_exposure,
            leverage,
            peak_equity: account.peak_equity,
            current_drawdown: account.current_drawdown,
            max_drawdown: account.max_drawdown,
            open_positions: totals.open_positions,
            net_external_cash_flows: account.net_external_cash_flows,
            expected_pnl_from_equity,
            pnl_reconciliation_delta,
        })
    }

    pub(crate) fn equity(&self) -> Money {
        self.equity
    }

    pub(crate) fn into_summary(self, state_hash: StateHash) -> AccountSummary {
        AccountSummary {
            account_id: self.account_id,
            base_currency: self.base_currency,
            cash: self.cash,
            position_market_value: self.position_market_value,
            equity: self.equity,
            trading_realized_pnl: self.trading_realized_pnl,
            interest_pnl: self.interest_pnl,
            borrow_pnl: self.borrow_pnl,
            funding_pnl: self.funding_pnl,
            financing_pnl: self.financing_pnl,
            total_financing_pnl: self.total_financing_pnl,
            realized_pnl: self.realized_pnl,
            unrealized_pnl: self.unrealized_pnl,
            total_pnl: self.total_pnl,
            gross_exposure: self.gross_exposure,
            net_exposure: self.net_exposure,
            leverage: self.leverage,
            peak_equity: self.peak_equity,
            current_drawdown: self.current_drawdown,
            max_drawdown: self.max_drawdown,
            open_positions: self.open_positions,
            net_external_cash_flows: self.net_external_cash_flows,
            pnl_reconciliation_delta: self.pnl_reconciliation_delta,
            state_hash,
        }
    }

    pub(crate) fn into_reconciliation(
        self,
        initial_cash: Money,
        state_hash: StateHash,
    ) -> AccountReconciliation {
        AccountReconciliation {
            account_id: self.account_id,
            base_currency: self.base_currency,
            initial_cash,
            cash: self.cash,
            net_external_cash_flows: self.net_external_cash_flows,
            position_market_value: self.position_market_value,
            equity: self.equity,
            realized_pnl: self.realized_pnl,
            unrealized_pnl: self.unrealized_pnl,
            total_pnl: self.total_pnl,
            expected_pnl_from_equity: self.expected_pnl_from_equity,
            pnl_reconciliation_delta: self.pnl_reconciliation_delta,
            state_hash,
        }
    }

    pub(crate) fn explain_change(before: Self, after: Self) -> Result<AccountChangeExplanation> {
        Ok(AccountChangeExplanation {
            account_id: after.account_id,
            base_currency: after.base_currency,
            before_cash: before.cash,
            after_cash: after.cash,
            cash_delta: after.cash.checked_sub(before.cash)?,
            before_position_market_value: before.position_market_value,
            after_position_market_value: after.position_market_value,
            position_market_value_delta: after
                .position_market_value
                .checked_sub(before.position_market_value)?,
            before_equity: before.equity,
            after_equity: after.equity,
            equity_delta: after.equity.checked_sub(before.equity)?,
            before_realized_pnl: before.realized_pnl,
            after_realized_pnl: after.realized_pnl,
            realized_pnl_delta: after.realized_pnl.checked_sub(before.realized_pnl)?,
            before_unrealized_pnl: before.unrealized_pnl,
            after_unrealized_pnl: after.unrealized_pnl,
            unrealized_pnl_delta: after.unrealized_pnl.checked_sub(before.unrealized_pnl)?,
            before_total_pnl: before.total_pnl,
            after_total_pnl: after.total_pnl,
            total_pnl_delta: after.total_pnl.checked_sub(before.total_pnl)?,
            before_net_external_cash_flows: before.net_external_cash_flows,
            after_net_external_cash_flows: after.net_external_cash_flows,
            net_external_cash_flows_delta: after
                .net_external_cash_flows
                .checked_sub(before.net_external_cash_flows)?,
            before_pnl_reconciliation_delta: before.pnl_reconciliation_delta,
            after_pnl_reconciliation_delta: after.pnl_reconciliation_delta,
            pnl_reconciliation_delta_change: after
                .pnl_reconciliation_delta
                .checked_sub(before.pnl_reconciliation_delta)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EngineConfig;
    use crate::event::{Event, EventKind, Fill, InitialCash, MarkPriceUpdate};
    use crate::metadata::{AccountMeta, BookMeta, CurrencyMeta, InstrumentMeta};

    fn money(value: &str) -> Money {
        Money::parse_decimal(value, CurrencyId::usd(), ACCOUNT_MONEY_SCALE).unwrap()
    }

    fn price(value: &str) -> Price {
        Price::parse_decimal(value).unwrap()
    }

    fn setup() -> Engine {
        let mut engine = Engine::new(EngineConfig::default());
        engine
            .register_currency(CurrencyMeta {
                currency_id: CurrencyId::usd(),
                code: "USD".to_string(),
                scale: ACCOUNT_MONEY_SCALE,
            })
            .unwrap();
        engine
            .register_account(AccountMeta {
                account_id: AccountId(1),
                base_currency: CurrencyId::usd(),
            })
            .unwrap();
        engine
            .register_book(BookMeta {
                account_id: AccountId(1),
                book_id: BookId(1),
            })
            .unwrap();
        engine
            .register_instrument(InstrumentMeta {
                instrument_id: InstrumentId(1),
                symbol: "AAPL".to_string(),
                currency_id: CurrencyId::usd(),
                price_scale: 4,
                qty_scale: 0,
                multiplier: FixedI128::one(),
            })
            .unwrap();
        engine
    }

    fn initial(seq: u64, amount: &str) -> Event {
        Event {
            seq,
            event_id: EventId(seq),
            ts_unix_ns: seq as i64,
            kind: EventKind::InitialCash(InitialCash {
                account_id: AccountId(1),
                currency_id: CurrencyId::usd(),
                amount: money(amount),
            }),
        }
    }

    fn fill(seq: u64, side: Side, qty: i128, px: &str, fee: &str) -> Event {
        Event {
            seq,
            event_id: EventId(seq),
            ts_unix_ns: seq as i64,
            kind: EventKind::Fill(Fill {
                account_id: AccountId(1),
                book_id: BookId(1),
                instrument_id: InstrumentId(1),
                side,
                qty: Qty::from_units(qty),
                price: price(px),
                fee: money(fee),
            }),
        }
    }

    fn mark(seq: u64, px: &str) -> Event {
        Event {
            seq,
            event_id: EventId(seq),
            ts_unix_ns: seq as i64,
            kind: EventKind::Mark(MarkPriceUpdate {
                instrument_id: InstrumentId(1),
                price: price(px),
            }),
        }
    }

    #[test]
    fn public_summary_and_drawdown_use_same_computed_equity() {
        let mut engine = setup();
        engine.apply(initial(1, "10000.00")).unwrap();
        engine.apply(fill(2, Side::Buy, 100, "10.00", "0")).unwrap();
        engine.apply(mark(3, "9.00")).unwrap();

        let metrics = AccountMetrics::compute(&engine, AccountId(1)).unwrap();
        let summary = engine.account_summary(AccountId(1)).unwrap();

        assert_eq!(metrics.equity(), summary.equity);
        assert_eq!(
            summary.current_drawdown,
            summary.equity.checked_sub(summary.peak_equity).unwrap()
        );
    }

    #[test]
    fn account_reconciliation_exposes_equity_pnl_formula() {
        let mut engine = setup();
        engine.apply(initial(1, "10000.00")).unwrap();
        engine.apply(fill(2, Side::Buy, 100, "10.00", "0")).unwrap();
        engine.apply(mark(3, "12.00")).unwrap();

        let reconciliation = engine.account_reconciliation(AccountId(1)).unwrap();

        assert_eq!(reconciliation.initial_cash, money("10000.00"));
        assert_eq!(reconciliation.cash, money("9000.00"));
        assert_eq!(reconciliation.position_market_value, money("1200.00"));
        assert_eq!(reconciliation.equity, money("10200.00"));
        assert_eq!(reconciliation.realized_pnl, money("0.00"));
        assert_eq!(reconciliation.unrealized_pnl, money("200.00"));
        assert_eq!(reconciliation.total_pnl, money("200.00"));
        assert_eq!(reconciliation.expected_pnl_from_equity, money("200.00"));
        assert_eq!(reconciliation.pnl_reconciliation_delta, money("0.00"));
    }

    #[test]
    fn apply_explained_reports_cash_equity_and_pnl_changes() {
        let mut engine = setup();
        let initial = engine.apply_explained(initial(1, "10000.00")).unwrap();

        assert_eq!(initial.receipt.changed_accounts, vec![AccountId(1)]);
        assert!(initial.receipt.changed_positions.is_empty());
        assert_eq!(initial.account_changes.len(), 1);
        assert_eq!(initial.account_changes[0].cash_delta, money("10000.00"));
        assert_eq!(initial.account_changes[0].equity_delta, money("10000.00"));
        assert_eq!(initial.account_changes[0].total_pnl_delta, money("0.00"));

        engine
            .apply_explained(fill(2, Side::Buy, 100, "10.00", "0"))
            .unwrap();
        let mark = engine.apply_explained(mark(3, "12.00")).unwrap();

        assert_eq!(mark.receipt.changed_accounts, vec![AccountId(1)]);
        assert_eq!(mark.receipt.changed_positions.len(), 1);
        assert_eq!(mark.account_changes.len(), 1);
        let change = &mark.account_changes[0];
        assert_eq!(change.cash_delta, money("0.00"));
        assert_eq!(change.position_market_value_delta, money("200.00"));
        assert_eq!(change.equity_delta, money("200.00"));
        assert_eq!(change.realized_pnl_delta, money("0.00"));
        assert_eq!(change.unrealized_pnl_delta, money("200.00"));
        assert_eq!(change.total_pnl_delta, money("200.00"));
        assert_eq!(change.after_pnl_reconciliation_delta, money("0.00"));
    }
}
