use crate::engine::{AccountSummary, Engine};
use crate::error::{Error, Result};
use crate::snapshot::StateHash;
use crate::types::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AccountMetrics {
    account_id: AccountId,
    base_currency: CurrencyId,
    cash: Money,
    position_market_value: Money,
    equity: Money,
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
    pnl_reconciliation_delta: Money,
}

impl AccountMetrics {
    pub(crate) fn compute(engine: &Engine, account_id: AccountId) -> Result<Self> {
        let account = engine
            .accounts
            .get(&account_id)
            .ok_or(Error::UnknownAccount(account_id))?;
        let zero = Money::zero(account.base_currency, engine.config.account_money_scale);
        let mut gross = zero;
        let mut net = zero;
        let mut unrealized = zero;
        let mut open_positions = 0_u32;

        for position in engine
            .positions
            .values()
            .filter(|p| p.key.account_id == account_id)
        {
            if position.signed_qty.value != 0 {
                open_positions = open_positions
                    .checked_add(1)
                    .ok_or(Error::ArithmeticOverflow)?;
            }
            gross = gross.checked_add(position.gross_exposure)?;
            net = net.checked_add(position.net_exposure)?;
            unrealized = unrealized.checked_add(position.unrealized_pnl)?;
        }

        let equity = account.cash.checked_add(net)?;
        let total_pnl = account.realized_pnl.checked_add(unrealized)?;
        let leverage = if equity.amount > 0 {
            Some(Ratio::from_fraction(
                gross.amount,
                equity.amount,
                ACCOUNT_RATIO_SCALE,
                engine.config.rounding_mode,
            )?)
        } else {
            None
        };
        let expected_pnl = equity
            .checked_sub(account.initial_cash)?
            .checked_sub(account.net_external_cash_flows)?;
        let pnl_reconciliation_delta = expected_pnl.checked_sub(total_pnl)?;

        Ok(Self {
            account_id,
            base_currency: account.base_currency,
            cash: account.cash,
            position_market_value: net,
            equity,
            realized_pnl: account.realized_pnl,
            unrealized_pnl: unrealized,
            total_pnl,
            gross_exposure: gross,
            net_exposure: net,
            leverage,
            peak_equity: account.peak_equity,
            current_drawdown: account.current_drawdown,
            max_drawdown: account.max_drawdown,
            open_positions,
            net_external_cash_flows: account.net_external_cash_flows,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{
        AccountMeta, BookMeta, EngineConfig, Event, EventKind, Fill, InitialCash, InstrumentMeta,
        MarkPriceUpdate,
    };

    fn money(value: &str) -> Money {
        Money::parse_decimal(value, CurrencyId::usd(), ACCOUNT_MONEY_SCALE).unwrap()
    }

    fn price(value: &str) -> Price {
        Price::parse_decimal(value).unwrap()
    }

    fn setup() -> Engine {
        let mut engine = Engine::new(EngineConfig::default());
        engine
            .register_currency(crate::engine::CurrencyMeta {
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
}
