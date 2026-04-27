use super::Engine;
use crate::account_metrics::AccountMetrics;
use crate::accounting::{self, FillAccountingConfig, FillAccountingInput, FillAccountingState};
use crate::error::{Error, Result};
use crate::event::{
    CashAdjustment, Event, EventKind, Fill, FinancingEvent, FxRateUpdate, InitialCash,
    InstrumentLifecycle, InstrumentSplit, InstrumentSymbolChange, MarkPriceUpdate,
};
use crate::position::PositionKey;
use crate::summary::ApplyReceipt;
use crate::types::*;
use crate::valuation::{self, ValuationConfig};
use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AccountingEffect {
    pub(crate) state: AccountingStateChanges,
    pub(crate) follow_up: AccountingFollowUp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AccountingStateChanges {
    pub(crate) changed_positions: Vec<PositionKey>,
    pub(crate) cash_delta: Money,
    pub(crate) realized_pnl_delta: Money,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AccountingFollowUp {
    pub(crate) drawdown_accounts: BTreeSet<AccountId>,
}

impl AccountingEffect {
    fn new(zero: Money) -> Self {
        Self {
            state: AccountingStateChanges {
                changed_positions: Vec::new(),
                cash_delta: zero,
                realized_pnl_delta: zero,
            },
            follow_up: AccountingFollowUp {
                drawdown_accounts: BTreeSet::new(),
            },
        }
    }

    fn record_changed_position(&mut self, key: PositionKey) {
        self.state.changed_positions.push(key);
    }

    fn record_cash_delta(&mut self, delta: Money) {
        self.state.cash_delta = delta;
    }

    fn record_realized_pnl_delta(&mut self, delta: Money) {
        self.state.realized_pnl_delta = delta;
    }

    fn require_drawdown_update(&mut self, account_id: AccountId) {
        self.follow_up.drawdown_accounts.insert(account_id);
    }

    fn drawdown_accounts(&self) -> impl Iterator<Item = AccountId> + '_ {
        self.follow_up.drawdown_accounts.iter().copied()
    }

    pub(crate) fn into_apply_receipt(self, sequence: u64) -> ApplyReceipt {
        ApplyReceipt {
            sequence,
            changed_positions: self.state.changed_positions,
            realized_pnl_delta: self.state.realized_pnl_delta,
            cash_delta: self.state.cash_delta,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FinancingBucket {
    Interest,
    Borrow,
    Funding,
    Financing,
}

struct EventApplication<'a> {
    engine: &'a mut Engine,
    event: &'a Event,
    effect: AccountingEffect,
}

impl<'a> EventApplication<'a> {
    fn new(engine: &'a mut Engine, event: &'a Event) -> Self {
        let zero = Money::zero(
            engine.config.base_currency,
            engine.config.account_money_scale,
        );
        Self {
            engine,
            event,
            effect: AccountingEffect::new(zero),
        }
    }

    fn apply(mut self, kind: &EventKind) -> Result<AccountingEffect> {
        match kind {
            EventKind::InitialCash(initial) => self.apply_initial_cash(initial)?,
            EventKind::CashAdjustment(adj) => self.apply_cash_adjustment(adj)?,
            EventKind::Interest(financing) => {
                self.apply_financing_event(financing, FinancingBucket::Interest)?
            }
            EventKind::Borrow(financing) => {
                self.apply_financing_event(financing, FinancingBucket::Borrow)?
            }
            EventKind::Funding(financing) => {
                self.apply_financing_event(financing, FinancingBucket::Funding)?
            }
            EventKind::Financing(financing) => {
                self.apply_financing_event(financing, FinancingBucket::Financing)?
            }
            EventKind::Fill(fill) => self.apply_fill(fill)?,
            EventKind::Mark(mark) => self.apply_mark(mark)?,
            EventKind::FxRate(fx) => self.apply_fx_rate(fx)?,
            EventKind::Split(split) => self.apply_split(split)?,
            EventKind::SymbolChange(change) => self.apply_symbol_change(change)?,
            EventKind::InstrumentLifecycle(lifecycle) => {
                self.apply_instrument_lifecycle(lifecycle)?
            }
            EventKind::TradeCorrection(_) | EventKind::TradeBust(_) => {}
        }

        Ok(self.effect)
    }

    fn valuation_config(&self) -> ValuationConfig {
        ValuationConfig {
            account_money_scale: self.engine.config.account_money_scale,
            rounding_mode: self.engine.config.rounding_mode,
            fx_routing: self.engine.config.fx_routing.clone(),
        }
    }

    fn apply_initial_cash(&mut self, initial: &InitialCash) -> Result<()> {
        self.engine.registry.ensure_account(initial.account_id)?;
        self.engine.registry.ensure_money(
            initial.amount,
            initial.currency_id,
            self.engine.config.account_money_scale,
        )?;
        self.engine
            .registry
            .ensure_account_currency(initial.account_id, initial.currency_id)?;
        let account = self.engine.accounts.get_mut(&initial.account_id).unwrap();
        let delta = initial.amount.checked_sub(account.cash)?;
        account.initial_cash = initial.amount;
        account.cash = initial.amount;
        account.initial_cash_set = true;

        self.effect.record_cash_delta(delta);
        self.effect.require_drawdown_update(initial.account_id);
        Ok(())
    }

    fn apply_cash_adjustment(&mut self, adj: &CashAdjustment) -> Result<()> {
        self.engine.registry.ensure_account(adj.account_id)?;
        self.engine.registry.ensure_money(
            adj.amount,
            adj.currency_id,
            self.engine.config.account_money_scale,
        )?;
        self.engine
            .registry
            .ensure_account_currency(adj.account_id, adj.currency_id)?;
        let account = self.engine.accounts.get_mut(&adj.account_id).unwrap();
        account.cash = account.cash.checked_add(adj.amount)?;
        account.net_external_cash_flows =
            account.net_external_cash_flows.checked_add(adj.amount)?;

        self.effect.record_cash_delta(adj.amount);
        self.effect.require_drawdown_update(adj.account_id);
        Ok(())
    }

    fn apply_financing_event(
        &mut self,
        financing: &FinancingEvent,
        bucket: FinancingBucket,
    ) -> Result<()> {
        self.engine.registry.ensure_account(financing.account_id)?;
        self.engine.registry.ensure_money(
            financing.amount,
            financing.currency_id,
            self.engine.config.account_money_scale,
        )?;
        self.engine
            .registry
            .ensure_account_currency(financing.account_id, financing.currency_id)?;

        let account = self.engine.accounts.get_mut(&financing.account_id).unwrap();
        account.cash = account.cash.checked_add(financing.amount)?;
        match bucket {
            FinancingBucket::Interest => {
                account.interest_pnl = account.interest_pnl.checked_add(financing.amount)?;
            }
            FinancingBucket::Borrow => {
                account.borrow_pnl = account.borrow_pnl.checked_add(financing.amount)?;
            }
            FinancingBucket::Funding => {
                account.funding_pnl = account.funding_pnl.checked_add(financing.amount)?;
            }
            FinancingBucket::Financing => {
                account.financing_pnl = account.financing_pnl.checked_add(financing.amount)?;
            }
        }
        account.total_financing_pnl = account.total_financing_pnl.checked_add(financing.amount)?;
        account.realized_pnl = account.realized_pnl.checked_add(financing.amount)?;

        self.effect.record_cash_delta(financing.amount);
        self.effect.record_realized_pnl_delta(financing.amount);
        self.effect.require_drawdown_update(financing.account_id);
        Ok(())
    }

    fn apply_fill(&mut self, fill: &Fill) -> Result<()> {
        let outcome = self.apply_fill_state(fill)?;
        self.effect.record_changed_position(outcome.key);
        self.effect.record_cash_delta(outcome.cash_delta);
        self.effect
            .record_realized_pnl_delta(outcome.realized_delta);
        self.effect.require_drawdown_update(fill.account_id);
        Ok(())
    }

    fn apply_fill_state(&mut self, fill: &Fill) -> Result<accounting::FillAccountingOutcome> {
        self.engine.registry.ensure_account(fill.account_id)?;
        self.engine
            .registry
            .ensure_book(fill.account_id, fill.book_id)?;
        self.engine
            .registry
            .ensure_instrument_tradeable(fill.instrument_id)?;
        let instrument = self.engine.registry.instrument(fill.instrument_id)?.clone();
        let account_currency = self.engine.registry.account_currency(fill.account_id)?;
        self.engine.registry.ensure_money(
            fill.fee,
            fill.fee.currency_id,
            self.engine.config.account_money_scale,
        )?;
        let valuation_config = self.valuation_config();
        let account = self.engine.accounts.get_mut(&fill.account_id).unwrap();

        accounting::apply_fill(
            FillAccountingInput {
                fill,
                instrument: &instrument,
                account_currency,
                config: FillAccountingConfig {
                    account_money_scale: self.engine.config.account_money_scale,
                    rounding: self.engine.config.rounding_mode,
                    allow_short: self.engine.config.allow_short,
                    allow_position_flip: self.engine.config.allow_position_flip,
                    method: self.engine.config.accounting_method,
                },
                valuation_config,
                seq: self.event.seq,
                event_id: self.event.event_id,
                ts_unix_ns: self.event.ts_unix_ns,
            },
            FillAccountingState {
                account,
                positions: &mut self.engine.positions,
                lots: &mut self.engine.lots,
                marks: &self.engine.marks,
                fx_rates: &self.engine.fx_rates,
            },
        )
    }

    fn apply_mark(&mut self, mark: &MarkPriceUpdate) -> Result<()> {
        let valuation_config = self.valuation_config();
        let update = valuation::apply_mark_update(
            mark,
            valuation::ValuationStores {
                registry: &self.engine.registry,
                positions: &mut self.engine.positions,
                marks: &mut self.engine.marks,
                fx_rates: &mut self.engine.fx_rates,
            },
            valuation_config,
            self.event.ts_unix_ns,
        )?;
        for key in update.changed_positions {
            self.effect.require_drawdown_update(key.account_id);
            self.effect.record_changed_position(key);
        }
        Ok(())
    }

    fn apply_fx_rate(&mut self, fx: &FxRateUpdate) -> Result<()> {
        let valuation_config = self.valuation_config();
        let update = valuation::apply_fx_rate_update(
            fx,
            valuation::ValuationStores {
                registry: &self.engine.registry,
                positions: &mut self.engine.positions,
                marks: &mut self.engine.marks,
                fx_rates: &mut self.engine.fx_rates,
            },
            valuation_config,
            self.event.ts_unix_ns,
        )?;
        for key in update.changed_positions {
            self.effect.require_drawdown_update(key.account_id);
            self.effect.record_changed_position(key);
        }
        Ok(())
    }

    fn apply_split(&mut self, split: &InstrumentSplit) -> Result<()> {
        if split.numerator == 0 || split.denominator == 0 {
            return Err(Error::InvalidSplitRatio);
        }
        let instrument = self
            .engine
            .registry
            .instrument(split.instrument_id)?
            .clone();
        let valuation_config = self.valuation_config();
        let numerator = i128::from(split.numerator);
        let denominator = i128::from(split.denominator);

        let mut next_marks = self.engine.marks.clone();
        let mut next_lots = self.engine.lots.clone();
        let mut next_positions = self.engine.positions.clone();
        let mut changed_keys = Vec::new();

        if let Some(mark) = next_marks.get_mut(&split.instrument_id) {
            mark.price = adjust_split_price(
                mark.price,
                numerator,
                denominator,
                instrument.price_scale,
                self.engine.config.rounding_mode,
            )?;
            mark.ts_unix_ns = self.event.ts_unix_ns;
        }

        for lot in next_lots
            .values_mut()
            .filter(|lot| lot.instrument_id == split.instrument_id)
        {
            lot.original_qty = adjust_split_qty(lot.original_qty, numerator, denominator)?;
            lot.remaining_qty = adjust_split_qty(lot.remaining_qty, numerator, denominator)?;
            lot.entry_price = adjust_split_price(
                lot.entry_price,
                numerator,
                denominator,
                instrument.price_scale,
                self.engine.config.rounding_mode,
            )?;
        }

        for (key, position) in next_positions
            .iter_mut()
            .filter(|(key, _)| key.instrument_id == split.instrument_id)
        {
            let key = *key;
            let account_currency = self.engine.registry.account_currency(key.account_id)?;
            position.signed_qty = adjust_split_qty(position.signed_qty, numerator, denominator)?;
            position.avg_price = position
                .avg_price
                .map(|price| {
                    adjust_split_price(
                        price,
                        numerator,
                        denominator,
                        instrument.price_scale,
                        self.engine.config.rounding_mode,
                    )
                })
                .transpose()?;
            position.updated_at_unix_ns = self.event.ts_unix_ns;
            valuation::revalue_position(
                position,
                &instrument,
                account_currency,
                next_marks.get(&key.instrument_id),
                &self.engine.fx_rates,
                valuation_config.clone(),
            )?;
            changed_keys.push(key);
        }

        self.engine.marks = next_marks;
        self.engine.lots = next_lots;
        self.engine.positions = next_positions;

        for key in changed_keys {
            self.effect.require_drawdown_update(key.account_id);
            self.effect.record_changed_position(key);
        }

        Ok(())
    }

    fn apply_symbol_change(&mut self, change: &InstrumentSymbolChange) -> Result<()> {
        self.engine
            .registry
            .update_instrument_symbol(change.instrument_id, change.symbol.clone())
    }

    fn apply_instrument_lifecycle(&mut self, lifecycle: &InstrumentLifecycle) -> Result<()> {
        self.engine
            .registry
            .set_instrument_lifecycle(lifecycle.instrument_id, lifecycle.state)
    }
}

impl Engine {
    pub(crate) fn apply_accounting_event_without_journal(
        &mut self,
        event: &Event,
        kind: &EventKind,
    ) -> Result<AccountingEffect> {
        let effect = EventApplication::new(self, event).apply(kind)?;
        self.apply_accounting_follow_up(&effect)?;
        Ok(effect)
    }

    fn apply_accounting_follow_up(&mut self, effect: &AccountingEffect) -> Result<()> {
        for account_id in effect.drawdown_accounts() {
            self.update_drawdown(account_id)?;
        }
        Ok(())
    }

    fn update_drawdown(&mut self, account_id: AccountId) -> Result<()> {
        let equity = AccountMetrics::compute(self, account_id)?.equity();
        let account = self.accounts.get_mut(&account_id).unwrap();
        if !account.initial_cash_set || equity.amount > account.peak_equity.amount {
            account.peak_equity = equity;
        }
        account.current_drawdown = equity.checked_sub(account.peak_equity)?;
        if account.current_drawdown.amount < account.max_drawdown.amount {
            account.max_drawdown = account.current_drawdown;
        }
        Ok(())
    }
}

fn adjust_split_qty(qty: Qty, numerator: i128, denominator: i128) -> Result<Qty> {
    let multiplied = qty
        .value
        .checked_mul(numerator)
        .ok_or(Error::ArithmeticOverflow)?;
    if multiplied % denominator != 0 {
        return Err(Error::InvalidScale);
    }
    Ok(Qty::new(multiplied / denominator, qty.scale))
}

fn adjust_split_price(
    price: Price,
    numerator: i128,
    denominator: i128,
    target_scale: u8,
    rounding: RoundingMode,
) -> Result<Price> {
    let multiplied = price
        .value
        .checked_mul(denominator)
        .ok_or(Error::ArithmeticOverflow)?;
    let value = div_round(multiplied, numerator, rounding)?;
    Price::new(value, price.scale).to_scale(target_scale, rounding)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EngineConfig;
    use crate::metadata::{AccountMeta, BookMeta, CurrencyMeta, InstrumentMeta};
    use std::collections::BTreeSet;

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

    fn initial_cash_event(amount: &str) -> Event {
        Event {
            seq: 1,
            event_id: EventId(1),
            ts_unix_ns: 1,
            kind: EventKind::InitialCash(InitialCash {
                account_id: AccountId(1),
                currency_id: CurrencyId::usd(),
                amount: money(amount),
            }),
        }
    }

    fn fill_event() -> Event {
        Event {
            seq: 2,
            event_id: EventId(2),
            ts_unix_ns: 2,
            kind: EventKind::Fill(Fill {
                account_id: AccountId(1),
                book_id: BookId(1),
                instrument_id: InstrumentId(1),
                side: Side::Buy,
                qty: Qty::from_units(100),
                price: price("10.00"),
                fee: money("1.00"),
            }),
        }
    }

    #[test]
    fn initial_cash_effect_names_state_changes_and_follow_up() {
        let mut engine = setup();
        let event = initial_cash_event("1000.00");

        let effect = engine
            .apply_accounting_event_without_journal(&event, &event.kind)
            .unwrap();

        assert_eq!(effect.state.changed_positions, Vec::new());
        assert_eq!(effect.state.cash_delta, money("1000.00"));
        assert_eq!(effect.state.realized_pnl_delta, money("0.00"));
        assert_eq!(
            effect.follow_up.drawdown_accounts,
            BTreeSet::from([AccountId(1)])
        );
    }

    #[test]
    fn fill_effect_names_position_and_account_changes() {
        let mut engine = setup();
        let initial = initial_cash_event("2000.00");
        engine
            .apply_accounting_event_without_journal(&initial, &initial.kind)
            .unwrap();
        let event = fill_event();

        let effect = engine
            .apply_accounting_event_without_journal(&event, &event.kind)
            .unwrap();

        assert_eq!(
            effect.state.changed_positions,
            vec![PositionKey {
                account_id: AccountId(1),
                book_id: BookId(1),
                instrument_id: InstrumentId(1),
            }]
        );
        assert_eq!(effect.state.cash_delta, money("-1001.00"));
        assert_eq!(effect.state.realized_pnl_delta, money("-1.00"));
        assert_eq!(
            effect.follow_up.drawdown_accounts,
            BTreeSet::from([AccountId(1)])
        );
    }
}
