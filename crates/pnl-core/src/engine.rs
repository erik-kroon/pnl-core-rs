use crate::account::AccountState;
use crate::account_metrics::AccountMetrics;
use crate::accounting::{
    apply_average_cost_fill, fill_position_key, AverageCostConfig, AverageCostFillInput,
};
use crate::config::EngineConfig;
use crate::error::Result;
use crate::event::{Event, EventKind, Fill, FxRateUpdate, MarkPriceUpdate};
use crate::metadata::{AccountMeta, BookMeta, CurrencyMeta, InstrumentMeta};
use crate::position::{FxRate, Mark, Position, PositionKey};
use crate::registry::Registry;
use crate::replay_journal::ReplayJournal;
use crate::snapshot::{CanonicalStateV1, StateHash};
use crate::summary::{AccountSummary, ApplyResult};
use crate::types::*;
use crate::valuation::{self, ValuationConfig};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

mod accounting_effect {
    use crate::position::PositionKey;
    use crate::snapshot::StateHash;
    use crate::summary::ApplyResult;
    use crate::types::{AccountId, Money};
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
        pub(crate) fn new(zero: Money) -> Self {
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

        pub(crate) fn record_changed_position(&mut self, key: PositionKey) {
            self.state.changed_positions.push(key);
        }

        pub(crate) fn record_cash_delta(&mut self, delta: Money) {
            self.state.cash_delta = delta;
        }

        pub(crate) fn record_realized_pnl_delta(&mut self, delta: Money) {
            self.state.realized_pnl_delta = delta;
        }

        pub(crate) fn require_drawdown_update(&mut self, account_id: AccountId) {
            self.follow_up.drawdown_accounts.insert(account_id);
        }

        pub(crate) fn drawdown_accounts(&self) -> impl Iterator<Item = AccountId> + '_ {
            self.follow_up.drawdown_accounts.iter().copied()
        }

        pub(crate) fn into_apply_result(self, sequence: u64, state_hash: StateHash) -> ApplyResult {
            ApplyResult {
                sequence,
                changed_positions: self.state.changed_positions,
                realized_pnl_delta: self.state.realized_pnl_delta,
                cash_delta: self.state.cash_delta,
                state_hash,
            }
        }
    }
}

pub(crate) use accounting_effect::AccountingEffect;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Engine {
    pub(crate) config: EngineConfig,
    pub(crate) registry: Registry,
    pub(crate) accounts: BTreeMap<AccountId, AccountState>,
    pub(crate) positions: BTreeMap<PositionKey, Position>,
    pub(crate) marks: BTreeMap<InstrumentId, Mark>,
    pub(crate) fx_rates: BTreeMap<(CurrencyId, CurrencyId), FxRate>,
    pub(crate) replay_journal: ReplayJournal,
}

impl Engine {
    pub fn new(config: EngineConfig) -> Self {
        Self {
            config,
            registry: Registry::new(),
            accounts: BTreeMap::new(),
            positions: BTreeMap::new(),
            marks: BTreeMap::new(),
            fx_rates: BTreeMap::new(),
            replay_journal: ReplayJournal::new(),
        }
    }

    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    pub fn positions(&self) -> impl Iterator<Item = &Position> {
        self.positions.values()
    }

    pub fn register_currency(&mut self, meta: CurrencyMeta) -> Result<()> {
        self.registry
            .register_currency(meta, self.config.account_money_scale)
    }

    pub fn register_account(&mut self, meta: AccountMeta) -> Result<()> {
        let inserted = self.registry.register_account(meta.clone())?;
        if !inserted {
            return Ok(());
        }
        let zero = Money::zero(meta.base_currency, self.config.account_money_scale);
        self.accounts.insert(
            meta.account_id,
            AccountState {
                account_id: meta.account_id,
                base_currency: meta.base_currency,
                initial_cash: zero,
                cash: zero,
                net_external_cash_flows: zero,
                realized_pnl: zero,
                peak_equity: zero,
                current_drawdown: zero,
                max_drawdown: zero,
                initial_cash_set: false,
            },
        );
        Ok(())
    }

    pub fn register_book(&mut self, meta: BookMeta) -> Result<()> {
        self.registry.register_book(meta)
    }

    pub fn register_instrument(&mut self, meta: InstrumentMeta) -> Result<()> {
        self.registry.register_instrument(meta)
    }

    pub fn apply_many(
        &mut self,
        events: impl IntoIterator<Item = Event>,
    ) -> Result<Vec<ApplyResult>> {
        events.into_iter().map(|event| self.apply(event)).collect()
    }

    pub fn apply(&mut self, event: Event) -> Result<ApplyResult> {
        crate::replay_journal::apply_event(self, event)
    }

    pub fn position(&self, key: PositionKey) -> Option<&Position> {
        self.positions.get(&key)
    }

    pub fn account_summary(&self, account_id: AccountId) -> Result<AccountSummary> {
        Ok(AccountMetrics::compute(self, account_id)?.into_summary(self.state_hash()))
    }

    pub fn state_hash(&self) -> StateHash {
        StateHash::from_canonical(&CanonicalStateV1::from_engine(self))
    }

    fn valuation_config(&self) -> ValuationConfig {
        ValuationConfig {
            account_money_scale: self.config.account_money_scale,
            rounding_mode: self.config.rounding_mode,
        }
    }

    pub(super) fn apply_accounting_effect(
        &mut self,
        event: &Event,
        kind: &EventKind,
    ) -> Result<AccountingEffect> {
        let zero = Money::zero(self.config.base_currency, self.config.account_money_scale);
        let mut effect = AccountingEffect::new(zero);

        match kind {
            EventKind::InitialCash(initial) => {
                self.registry.ensure_account(initial.account_id)?;
                self.registry.ensure_money(
                    initial.amount,
                    initial.currency_id,
                    self.config.account_money_scale,
                )?;
                self.registry
                    .ensure_account_currency(initial.account_id, initial.currency_id)?;
                let account = self.accounts.get_mut(&initial.account_id).unwrap();
                let delta = initial.amount.checked_sub(account.cash)?;
                account.initial_cash = initial.amount;
                account.cash = initial.amount;
                account.initial_cash_set = true;
                effect.record_cash_delta(delta);
                effect.require_drawdown_update(initial.account_id);
            }
            EventKind::CashAdjustment(adj) => {
                self.registry.ensure_account(adj.account_id)?;
                self.registry.ensure_money(
                    adj.amount,
                    adj.currency_id,
                    self.config.account_money_scale,
                )?;
                self.registry
                    .ensure_account_currency(adj.account_id, adj.currency_id)?;
                let account = self.accounts.get_mut(&adj.account_id).unwrap();
                account.cash = account.cash.checked_add(adj.amount)?;
                account.net_external_cash_flows =
                    account.net_external_cash_flows.checked_add(adj.amount)?;
                effect.record_cash_delta(adj.amount);
                effect.require_drawdown_update(adj.account_id);
            }
            EventKind::Fill(fill) => {
                let (changed, c_delta, r_delta) = self.apply_fill(fill, event.ts_unix_ns)?;
                effect.record_changed_position(changed);
                effect.record_cash_delta(c_delta);
                effect.record_realized_pnl_delta(r_delta);
                effect.require_drawdown_update(fill.account_id);
            }
            EventKind::Mark(mark) => {
                self.apply_mark(mark, event.ts_unix_ns, &mut effect)?;
            }
            EventKind::FxRate(fx) => {
                self.apply_fx_rate(fx, event.ts_unix_ns, &mut effect)?;
            }
            EventKind::TradeCorrection(_) | EventKind::TradeBust(_) => {}
        }

        Ok(effect)
    }

    pub(super) fn reset_accounting_state_for_replay(&mut self) {
        for account in self.accounts.values_mut() {
            let zero = Money::zero(account.base_currency, self.config.account_money_scale);
            account.initial_cash = zero;
            account.cash = zero;
            account.net_external_cash_flows = zero;
            account.realized_pnl = zero;
            account.peak_equity = zero;
            account.current_drawdown = zero;
            account.max_drawdown = zero;
            account.initial_cash_set = false;
        }
        self.positions.clear();
        self.marks.clear();
        self.fx_rates.clear();
    }

    fn apply_fill(&mut self, fill: &Fill, ts_unix_ns: i64) -> Result<(PositionKey, Money, Money)> {
        self.registry.ensure_account(fill.account_id)?;
        self.registry.ensure_book(fill.account_id, fill.book_id)?;
        let instrument = self.registry.instrument(fill.instrument_id)?.clone();
        let account_currency = self.registry.account_currency(fill.account_id)?;
        self.registry.ensure_money(
            fill.fee,
            fill.fee.currency_id,
            self.config.account_money_scale,
        )?;
        let valuation_config = self.valuation_config();
        let key = fill_position_key(fill);

        let position = self.positions.remove(&key);
        let mut outcome = apply_average_cost_fill(
            AverageCostFillInput {
                position,
                fill,
                instrument: &instrument,
                account_currency,
                config: AverageCostConfig {
                    account_money_scale: self.config.account_money_scale,
                    rounding: self.config.rounding_mode,
                    allow_short: self.config.allow_short,
                    allow_position_flip: self.config.allow_position_flip,
                },
                ts_unix_ns,
            },
            |money, to_currency_id| {
                valuation::convert_money(money, to_currency_id, &self.fx_rates, valuation_config)
            },
        )?;
        let cash_delta = outcome.cash_delta;
        let realized_delta = outcome.realized_delta;
        valuation::revalue_position(
            &mut outcome.position,
            &instrument,
            account_currency,
            self.marks.get(&key.instrument_id),
            &self.fx_rates,
            valuation_config,
        )?;

        let account = self.accounts.get_mut(&fill.account_id).unwrap();
        account.cash = account.cash.checked_add(cash_delta)?;
        account.realized_pnl = account.realized_pnl.checked_add(realized_delta)?;

        self.positions.insert(key, outcome.position);
        Ok((outcome.key, cash_delta, realized_delta))
    }

    fn apply_mark(
        &mut self,
        mark: &MarkPriceUpdate,
        ts_unix_ns: i64,
        effect: &mut AccountingEffect,
    ) -> Result<()> {
        let instrument = self.registry.instrument(mark.instrument_id)?.clone();
        let valuation_config = self.valuation_config();
        let normalized_mark =
            valuation::normalize_mark(mark, &instrument, valuation_config, ts_unix_ns)?;
        let keys = valuation::positions_affected_by_mark(&self.positions, mark.instrument_id);
        valuation::ensure_direct_rates_for_positions(
            &keys,
            &self.registry,
            instrument.currency_id,
            &self.fx_rates,
        )?;

        self.marks.insert(mark.instrument_id, normalized_mark);
        for key in keys {
            let account_currency = self.registry.account_currency(key.account_id)?;
            let mut position = self.positions.remove(&key).unwrap();
            valuation::revalue_position(
                &mut position,
                &instrument,
                account_currency,
                self.marks.get(&key.instrument_id),
                &self.fx_rates,
                valuation_config,
            )?;
            effect.require_drawdown_update(key.account_id);
            effect.record_changed_position(key);
            self.positions.insert(key, position);
        }
        Ok(())
    }

    fn apply_fx_rate(
        &mut self,
        fx: &FxRateUpdate,
        ts_unix_ns: i64,
        effect: &mut AccountingEffect,
    ) -> Result<()> {
        self.registry.ensure_currency(fx.from_currency_id)?;
        self.registry.ensure_currency(fx.to_currency_id)?;
        let valuation_config = self.valuation_config();
        let rate = valuation::normalize_fx_rate(fx, valuation_config, ts_unix_ns)?;
        self.fx_rates
            .insert((fx.from_currency_id, fx.to_currency_id), rate);

        let keys = valuation::positions_affected_by_fx_rate(
            &self.positions,
            &self.registry,
            fx.from_currency_id,
            fx.to_currency_id,
        );
        for key in keys {
            let instrument = self.registry.instrument(key.instrument_id)?.clone();
            let account_currency = self.registry.account_currency(key.account_id)?;
            let mut position = self.positions.remove(&key).unwrap();
            valuation::revalue_position(
                &mut position,
                &instrument,
                account_currency,
                self.marks.get(&key.instrument_id),
                &self.fx_rates,
                valuation_config,
            )?;
            effect.require_drawdown_update(key.account_id);
            effect.record_changed_position(key);
            self.positions.insert(key, position);
        }
        Ok(())
    }

    pub(super) fn apply_accounting_follow_up(&mut self, effect: &AccountingEffect) -> Result<()> {
        for account_id in effect.drawdown_accounts() {
            self.update_drawdown(account_id)?;
        }
        Ok(())
    }

    pub(super) fn update_drawdown(&mut self, account_id: AccountId) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::InitialCash;
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

        let effect = engine.apply_accounting_effect(&event, &event.kind).unwrap();

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
            .apply_accounting_effect(&initial, &initial.kind)
            .unwrap();
        let event = fill_event();

        let effect = engine.apply_accounting_effect(&event, &event.kind).unwrap();

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
