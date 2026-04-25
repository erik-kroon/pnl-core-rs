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
use std::collections::{BTreeMap, BTreeSet};

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
    ) -> Result<(Vec<PositionKey>, Money, Money, BTreeSet<AccountId>)> {
        let zero = Money::zero(self.config.base_currency, self.config.account_money_scale);
        let mut changed_positions = Vec::new();
        let mut cash_delta = zero;
        let mut realized_delta = zero;
        let mut drawdown_accounts = BTreeSet::new();

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
                cash_delta = delta;
                drawdown_accounts.insert(initial.account_id);
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
                cash_delta = adj.amount;
                drawdown_accounts.insert(adj.account_id);
            }
            EventKind::Fill(fill) => {
                let (changed, c_delta, r_delta) = self.apply_fill(fill, event.ts_unix_ns)?;
                changed_positions.push(changed);
                cash_delta = c_delta;
                realized_delta = r_delta;
                drawdown_accounts.insert(fill.account_id);
            }
            EventKind::Mark(mark) => {
                self.apply_mark(
                    mark,
                    event.ts_unix_ns,
                    &mut changed_positions,
                    &mut drawdown_accounts,
                )?;
            }
            EventKind::FxRate(fx) => {
                self.apply_fx_rate(
                    fx,
                    event.ts_unix_ns,
                    &mut changed_positions,
                    &mut drawdown_accounts,
                )?;
            }
            EventKind::TradeCorrection(_) | EventKind::TradeBust(_) => {}
        }

        Ok((
            changed_positions,
            cash_delta,
            realized_delta,
            drawdown_accounts,
        ))
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
        changed_positions: &mut Vec<PositionKey>,
        drawdown_accounts: &mut BTreeSet<AccountId>,
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
            drawdown_accounts.insert(key.account_id);
            changed_positions.push(key);
            self.positions.insert(key, position);
        }
        Ok(())
    }

    fn apply_fx_rate(
        &mut self,
        fx: &FxRateUpdate,
        ts_unix_ns: i64,
        changed_positions: &mut Vec<PositionKey>,
        drawdown_accounts: &mut BTreeSet<AccountId>,
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
            drawdown_accounts.insert(key.account_id);
            changed_positions.push(key);
            self.positions.insert(key, position);
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
