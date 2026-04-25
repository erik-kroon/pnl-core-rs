use crate::accounting::{
    apply_average_cost_fill, fill_position_key, revalue_position, AverageCostConfig,
    AverageCostFillInput, PositionRevaluation,
};
use crate::error::{Error, Result};
use crate::replay_journal::{JournalAction, ReplayJournal};
use crate::snapshot::{CanonicalStateV1, StateHash};
use crate::types::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrencyMeta {
    pub currency_id: CurrencyId,
    pub code: String,
    pub scale: u8,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountMeta {
    pub account_id: AccountId,
    pub base_currency: CurrencyId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookMeta {
    pub account_id: AccountId,
    pub book_id: BookId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstrumentMeta {
    pub instrument_id: InstrumentId,
    pub symbol: String,
    pub currency_id: CurrencyId,
    pub price_scale: u8,
    pub qty_scale: u8,
    pub multiplier: FixedI128,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PositionKey {
    pub account_id: AccountId,
    pub book_id: BookId,
    pub instrument_id: InstrumentId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub key: PositionKey,
    pub signed_qty: Qty,
    pub avg_price: Option<Price>,
    pub cost_basis: Money,
    pub realized_pnl: Money,
    pub unrealized_pnl: Money,
    pub gross_exposure: Money,
    pub net_exposure: Money,
    pub opened_at_unix_ns: Option<i64>,
    pub updated_at_unix_ns: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mark {
    pub instrument_id: InstrumentId,
    pub price: Price,
    pub ts_unix_ns: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FxRate {
    pub from_currency_id: CurrencyId,
    pub to_currency_id: CurrencyId,
    pub rate: Price,
    pub ts_unix_ns: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountState {
    pub account_id: AccountId,
    pub base_currency: CurrencyId,
    pub initial_cash: Money,
    pub cash: Money,
    pub net_external_cash_flows: Money,
    pub realized_pnl: Money,
    pub peak_equity: Money,
    pub current_drawdown: Money,
    pub max_drawdown: Money,
    pub initial_cash_set: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    pub seq: u64,
    pub event_id: EventId,
    pub ts_unix_ns: i64,
    pub kind: EventKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventKind {
    InitialCash(InitialCash),
    CashAdjustment(CashAdjustment),
    Fill(Fill),
    Mark(MarkPriceUpdate),
    FxRate(FxRateUpdate),
    TradeCorrection(TradeCorrection),
    TradeBust(TradeBust),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitialCash {
    pub account_id: AccountId,
    pub currency_id: CurrencyId,
    pub amount: Money,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CashAdjustment {
    pub account_id: AccountId,
    pub currency_id: CurrencyId,
    pub amount: Money,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fill {
    pub account_id: AccountId,
    pub book_id: BookId,
    pub instrument_id: InstrumentId,
    pub side: Side,
    pub qty: Qty,
    pub price: Price,
    pub fee: Money,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeCorrection {
    pub original_event_id: EventId,
    pub replacement: Fill,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeBust {
    pub original_event_id: EventId,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarkPriceUpdate {
    pub instrument_id: InstrumentId,
    pub price: Price,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FxRateUpdate {
    pub from_currency_id: CurrencyId,
    pub to_currency_id: CurrencyId,
    pub rate: Price,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplyResult {
    pub sequence: u64,
    pub changed_positions: Vec<PositionKey>,
    pub realized_pnl_delta: Money,
    pub cash_delta: Money,
    pub state_hash: StateHash,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountSummary {
    pub account_id: AccountId,
    pub base_currency: CurrencyId,
    pub cash: Money,
    pub position_market_value: Money,
    pub equity: Money,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Engine {
    pub(crate) config: EngineConfig,
    pub(crate) currencies: BTreeMap<CurrencyId, CurrencyMeta>,
    pub(crate) accounts: BTreeMap<AccountId, AccountState>,
    pub(crate) books: BTreeSet<(AccountId, BookId)>,
    pub(crate) instruments: BTreeMap<InstrumentId, InstrumentMeta>,
    pub(crate) positions: BTreeMap<PositionKey, Position>,
    pub(crate) marks: BTreeMap<InstrumentId, Mark>,
    pub(crate) fx_rates: BTreeMap<(CurrencyId, CurrencyId), FxRate>,
    pub(crate) replay_journal: ReplayJournal,
}

impl Engine {
    pub fn new(config: EngineConfig) -> Self {
        Self {
            config,
            currencies: BTreeMap::new(),
            accounts: BTreeMap::new(),
            books: BTreeSet::new(),
            instruments: BTreeMap::new(),
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
        if meta.scale != self.config.account_money_scale {
            return Err(Error::InvalidScale);
        }
        self.currencies.insert(meta.currency_id, meta);
        Ok(())
    }

    pub fn register_account(&mut self, meta: AccountMeta) -> Result<()> {
        self.ensure_currency(meta.base_currency)?;
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
        self.ensure_account(meta.account_id)?;
        self.books.insert((meta.account_id, meta.book_id));
        Ok(())
    }

    pub fn register_instrument(&mut self, meta: InstrumentMeta) -> Result<()> {
        self.ensure_currency(meta.currency_id)?;
        if meta.multiplier.value <= 0 {
            return Err(Error::InvalidScale);
        }
        self.instruments.insert(meta.instrument_id, meta);
        Ok(())
    }

    pub fn apply_many(
        &mut self,
        events: impl IntoIterator<Item = Event>,
    ) -> Result<Vec<ApplyResult>> {
        events.into_iter().map(|event| self.apply(event)).collect()
    }

    pub fn apply(&mut self, event: Event) -> Result<ApplyResult> {
        let action = self
            .replay_journal
            .prepare(self.config.expected_start_seq, &event)?;
        if action == JournalAction::RewriteHistory {
            return self.apply_history_rewrite(event);
        }

        let (changed_positions, cash_delta, realized_delta, drawdown_accounts) =
            self.apply_accounting_effect(&event, &event.kind)?;
        for account_id in drawdown_accounts {
            self.update_drawdown(account_id)?;
        }

        self.replay_journal.record_accepted(event);
        Ok(ApplyResult {
            sequence: self.replay_journal.last_seq(),
            changed_positions,
            realized_pnl_delta: realized_delta,
            cash_delta,
            state_hash: self.state_hash(),
        })
    }

    pub fn position(&self, key: PositionKey) -> Option<&Position> {
        self.positions.get(&key)
    }

    pub fn account_summary(&self, account_id: AccountId) -> Result<AccountSummary> {
        self.ensure_account(account_id)?;
        let account = self.accounts.get(&account_id).unwrap();
        let zero = Money::zero(account.base_currency, self.config.account_money_scale);
        let mut gross = zero;
        let mut net = zero;
        let mut unrealized = zero;
        let mut open_positions = 0_u32;
        for position in self
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
                self.config.rounding_mode,
            )?)
        } else {
            None
        };
        let expected_pnl = equity
            .checked_sub(account.initial_cash)?
            .checked_sub(account.net_external_cash_flows)?;
        let pnl_reconciliation_delta = expected_pnl.checked_sub(total_pnl)?;
        Ok(AccountSummary {
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
            state_hash: self.state_hash(),
        })
    }

    pub fn state_hash(&self) -> StateHash {
        StateHash::from_canonical(&CanonicalStateV1::from_engine(self))
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
                self.ensure_account(initial.account_id)?;
                self.ensure_money(initial.amount, initial.currency_id)?;
                self.ensure_account_currency(initial.account_id, initial.currency_id)?;
                let account = self.accounts.get_mut(&initial.account_id).unwrap();
                let delta = initial.amount.checked_sub(account.cash)?;
                account.initial_cash = initial.amount;
                account.cash = initial.amount;
                account.initial_cash_set = true;
                cash_delta = delta;
                drawdown_accounts.insert(initial.account_id);
            }
            EventKind::CashAdjustment(adj) => {
                self.ensure_account(adj.account_id)?;
                self.ensure_money(adj.amount, adj.currency_id)?;
                self.ensure_account_currency(adj.account_id, adj.currency_id)?;
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

    fn apply_history_rewrite(&mut self, event: Event) -> Result<ApplyResult> {
        let target = self.replay_journal.validate_rewrite_target(&event)?;
        let before_account = self
            .accounts
            .get(&target.account_id)
            .ok_or(Error::UnknownAccount(target.account_id))?
            .clone();

        let mut next = self.clone();
        next.replay_journal.record_accepted(event);
        let mut journal = std::mem::take(&mut next.replay_journal);
        journal.rebuild_engine(&mut next)?;
        next.replay_journal = journal;

        let after_account = next
            .accounts
            .get(&target.account_id)
            .ok_or(Error::UnknownAccount(target.account_id))?;
        let cash_delta = after_account.cash.checked_sub(before_account.cash)?;
        let realized_delta = after_account
            .realized_pnl
            .checked_sub(before_account.realized_pnl)?;
        let sequence = next.replay_journal.last_seq();
        let state_hash = next.state_hash();
        *self = next;

        Ok(ApplyResult {
            sequence,
            changed_positions: vec![target.position_key],
            realized_pnl_delta: realized_delta,
            cash_delta,
            state_hash,
        })
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
        self.ensure_account(fill.account_id)?;
        self.ensure_book(fill.account_id, fill.book_id)?;
        let instrument = self.ensure_instrument(fill.instrument_id)?.clone();
        let account_currency = self.accounts.get(&fill.account_id).unwrap().base_currency;
        self.ensure_money(fill.fee, fill.fee.currency_id)?;
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
            |money, to_currency_id| self.convert_money(money, to_currency_id),
        )?;
        let cash_delta = outcome.cash_delta;
        let realized_delta = outcome.realized_delta;
        self.revalue_position(&mut outcome.position, &instrument)?;

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
        let instrument = self.ensure_instrument(mark.instrument_id)?.clone();
        let price = mark
            .price
            .to_scale(instrument.price_scale, self.config.rounding_mode)?;
        let keys: Vec<_> = self
            .positions
            .keys()
            .copied()
            .filter(|key| key.instrument_id == mark.instrument_id)
            .collect();
        for key in &keys {
            let account_currency = self.accounts.get(&key.account_id).unwrap().base_currency;
            self.ensure_conversion_rate(instrument.currency_id, account_currency)?;
        }

        self.marks.insert(
            mark.instrument_id,
            Mark {
                instrument_id: mark.instrument_id,
                price,
                ts_unix_ns,
            },
        );
        for key in keys {
            let mut position = self.positions.remove(&key).unwrap();
            self.revalue_position(&mut position, &instrument)?;
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
        self.ensure_currency(fx.from_currency_id)?;
        self.ensure_currency(fx.to_currency_id)?;
        if fx.rate.value <= 0 {
            return Err(Error::InvalidPrice);
        }
        let rate = fx.rate.to_scale(fx.rate.scale, self.config.rounding_mode)?;
        self.fx_rates.insert(
            (fx.from_currency_id, fx.to_currency_id),
            FxRate {
                from_currency_id: fx.from_currency_id,
                to_currency_id: fx.to_currency_id,
                rate,
                ts_unix_ns,
            },
        );

        let keys: Vec<_> = self
            .positions
            .keys()
            .copied()
            .filter(|key| {
                let Some(instrument) = self.instruments.get(&key.instrument_id) else {
                    return false;
                };
                let Some(account) = self.accounts.get(&key.account_id) else {
                    return false;
                };
                instrument.currency_id == fx.from_currency_id
                    && account.base_currency == fx.to_currency_id
            })
            .collect();
        for key in keys {
            let instrument = self.instruments.get(&key.instrument_id).unwrap().clone();
            let mut position = self.positions.remove(&key).unwrap();
            self.revalue_position(&mut position, &instrument)?;
            drawdown_accounts.insert(key.account_id);
            changed_positions.push(key);
            self.positions.insert(key, position);
        }
        Ok(())
    }

    fn revalue_position(&self, position: &mut Position, instrument: &InstrumentMeta) -> Result<()> {
        let account_currency = self
            .accounts
            .get(&position.key.account_id)
            .ok_or(Error::UnknownAccount(position.key.account_id))?
            .base_currency;
        let valuation_price = self.marks.get(&position.key.instrument_id).map(|m| m.price);
        revalue_position(
            position,
            PositionRevaluation {
                account_currency,
                account_money_scale: self.config.account_money_scale,
                valuation_price,
                instrument_currency: instrument.currency_id,
                multiplier: instrument.multiplier,
                rounding: self.config.rounding_mode,
            },
            |money, to_currency_id| self.convert_money(money, to_currency_id),
        )
    }

    pub(super) fn update_drawdown(&mut self, account_id: AccountId) -> Result<()> {
        let summary = self.account_summary_without_hash(account_id)?;
        let account = self.accounts.get_mut(&account_id).unwrap();
        if !account.initial_cash_set || summary.equity.amount > account.peak_equity.amount {
            account.peak_equity = summary.equity;
        }
        account.current_drawdown = summary.equity.checked_sub(account.peak_equity)?;
        if account.current_drawdown.amount < account.max_drawdown.amount {
            account.max_drawdown = account.current_drawdown;
        }
        Ok(())
    }

    fn account_summary_without_hash(&self, account_id: AccountId) -> Result<AccountSummary> {
        self.ensure_account(account_id)?;
        let account = self.accounts.get(&account_id).unwrap();
        let zero = Money::zero(account.base_currency, self.config.account_money_scale);
        let mut gross = zero;
        let mut net = zero;
        let mut unrealized = zero;
        let mut open_positions = 0_u32;
        for position in self
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
                self.config.rounding_mode,
            )?)
        } else {
            None
        };
        let expected_pnl = equity
            .checked_sub(account.initial_cash)?
            .checked_sub(account.net_external_cash_flows)?;
        let pnl_reconciliation_delta = expected_pnl.checked_sub(total_pnl)?;
        Ok(AccountSummary {
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
            state_hash: StateHash::zero(),
        })
    }

    fn ensure_currency(&self, currency_id: CurrencyId) -> Result<()> {
        if !self.currencies.contains_key(&currency_id) {
            return Err(Error::UnknownCurrency(currency_id));
        }
        Ok(())
    }

    fn ensure_account(&self, account_id: AccountId) -> Result<()> {
        if !self.accounts.contains_key(&account_id) {
            return Err(Error::UnknownAccount(account_id));
        }
        Ok(())
    }

    fn ensure_book(&self, account_id: AccountId, book_id: BookId) -> Result<()> {
        if !self.books.contains(&(account_id, book_id)) {
            return Err(Error::UnknownBook {
                account_id,
                book_id,
            });
        }
        Ok(())
    }

    fn ensure_instrument(&self, instrument_id: InstrumentId) -> Result<&InstrumentMeta> {
        self.instruments
            .get(&instrument_id)
            .ok_or(Error::UnknownInstrument(instrument_id))
    }

    fn ensure_money(&self, money: Money, currency_id: CurrencyId) -> Result<()> {
        self.ensure_currency(currency_id)?;
        if money.currency_id != currency_id || money.scale != self.config.account_money_scale {
            return Err(Error::InvalidScale);
        }
        Ok(())
    }

    fn ensure_account_currency(
        &self,
        account_id: AccountId,
        currency_id: CurrencyId,
    ) -> Result<()> {
        let account = self
            .accounts
            .get(&account_id)
            .ok_or(Error::UnknownAccount(account_id))?;
        if account.base_currency != currency_id {
            return Err(Error::CurrencyMismatch {
                money_currency: currency_id,
                expected_currency: account.base_currency,
            });
        }
        Ok(())
    }

    fn convert_money(&self, money: Money, to_currency_id: CurrencyId) -> Result<Money> {
        if money.currency_id == to_currency_id {
            return money_from_components(
                money.amount,
                money.scale,
                to_currency_id,
                self.config.account_money_scale,
                self.config.rounding_mode,
            );
        }
        let rate = self
            .fx_rates
            .get(&(money.currency_id, to_currency_id))
            .ok_or(Error::MissingFxRate {
                from_currency: money.currency_id,
                to_currency: to_currency_id,
            })?
            .rate;
        convert_money_with_rate(
            money,
            to_currency_id,
            rate,
            self.config.account_money_scale,
            self.config.rounding_mode,
        )
    }

    fn ensure_conversion_rate(
        &self,
        from_currency_id: CurrencyId,
        to_currency_id: CurrencyId,
    ) -> Result<()> {
        if from_currency_id == to_currency_id
            || self
                .fx_rates
                .contains_key(&(from_currency_id, to_currency_id))
        {
            return Ok(());
        }
        Err(Error::MissingFxRate {
            from_currency: from_currency_id,
            to_currency: to_currency_id,
        })
    }
}
