use crate::error::{Error, Result};
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
pub struct MarkPriceUpdate {
    pub instrument_id: InstrumentId,
    pub price: Price,
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
    pub cash: Money,
    pub position_market_value: Money,
    pub equity: Money,
    pub realized_pnl: Money,
    pub unrealized_pnl: Money,
    pub total_pnl: Money,
    pub gross_exposure: Money,
    pub net_exposure: Money,
    pub peak_equity: Money,
    pub current_drawdown: Money,
    pub max_drawdown: Money,
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
    pub(crate) seen_events: BTreeSet<EventId>,
    pub(crate) last_seq: u64,
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
            seen_events: BTreeSet::new(),
            last_seq: 0,
        }
    }

    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    pub fn positions(&self) -> impl Iterator<Item = &Position> {
        self.positions.values()
    }

    pub fn register_currency(&mut self, meta: CurrencyMeta) -> Result<()> {
        if meta.currency_id != self.config.base_currency {
            return Err(Error::MultiCurrencyUnsupported {
                instrument_currency: meta.currency_id,
                account_currency: self.config.base_currency,
            });
        }
        if meta.scale != self.config.account_money_scale {
            return Err(Error::InvalidScale);
        }
        self.currencies.insert(meta.currency_id, meta);
        Ok(())
    }

    pub fn register_account(&mut self, meta: AccountMeta) -> Result<()> {
        self.ensure_currency(meta.base_currency)?;
        if meta.base_currency != self.config.base_currency {
            return Err(Error::MultiCurrencyUnsupported {
                instrument_currency: meta.base_currency,
                account_currency: self.config.base_currency,
            });
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
        self.ensure_account(meta.account_id)?;
        self.books.insert((meta.account_id, meta.book_id));
        Ok(())
    }

    pub fn register_instrument(&mut self, meta: InstrumentMeta) -> Result<()> {
        self.ensure_currency(meta.currency_id)?;
        if meta.currency_id != self.config.base_currency {
            return Err(Error::MultiCurrencyUnsupported {
                instrument_currency: meta.currency_id,
                account_currency: self.config.base_currency,
            });
        }
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
        self.validate_event_order(&event)?;
        if self.seen_events.contains(&event.event_id) {
            return Err(Error::DuplicateEvent(event.event_id));
        }

        let zero = Money::zero(self.config.base_currency, self.config.account_money_scale);
        let mut changed_positions = Vec::new();
        let mut cash_delta = zero;
        let mut realized_delta = zero;
        let mut drawdown_accounts = BTreeSet::new();

        match &event.kind {
            EventKind::InitialCash(initial) => {
                self.ensure_account(initial.account_id)?;
                self.ensure_money(initial.amount, initial.currency_id)?;
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
        }

        for account_id in drawdown_accounts {
            self.update_drawdown(account_id)?;
        }

        self.last_seq = event.seq;
        self.seen_events.insert(event.event_id);
        Ok(ApplyResult {
            sequence: self.last_seq,
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
        for position in self
            .positions
            .values()
            .filter(|p| p.key.account_id == account_id)
        {
            gross = gross.checked_add(position.gross_exposure)?;
            net = net.checked_add(position.net_exposure)?;
            unrealized = unrealized.checked_add(position.unrealized_pnl)?;
        }
        let equity = account.cash.checked_add(net)?;
        let total_pnl = account.realized_pnl.checked_add(unrealized)?;
        let expected_pnl = equity
            .checked_sub(account.initial_cash)?
            .checked_sub(account.net_external_cash_flows)?;
        let pnl_reconciliation_delta = expected_pnl.checked_sub(total_pnl)?;
        Ok(AccountSummary {
            account_id,
            cash: account.cash,
            position_market_value: net,
            equity,
            realized_pnl: account.realized_pnl,
            unrealized_pnl: unrealized,
            total_pnl,
            gross_exposure: gross,
            net_exposure: net,
            peak_equity: account.peak_equity,
            current_drawdown: account.current_drawdown,
            max_drawdown: account.max_drawdown,
            net_external_cash_flows: account.net_external_cash_flows,
            pnl_reconciliation_delta,
            state_hash: self.state_hash(),
        })
    }

    pub fn state_hash(&self) -> StateHash {
        StateHash::from_canonical(&CanonicalStateV1::from_engine(self))
    }

    fn apply_fill(&mut self, fill: &Fill, ts_unix_ns: i64) -> Result<(PositionKey, Money, Money)> {
        self.ensure_account(fill.account_id)?;
        self.ensure_book(fill.account_id, fill.book_id)?;
        let instrument = self.ensure_instrument(fill.instrument_id)?.clone();
        let account_currency = self.accounts.get(&fill.account_id).unwrap().base_currency;
        if instrument.currency_id != account_currency {
            return Err(Error::MultiCurrencyUnsupported {
                instrument_currency: instrument.currency_id,
                account_currency,
            });
        }
        self.ensure_money(fill.fee, account_currency)?;
        let qty = fill.qty.to_scale_exact(instrument.qty_scale)?;
        if qty.value <= 0 {
            return Err(Error::InvalidQuantity);
        }
        let price = fill
            .price
            .to_scale(instrument.price_scale, self.config.rounding_mode)?;

        let signed_delta = qty
            .value
            .checked_mul(fill.side.sign())
            .ok_or(Error::ArithmeticOverflow)?;
        let key = PositionKey {
            account_id: fill.account_id,
            book_id: fill.book_id,
            instrument_id: fill.instrument_id,
        };

        let signed_trade_value = value_qty_price_multiplier(
            signed_delta,
            qty.scale,
            price,
            instrument.multiplier,
            instrument.currency_id,
            self.config.account_money_scale,
            self.config.rounding_mode,
        )?;
        let signed_cash_trade = signed_trade_value.checked_neg()?;
        let cash_delta = signed_cash_trade.checked_sub(fill.fee)?;

        let zero = Money::zero(account_currency, self.config.account_money_scale);
        let mut position = self.positions.remove(&key).unwrap_or_else(|| Position {
            key,
            signed_qty: Qty::zero(instrument.qty_scale),
            avg_price: None,
            cost_basis: zero,
            realized_pnl: zero,
            unrealized_pnl: zero,
            gross_exposure: zero,
            net_exposure: zero,
            opened_at_unix_ns: None,
            updated_at_unix_ns: ts_unix_ns,
        });

        let old_qty = position.signed_qty.value;
        let new_qty = old_qty
            .checked_add(signed_delta)
            .ok_or(Error::ArithmeticOverflow)?;
        if !self.config.allow_short && new_qty < 0 {
            return Err(Error::ShortPositionNotAllowed);
        }
        if !self.config.allow_position_flip
            && old_qty != 0
            && old_qty.signum() != new_qty.signum()
            && new_qty != 0
        {
            return Err(Error::PositionFlipNotAllowed);
        }

        let mut realized_delta = fill.fee.checked_neg()?;
        if old_qty == 0 || old_qty.signum() == signed_delta.signum() {
            position.avg_price = Some(if old_qty == 0 {
                price
            } else {
                weighted_avg_price(
                    old_qty.abs(),
                    position.avg_price.unwrap(),
                    qty.value,
                    price,
                    instrument.price_scale,
                    self.config.rounding_mode,
                )?
            });
            if old_qty == 0 {
                position.opened_at_unix_ns = Some(ts_unix_ns);
            }
            position.cost_basis = position.cost_basis.checked_add(signed_trade_value)?;
        } else {
            let closed_qty = old_qty.abs().min(qty.value);
            let old_cost_basis = position.cost_basis;
            let closed_basis_amount = div_round(
                old_cost_basis
                    .amount
                    .checked_mul(closed_qty)
                    .ok_or(Error::ArithmeticOverflow)?,
                old_qty.abs(),
                self.config.rounding_mode,
            )?;
            let closed_basis = Money::new(
                closed_basis_amount,
                self.config.account_money_scale,
                account_currency,
            );
            let closing_signed_qty = closed_qty
                .checked_mul(signed_delta.signum())
                .ok_or(Error::ArithmeticOverflow)?;
            let closing_trade_value = value_qty_price_multiplier(
                closing_signed_qty,
                qty.scale,
                price,
                instrument.multiplier,
                instrument.currency_id,
                self.config.account_money_scale,
                self.config.rounding_mode,
            )?;
            let closing_cash_trade = closing_trade_value.checked_neg()?;
            let realized = closing_cash_trade.checked_sub(closed_basis)?;
            realized_delta = realized_delta.checked_add(realized)?;
            if new_qty == 0 {
                position.avg_price = None;
                position.cost_basis = zero;
                position.opened_at_unix_ns = None;
            } else if old_qty.signum() != new_qty.signum() {
                position.avg_price = Some(price);
                position.cost_basis = value_qty_price_multiplier(
                    new_qty,
                    qty.scale,
                    price,
                    instrument.multiplier,
                    instrument.currency_id,
                    self.config.account_money_scale,
                    self.config.rounding_mode,
                )?;
                position.opened_at_unix_ns = Some(ts_unix_ns);
            } else {
                position.cost_basis = old_cost_basis.checked_sub(closed_basis)?;
            }
        }

        position.signed_qty = Qty::new(new_qty, instrument.qty_scale);
        position.realized_pnl = position.realized_pnl.checked_add(realized_delta)?;
        position.updated_at_unix_ns = ts_unix_ns;
        self.revalue_position(&mut position, &instrument)?;

        let account = self.accounts.get_mut(&fill.account_id).unwrap();
        account.cash = account.cash.checked_add(cash_delta)?;
        account.realized_pnl = account.realized_pnl.checked_add(realized_delta)?;

        self.positions.insert(key, position);
        Ok((key, cash_delta, realized_delta))
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
        self.marks.insert(
            mark.instrument_id,
            Mark {
                instrument_id: mark.instrument_id,
                price,
                ts_unix_ns,
            },
        );

        let keys: Vec<_> = self
            .positions
            .keys()
            .copied()
            .filter(|key| key.instrument_id == mark.instrument_id)
            .collect();
        for key in keys {
            let mut position = self.positions.remove(&key).unwrap();
            self.revalue_position(&mut position, &instrument)?;
            drawdown_accounts.insert(key.account_id);
            changed_positions.push(key);
            self.positions.insert(key, position);
        }
        Ok(())
    }

    fn revalue_position(&self, position: &mut Position, instrument: &InstrumentMeta) -> Result<()> {
        let zero = Money::zero(instrument.currency_id, self.config.account_money_scale);
        if position.signed_qty.value == 0 {
            position.unrealized_pnl = zero;
            position.gross_exposure = zero;
            position.net_exposure = zero;
            return Ok(());
        }
        let valuation_price = self.marks.get(&position.key.instrument_id).map(|m| m.price);
        position.net_exposure = if let Some(valuation_price) = valuation_price {
            value_qty_price_multiplier(
                position.signed_qty.value,
                position.signed_qty.scale,
                valuation_price,
                instrument.multiplier,
                instrument.currency_id,
                self.config.account_money_scale,
                self.config.rounding_mode,
            )?
        } else {
            position.cost_basis
        };
        position.gross_exposure = position.net_exposure.abs();
        position.unrealized_pnl = position.net_exposure.checked_sub(position.cost_basis)?;
        Ok(())
    }

    fn update_drawdown(&mut self, account_id: AccountId) -> Result<()> {
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
        for position in self
            .positions
            .values()
            .filter(|p| p.key.account_id == account_id)
        {
            gross = gross.checked_add(position.gross_exposure)?;
            net = net.checked_add(position.net_exposure)?;
            unrealized = unrealized.checked_add(position.unrealized_pnl)?;
        }
        let equity = account.cash.checked_add(net)?;
        let total_pnl = account.realized_pnl.checked_add(unrealized)?;
        let expected_pnl = equity
            .checked_sub(account.initial_cash)?
            .checked_sub(account.net_external_cash_flows)?;
        let pnl_reconciliation_delta = expected_pnl.checked_sub(total_pnl)?;
        Ok(AccountSummary {
            account_id,
            cash: account.cash,
            position_market_value: net,
            equity,
            realized_pnl: account.realized_pnl,
            unrealized_pnl: unrealized,
            total_pnl,
            gross_exposure: gross,
            net_exposure: net,
            peak_equity: account.peak_equity,
            current_drawdown: account.current_drawdown,
            max_drawdown: account.max_drawdown,
            net_external_cash_flows: account.net_external_cash_flows,
            pnl_reconciliation_delta,
            state_hash: StateHash::zero(),
        })
    }

    fn validate_event_order(&self, event: &Event) -> Result<()> {
        let expected = if self.last_seq == 0 {
            self.config.expected_start_seq
        } else {
            self.last_seq
                .checked_add(1)
                .ok_or(Error::ArithmeticOverflow)?
        };
        if event.seq != expected {
            return Err(Error::OutOfOrderEvent {
                expected,
                received: event.seq,
            });
        }
        Ok(())
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
}

fn weighted_avg_price(
    old_abs_qty: i128,
    old_avg: Price,
    add_abs_qty: i128,
    add_price: Price,
    price_scale: u8,
    rounding: RoundingMode,
) -> Result<Price> {
    let old_price = old_avg.to_scale(price_scale, rounding)?;
    let new_price = add_price.to_scale(price_scale, rounding)?;
    let old_cost = old_abs_qty
        .checked_mul(old_price.value)
        .ok_or(Error::ArithmeticOverflow)?;
    let add_cost = add_abs_qty
        .checked_mul(new_price.value)
        .ok_or(Error::ArithmeticOverflow)?;
    let total_cost = old_cost
        .checked_add(add_cost)
        .ok_or(Error::ArithmeticOverflow)?;
    let total_qty = old_abs_qty
        .checked_add(add_abs_qty)
        .ok_or(Error::ArithmeticOverflow)?;
    Ok(Price::new(
        div_round(total_cost, total_qty, rounding)?,
        price_scale,
    ))
}
