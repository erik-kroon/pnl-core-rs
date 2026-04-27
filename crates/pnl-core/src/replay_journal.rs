//! Replay ownership for accepted events.
//!
//! The journal validates event order and duplicate IDs, retains the canonical
//! event log, applies correction/bust override rules, and coordinates
//! deterministic accounting rebuilds after a history rewrite.

use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::event::{Event, EventKind, Fill};
use crate::position::PositionKey;
use crate::summary::ApplyReceipt;
use crate::types::*;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct ReplayJournal {
    seen_events: BTreeSet<EventId>,
    events: Vec<Event>,
    last_seq: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JournalAction {
    ApplyAccounting,
    RewriteHistory,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RewriteTarget {
    account_id: AccountId,
    position_key: PositionKey,
}

pub(crate) fn apply_event(engine: &mut Engine, event: Event) -> Result<ApplyReceipt> {
    let action = engine
        .replay_journal
        .prepare(engine.config.expected_start_seq, &event)?;
    match action {
        JournalAction::ApplyAccounting => apply_accounting_event(engine, event),
        JournalAction::RewriteHistory => apply_history_rewrite(engine, event),
    }
}

impl ReplayJournal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_parts(seen_events: BTreeSet<EventId>, events: Vec<Event>, last_seq: u64) -> Self {
        Self {
            seen_events,
            events,
            last_seq,
        }
    }

    pub fn seen_events(&self) -> &BTreeSet<EventId> {
        &self.seen_events
    }

    pub fn events(&self) -> &[Event] {
        &self.events
    }

    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }

    pub fn validate_restored(&self, expected_start_seq: u64) -> Result<()> {
        let mut replay = Self::new();
        for event in &self.events {
            replay.validate_next_event(expected_start_seq, event)?;
            replay.record_replayed_event(event);
        }
        if replay.seen_events != self.seen_events || replay.last_seq != self.last_seq {
            return Err(Error::SnapshotValidation("journal state invalid"));
        }
        Ok(())
    }

    fn prepare(&self, expected_start_seq: u64, event: &Event) -> Result<JournalAction> {
        self.validate_next_event(expected_start_seq, event)?;
        if matches!(
            event.kind,
            EventKind::TradeCorrection(_) | EventKind::TradeBust(_)
        ) {
            Ok(JournalAction::RewriteHistory)
        } else {
            Ok(JournalAction::ApplyAccounting)
        }
    }

    fn record_accepted(&mut self, event: Event) {
        self.last_seq = event.seq;
        self.seen_events.insert(event.event_id);
        self.events.push(event);
    }

    fn validate_rewrite_target(&self, event: &Event) -> Result<RewriteTarget> {
        let (original_event_id, replacement) = match &event.kind {
            EventKind::TradeCorrection(correction) => {
                (correction.original_event_id, Some(&correction.replacement))
            }
            EventKind::TradeBust(bust) => (bust.original_event_id, None),
            _ => unreachable!("history rewrite only handles correction and bust events"),
        };
        let original = self.original_fill(original_event_id)?;
        if let Some(replacement) = replacement {
            ensure_same_fill_key(original, replacement)?;
        }
        Ok(RewriteTarget {
            account_id: original.account_id,
            position_key: fill_key(original),
        })
    }

    fn rebuild_canonical_accounting_state(&self, engine: &Engine) -> Result<Engine> {
        let overrides = correction_overrides(&self.events)?;
        let mut rebuilt = engine.clone();
        let mut replayed = Self::new();

        reset_canonical_accounting_state(&mut rebuilt);

        for event in &self.events {
            replayed.validate_next_event(rebuilt.config.expected_start_seq, event)?;
            if let Some(kind) = canonical_accounting_kind(event, &overrides) {
                rebuilt.apply_accounting_event_without_journal(event, &kind)?;
            }
            replayed.record_replayed_event(event);
        }

        if replayed.seen_events != self.seen_events || replayed.last_seq != self.last_seq {
            return Err(Error::SnapshotValidation("journal state invalid"));
        }

        rebuilt.replay_journal = self.clone();
        Ok(rebuilt)
    }

    fn validate_next_event(&self, expected_start_seq: u64, event: &Event) -> Result<()> {
        let expected = if self.last_seq == 0 {
            expected_start_seq
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
        if self.seen_events.contains(&event.event_id) {
            return Err(Error::DuplicateEvent(event.event_id));
        }
        Ok(())
    }

    fn record_replayed_event(&mut self, event: &Event) {
        self.last_seq = event.seq;
        self.seen_events.insert(event.event_id);
    }

    fn original_fill(&self, event_id: EventId) -> Result<&Fill> {
        let event = self
            .events
            .iter()
            .find(|event| event.event_id == event_id)
            .ok_or(Error::UnknownOriginalEvent(event_id))?;
        match &event.kind {
            EventKind::Fill(fill) => Ok(fill),
            _ => Err(Error::CorrectionTargetNotFill(event_id)),
        }
    }
}

fn apply_accounting_event(engine: &mut Engine, event: Event) -> Result<ApplyReceipt> {
    let effect = engine.apply_accounting_event_without_journal(&event, &event.kind)?;

    engine.replay_journal.record_accepted(event);
    Ok(effect.into_apply_receipt(engine.replay_journal.last_seq()))
}

fn apply_history_rewrite(engine: &mut Engine, event: Event) -> Result<ApplyReceipt> {
    let target = engine.replay_journal.validate_rewrite_target(&event)?;
    let before_account = engine
        .accounts
        .get(&target.account_id)
        .ok_or(Error::UnknownAccount(target.account_id))?
        .clone();

    let mut next = engine.clone();
    next.replay_journal.record_accepted(event);
    next = next
        .replay_journal
        .rebuild_canonical_accounting_state(&next)?;

    let after_account = next
        .accounts
        .get(&target.account_id)
        .ok_or(Error::UnknownAccount(target.account_id))?;
    let cash_delta = after_account.cash.checked_sub(before_account.cash)?;
    let realized_delta = after_account
        .realized_pnl
        .checked_sub(before_account.realized_pnl)?;
    let sequence = next.replay_journal.last_seq();
    *engine = next;

    Ok(ApplyReceipt {
        sequence,
        changed_positions: vec![target.position_key],
        realized_pnl_delta: realized_delta,
        cash_delta,
    })
}

fn correction_overrides(events: &[Event]) -> Result<BTreeMap<EventId, Option<Fill>>> {
    let mut original_fills = BTreeMap::new();
    let mut overrides = BTreeMap::new();

    for event in events {
        match &event.kind {
            EventKind::Fill(fill) => {
                original_fills.insert(event.event_id, fill.clone());
            }
            EventKind::TradeCorrection(correction) => {
                let original = original_fills
                    .get(&correction.original_event_id)
                    .ok_or(Error::UnknownOriginalEvent(correction.original_event_id))?;
                ensure_same_fill_key(original, &correction.replacement)?;
                overrides.insert(
                    correction.original_event_id,
                    Some(correction.replacement.clone()),
                );
            }
            EventKind::TradeBust(bust) => {
                if !original_fills.contains_key(&bust.original_event_id) {
                    return Err(Error::UnknownOriginalEvent(bust.original_event_id));
                }
                overrides.insert(bust.original_event_id, None);
            }
            _ => {}
        }
    }

    Ok(overrides)
}

fn canonical_accounting_kind(
    event: &Event,
    overrides: &BTreeMap<EventId, Option<Fill>>,
) -> Option<EventKind> {
    match &event.kind {
        EventKind::Fill(_) => match overrides.get(&event.event_id) {
            Some(Some(replacement)) => Some(EventKind::Fill(replacement.clone())),
            Some(None) => None,
            None => Some(event.kind.clone()),
        },
        EventKind::TradeCorrection(_) | EventKind::TradeBust(_) => None,
        _ => Some(event.kind.clone()),
    }
}

fn reset_canonical_accounting_state(engine: &mut Engine) {
    for account in engine.accounts.values_mut() {
        let zero = Money::zero(account.base_currency, engine.config.account_money_scale);
        account.initial_cash = zero;
        account.cash = zero;
        account.net_external_cash_flows = zero;
        account.trading_realized_pnl = zero;
        account.interest_pnl = zero;
        account.borrow_pnl = zero;
        account.funding_pnl = zero;
        account.financing_pnl = zero;
        account.total_financing_pnl = zero;
        account.realized_pnl = zero;
        account.peak_equity = zero;
        account.current_drawdown = zero;
        account.max_drawdown = zero;
        account.initial_cash_set = false;
    }
    engine.positions.clear();
    engine.lots.clear();
    engine.marks.clear();
    engine.fx_rates.clear();
    engine.registry.reset_instrument_lifecycles();
}

fn ensure_same_fill_key(original: &Fill, replacement: &Fill) -> Result<()> {
    if original.account_id != replacement.account_id
        || original.book_id != replacement.book_id
        || original.instrument_id != replacement.instrument_id
    {
        return Err(Error::CorrectionKeyMismatch);
    }
    Ok(())
}

fn fill_key(fill: &Fill) -> PositionKey {
    PositionKey {
        account_id: fill.account_id,
        book_id: fill.book_id,
        instrument_id: fill.instrument_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EngineConfig;
    use crate::event::{InitialCash, MarkPriceUpdate};
    use crate::metadata::{AccountMeta, BookMeta, CurrencyMeta, InstrumentMeta};

    fn money(value: &str) -> Money {
        Money::parse_decimal(value, CurrencyId::usd(), ACCOUNT_MONEY_SCALE).unwrap()
    }

    fn setup_engine() -> Engine {
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

    fn fill(price: i128) -> Fill {
        Fill {
            account_id: AccountId(1),
            book_id: BookId(1),
            instrument_id: InstrumentId(1),
            side: Side::Buy,
            qty: Qty::from_units(10),
            price: Price::new(price, 0),
            fee: Money::zero(CurrencyId::usd(), ACCOUNT_MONEY_SCALE),
        }
    }

    fn initial_event() -> Event {
        Event {
            seq: 1,
            event_id: EventId(1),
            ts_unix_ns: 1,
            kind: EventKind::InitialCash(InitialCash {
                account_id: AccountId(1),
                currency_id: CurrencyId::usd(),
                amount: money("1000.00"),
            }),
        }
    }

    fn fill_event(seq: u64, event_id: EventId, fill: Fill) -> Event {
        Event {
            seq,
            event_id,
            ts_unix_ns: seq as i64,
            kind: EventKind::Fill(fill),
        }
    }

    fn mark_event(seq: u64, event_id: EventId, price: i128) -> Event {
        Event {
            seq,
            event_id,
            ts_unix_ns: seq as i64,
            kind: EventKind::Mark(MarkPriceUpdate {
                instrument_id: InstrumentId(1),
                price: Price::new(price, 0),
            }),
        }
    }

    fn correction_event(seq: u64, event_id: EventId, replacement: Fill) -> Event {
        Event {
            seq,
            event_id,
            ts_unix_ns: seq as i64,
            kind: EventKind::TradeCorrection(crate::event::TradeCorrection {
                original_event_id: EventId(10),
                replacement,
                reason: None,
            }),
        }
    }

    fn bust_event(seq: u64, event_id: EventId) -> Event {
        Event {
            seq,
            event_id,
            ts_unix_ns: seq as i64,
            kind: EventKind::TradeBust(crate::event::TradeBust {
                original_event_id: EventId(10),
                reason: None,
            }),
        }
    }

    #[test]
    fn prepare_rejects_duplicate_event_ids() {
        let mut journal = ReplayJournal::new();
        journal.record_accepted(fill_event(1, EventId(10), fill(10)));

        let err = journal
            .prepare(1, &fill_event(2, EventId(10), fill(11)))
            .unwrap_err();

        assert_eq!(err, Error::DuplicateEvent(EventId(10)));
    }

    #[test]
    fn correction_overrides_keep_the_latest_rewrite() {
        let replacement = fill(11);
        let events = vec![
            fill_event(1, EventId(10), fill(10)),
            correction_event(2, EventId(11), replacement),
            bust_event(3, EventId(12)),
        ];

        let overrides = correction_overrides(&events).unwrap();

        assert_eq!(overrides.get(&EventId(10)), Some(&None));
    }

    #[test]
    fn rebuild_canonical_accounting_state_applies_latest_rewrite_from_journal() {
        let mut engine = setup_engine();
        let replacement = fill(9);
        let events = vec![
            initial_event(),
            fill_event(2, EventId(10), fill(10)),
            correction_event(3, EventId(11), replacement),
            mark_event(4, EventId(12), 12),
        ];
        for event in events.clone() {
            engine.replay_journal.record_accepted(event);
        }

        let rebuilt = engine
            .replay_journal
            .rebuild_canonical_accounting_state(&engine)
            .unwrap();

        assert_eq!(rebuilt.replay_journal.events(), events.as_slice());
        assert_eq!(rebuilt.replay_journal.last_seq(), 4);
        assert_eq!(
            rebuilt.account_summary(AccountId(1)).unwrap().cash,
            money("910.00")
        );
        assert_eq!(
            rebuilt.position(fill_key(&fill(10))).unwrap().cost_basis,
            money("90.00")
        );
        assert_eq!(
            rebuilt
                .account_summary(AccountId(1))
                .unwrap()
                .unrealized_pnl,
            money("30.00")
        );
    }

    #[test]
    fn rewrite_target_requires_same_position_key() {
        let mut journal = ReplayJournal::new();
        journal.record_accepted(fill_event(1, EventId(10), fill(10)));

        let mut replacement = fill(11);
        replacement.instrument_id = InstrumentId(2);
        let err = journal
            .validate_rewrite_target(&correction_event(2, EventId(11), replacement))
            .unwrap_err();

        assert_eq!(err, Error::CorrectionKeyMismatch);
    }
}
