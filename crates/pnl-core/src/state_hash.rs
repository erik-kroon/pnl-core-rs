use crate::account::AccountState;
use crate::config::EngineConfig;
use crate::engine::Engine;
use crate::event::Event;
use crate::metadata::{BookMeta, CurrencyMeta, InstrumentLifecycleMeta, InstrumentMeta};
use crate::position::{FxRate, Lot, Mark, Position};
use crate::types::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateHash(pub [u8; 32]);

impl StateHash {
    pub const fn zero() -> Self {
        Self([0; 32])
    }

    pub fn to_hex(self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalStateV2 {
    pub config: EngineConfig,
    pub currencies: Vec<CurrencyMeta>,
    pub accounts: Vec<AccountState>,
    pub books: Vec<BookMeta>,
    pub instruments: Vec<InstrumentMeta>,
    pub instrument_lifecycles: Vec<InstrumentLifecycleMeta>,
    pub positions: Vec<Position>,
    pub lots: Vec<Lot>,
    pub marks: Vec<Mark>,
    pub fx_rates: Vec<FxRate>,
    pub seen_events: Vec<EventId>,
    pub event_log: Vec<Event>,
    pub last_seq: u64,
}

impl CanonicalStateV2 {
    pub(crate) fn from_engine(engine: &Engine) -> Self {
        Self {
            config: engine.config.clone(),
            currencies: engine.registry.currencies().cloned().collect(),
            accounts: engine.accounts.values().cloned().collect(),
            books: engine.registry.books().collect(),
            instruments: engine.registry.instruments().cloned().collect(),
            instrument_lifecycles: engine.registry.instrument_lifecycles().collect(),
            positions: engine.positions.values().cloned().collect(),
            lots: engine.lots.values().cloned().collect(),
            marks: engine.marks.values().cloned().collect(),
            fx_rates: engine.fx_rates.values().cloned().collect(),
            seen_events: engine
                .replay_journal
                .seen_events()
                .iter()
                .copied()
                .collect(),
            event_log: engine.replay_journal.events().to_vec(),
            last_seq: engine.replay_journal.last_seq(),
        }
    }
}

pub(crate) fn hash_engine_state(engine: &Engine) -> StateHash {
    hash_canonical_state(&CanonicalStateV2::from_engine(engine))
}

pub(crate) fn hash_canonical_state(state: &CanonicalStateV2) -> StateHash {
    let bytes = postcard::to_allocvec(state).expect("canonical state should serialize");
    StateHash(*blake3::hash(&bytes).as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EngineConfig;
    use crate::engine::Engine;
    use crate::event::{Event, EventKind, InitialCash};
    use crate::metadata::{AccountMeta, BookMeta, CurrencyMeta};

    fn registered_engine() -> Engine {
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
    }

    fn initial_cash(seq: u64, event_id: EventId) -> Event {
        Event {
            seq,
            event_id,
            ts_unix_ns: seq as i64,
            kind: EventKind::InitialCash(InitialCash {
                account_id: AccountId(1),
                currency_id: CurrencyId::usd(),
                amount: Money::parse_decimal("100.00", CurrencyId::usd(), ACCOUNT_MONEY_SCALE)
                    .unwrap(),
            }),
        }
    }

    #[test]
    fn hash_engine_state_is_stable_for_same_canonical_material() {
        let mut a = registered_engine();
        let mut b = registered_engine();

        a.apply(initial_cash(1, EventId(1))).unwrap();
        b.apply(initial_cash(1, EventId(1))).unwrap();

        assert_eq!(hash_engine_state(&a), hash_engine_state(&b));
    }

    #[test]
    fn hash_engine_state_includes_replay_journal_material() {
        let mut a = registered_engine();
        let mut b = registered_engine();

        a.apply(initial_cash(1, EventId(1))).unwrap();
        b.apply(initial_cash(1, EventId(99))).unwrap();

        let a_summary = a.account_summary(AccountId(1)).unwrap();
        let b_summary = b.account_summary(AccountId(1)).unwrap();
        assert_eq!(a_summary.cash, b_summary.cash);
        assert_eq!(a_summary.equity, b_summary.equity);
        assert_eq!(a_summary.realized_pnl, b_summary.realized_pnl);
        assert_eq!(a_summary.open_positions, b_summary.open_positions);
        assert_ne!(hash_engine_state(&a), hash_engine_state(&b));
    }

    #[test]
    fn apply_receipt_does_not_compute_state_hash() {
        let mut engine = registered_engine();
        let before = hash_engine_state(&engine);

        let result = engine.apply(initial_cash(1, EventId(1))).unwrap();

        assert_eq!(result.sequence, 1);
        assert_ne!(hash_engine_state(&engine), before);
    }

    #[test]
    fn apply_many_reports_one_final_state_hash() {
        let mut engine = registered_engine();

        let report = engine.apply_many([initial_cash(1, EventId(1))]).unwrap();

        assert_eq!(report.applied, 1);
        assert_eq!(report.last_sequence, 1);
        assert_eq!(report.state_hash, hash_engine_state(&engine));
    }
}
