use crate::account::AccountState;
use crate::config::EngineConfig;
use crate::engine::Engine;
use crate::event::Event;
use crate::metadata::{AccountMeta, BookMeta, CurrencyMeta, InstrumentMeta};
use crate::position::{FxRate, Mark, Position};
use crate::registry::Registry;
use crate::replay_journal::ReplayJournal;
use crate::types::EventId;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalStateV1 {
    pub config: EngineConfig,
    pub currencies: Vec<CurrencyMeta>,
    pub accounts: Vec<AccountState>,
    pub books: Vec<BookMeta>,
    pub instruments: Vec<InstrumentMeta>,
    pub positions: Vec<Position>,
    pub marks: Vec<Mark>,
    pub fx_rates: Vec<FxRate>,
    pub seen_events: Vec<EventId>,
    pub event_log: Vec<Event>,
    pub last_seq: u64,
}

impl CanonicalStateV1 {
    pub(crate) fn from_engine(engine: &Engine) -> Self {
        Self {
            config: engine.config.clone(),
            currencies: engine.registry.currencies().cloned().collect(),
            accounts: engine.accounts.values().cloned().collect(),
            books: engine.registry.books().collect(),
            instruments: engine.registry.instruments().cloned().collect(),
            positions: engine.positions.values().cloned().collect(),
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

    pub(super) fn into_engine(self) -> Engine {
        let accounts = self.accounts;
        let registry = Registry::from_parts(
            self.currencies
                .into_iter()
                .map(|meta| (meta.currency_id, meta))
                .collect(),
            accounts
                .iter()
                .map(|account| {
                    (
                        account.account_id,
                        AccountMeta {
                            account_id: account.account_id,
                            base_currency: account.base_currency,
                        },
                    )
                })
                .collect(),
            self.books
                .into_iter()
                .map(|book| (book.account_id, book.book_id))
                .collect(),
            self.instruments
                .into_iter()
                .map(|meta| (meta.instrument_id, meta))
                .collect(),
        );
        Engine {
            config: self.config,
            registry,
            accounts: accounts
                .into_iter()
                .map(|account| (account.account_id, account))
                .collect(),
            positions: self.positions.into_iter().map(|p| (p.key, p)).collect(),
            marks: self
                .marks
                .into_iter()
                .map(|m| (m.instrument_id, m))
                .collect(),
            fx_rates: self
                .fx_rates
                .into_iter()
                .map(|rate| ((rate.from_currency_id, rate.to_currency_id), rate))
                .collect(),
            replay_journal: ReplayJournal::from_parts(
                self.seen_events.into_iter().collect(),
                self.event_log,
                self.last_seq,
            ),
        }
    }
}
