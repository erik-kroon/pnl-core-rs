use crate::account::AccountState;
use crate::config::EngineConfig;
use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::event::Event;
use crate::metadata::{AccountMeta, BookMeta, CurrencyMeta, InstrumentMeta};
use crate::position::{FxRate, Mark, Position};
use crate::registry::Registry;
use crate::replay_journal::ReplayJournal;
use crate::types::*;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

const SNAPSHOT_MAGIC: &[u8; 8] = b"PNLRS001";
const SNAPSHOT_VERSION: u16 = 1;
const SNAPSHOT_CODEC_POSTCARD: u8 = 1;
const SNAPSHOT_COMPRESSION_NONE: u8 = 0;
const HEADER_LEN: usize = 56;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateHash(pub [u8; 32]);

impl StateHash {
    pub const fn zero() -> Self {
        Self([0; 32])
    }

    pub(crate) fn from_canonical(state: &CanonicalStateV1) -> Self {
        let bytes = postcard::to_allocvec(state).expect("canonical state should serialize");
        Self(*blake3::hash(&bytes).as_bytes())
    }

    pub fn to_hex(self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMetadataV1 {
    pub snapshot_sequence: u64,
    pub last_applied_event_seq: u64,
    pub state_hash: StateHash,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotV1 {
    pub metadata: SnapshotMetadataV1,
    pub state: CanonicalStateV1,
}

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

    fn into_engine(self) -> Engine {
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

impl Engine {
    pub fn snapshot(&self) -> Result<SnapshotV1> {
        let state = CanonicalStateV1::from_engine(self);
        let state_hash = StateHash::from_canonical(&state);
        Ok(SnapshotV1 {
            metadata: SnapshotMetadataV1 {
                snapshot_sequence: self.replay_journal.last_seq(),
                last_applied_event_seq: self.replay_journal.last_seq(),
                state_hash,
            },
            state,
        })
    }

    pub fn restore(snapshot: SnapshotV1) -> Result<Self> {
        let hash = StateHash::from_canonical(&snapshot.state);
        if hash != snapshot.metadata.state_hash {
            return Err(Error::SnapshotHashMismatch);
        }
        let engine = snapshot.state.into_engine();
        engine.validate_restored_state()?;
        Ok(engine)
    }

    pub fn write_snapshot<W: Write>(&self, mut writer: W) -> Result<()> {
        let snapshot = self.snapshot()?;
        let payload = postcard::to_allocvec(&snapshot)?;
        let payload_hash = *blake3::hash(&payload).as_bytes();
        let mut header = Vec::with_capacity(HEADER_LEN);
        header.extend_from_slice(SNAPSHOT_MAGIC);
        header.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
        header.push(SNAPSHOT_CODEC_POSTCARD);
        header.push(SNAPSHOT_COMPRESSION_NONE);
        header.extend_from_slice(&0_u32.to_le_bytes());
        header.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        header.extend_from_slice(&payload_hash);
        debug_assert_eq!(header.len(), HEADER_LEN);
        writer.write_all(&header)?;
        writer.write_all(&payload)?;
        Ok(())
    }

    pub fn read_snapshot<R: Read>(mut reader: R) -> Result<Self> {
        let mut header = [0_u8; HEADER_LEN];
        reader.read_exact(&mut header)?;
        if &header[0..8] != SNAPSHOT_MAGIC {
            return Err(Error::SnapshotValidation("invalid magic"));
        }
        let version = u16::from_le_bytes([header[8], header[9]]);
        if version != SNAPSHOT_VERSION {
            return Err(Error::SnapshotVersionUnsupported(version));
        }
        if header[10] != SNAPSHOT_CODEC_POSTCARD || header[11] != SNAPSHOT_COMPRESSION_NONE {
            return Err(Error::SnapshotValidation(
                "unsupported codec or compression",
            ));
        }
        let payload_len = u64::from_le_bytes(
            header[16..24]
                .try_into()
                .map_err(|_| Error::SnapshotValidation("invalid payload length"))?,
        ) as usize;
        let expected_hash: [u8; 32] = header[24..56]
            .try_into()
            .map_err(|_| Error::SnapshotValidation("invalid hash"))?;
        let mut payload = vec![0_u8; payload_len];
        reader.read_exact(&mut payload)?;
        if *blake3::hash(&payload).as_bytes() != expected_hash {
            return Err(Error::SnapshotHashMismatch);
        }
        let snapshot: SnapshotV1 = postcard::from_bytes(&payload)?;
        Self::restore(snapshot)
    }

    pub fn write_snapshot_json<W: Write>(&self, writer: W) -> Result<()> {
        let snapshot = self.snapshot()?;
        serde_json::to_writer_pretty(writer, &snapshot)?;
        Ok(())
    }

    fn validate_restored_state(&self) -> Result<()> {
        if self
            .registry
            .ensure_currency(self.config.base_currency)
            .is_err()
        {
            return Err(Error::SnapshotValidation("missing base currency"));
        }
        for account in self.accounts.values() {
            if self.registry.ensure_account(account.account_id).is_err()
                || self
                    .registry
                    .account_currency(account.account_id)
                    .is_ok_and(|currency| currency != account.base_currency)
            {
                return Err(Error::SnapshotValidation("account metadata invalid"));
            }
            if self
                .registry
                .ensure_currency(account.base_currency)
                .is_err()
            {
                return Err(Error::SnapshotValidation("account currency missing"));
            }
            if account.cash.currency_id != account.base_currency
                || account.cash.scale != self.config.account_money_scale
            {
                return Err(Error::SnapshotValidation("invalid account cash"));
            }
        }
        for book in self.registry.books() {
            let account_id = book.account_id;
            let book_id = book.book_id;
            if !self.accounts.contains_key(&account_id) {
                return Err(Error::SnapshotValidation("book references missing account"));
            }
            if book_id.0 == 0 {
                return Err(Error::SnapshotValidation("invalid book id"));
            }
        }
        for instrument in self.registry.instruments() {
            if self
                .registry
                .ensure_currency(instrument.currency_id)
                .is_err()
            {
                return Err(Error::SnapshotValidation("instrument currency missing"));
            }
        }
        for rate in self.fx_rates.values() {
            if self
                .registry
                .ensure_currency(rate.from_currency_id)
                .is_err()
                || self.registry.ensure_currency(rate.to_currency_id).is_err()
                || rate.rate.value <= 0
            {
                return Err(Error::SnapshotValidation("invalid fx rate"));
            }
        }
        for position in self.positions.values() {
            if !self.accounts.contains_key(&position.key.account_id)
                || self
                    .registry
                    .ensure_book(position.key.account_id, position.key.book_id)
                    .is_err()
                || self
                    .registry
                    .instrument(position.key.instrument_id)
                    .is_err()
            {
                return Err(Error::SnapshotValidation("position reference invalid"));
            }
            if position.signed_qty.value == 0 && position.avg_price.is_some() {
                return Err(Error::SnapshotValidation("flat position has avg price"));
            }
            if position.signed_qty.value != 0 && position.avg_price.is_none() {
                return Err(Error::SnapshotValidation("open position missing avg price"));
            }
            let account = self.accounts.get(&position.key.account_id).unwrap();
            for money in [
                position.cost_basis,
                position.realized_pnl,
                position.unrealized_pnl,
                position.gross_exposure,
                position.net_exposure,
            ] {
                if money.currency_id != account.base_currency
                    || money.scale != self.config.account_money_scale
                {
                    return Err(Error::SnapshotValidation("position money invalid"));
                }
            }
        }
        self.replay_journal
            .validate_restored(self.config.expected_start_seq)?;
        Ok(())
    }
}
