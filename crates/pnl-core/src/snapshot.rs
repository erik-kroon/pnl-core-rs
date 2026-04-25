mod codec;
mod validation;

use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::metadata::AccountMeta;
use crate::registry::Registry;
use crate::replay_journal::ReplayJournal;
use crate::state_hash::{hash_canonical_state, CanonicalStateV1, StateHash};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

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

impl CanonicalStateV1 {
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
        let state_hash = hash_canonical_state(&state);
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
        let hash = hash_canonical_state(&snapshot.state);
        if hash != snapshot.metadata.state_hash {
            return Err(Error::SnapshotHashMismatch);
        }
        let engine = snapshot.state.into_engine();
        validation::validate_restored_state(&engine)?;
        Ok(engine)
    }

    pub fn write_snapshot<W: Write>(&self, writer: W) -> Result<()> {
        let snapshot = self.snapshot()?;
        codec::write_snapshot(&snapshot, writer)
    }

    pub fn read_snapshot<R: Read>(reader: R) -> Result<Self> {
        let snapshot = codec::read_snapshot(reader)?;
        Self::restore(snapshot)
    }

    pub fn write_snapshot_json<W: Write>(&self, writer: W) -> Result<()> {
        let snapshot = self.snapshot()?;
        serde_json::to_writer_pretty(writer, &snapshot)?;
        Ok(())
    }
}
