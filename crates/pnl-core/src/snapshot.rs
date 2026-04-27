mod codec;
mod validation;

use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::metadata::AccountMeta;
use crate::registry::Registry;
use crate::replay_journal::ReplayJournal;
use crate::state_hash::{hash_canonical_state, CanonicalStateV2, StateHash};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMetadataV2 {
    pub last_applied_event_seq: u64,
    pub state_hash: StateHash,
    pub producer: String,
    pub build_version: String,
    pub fixture_identifier: Option<String>,
    pub user_notes: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotMetadataOptions {
    pub producer: String,
    pub build_version: String,
    pub fixture_identifier: Option<String>,
    pub user_notes: Option<String>,
}

impl Default for SnapshotMetadataOptions {
    fn default() -> Self {
        Self {
            producer: "pnl-core-rs".to_string(),
            build_version: env!("CARGO_PKG_VERSION").to_string(),
            fixture_identifier: None,
            user_notes: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotV2 {
    pub metadata: SnapshotMetadataV2,
    pub state: CanonicalStateV2,
}

impl CanonicalStateV2 {
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
            self.instrument_lifecycles
                .into_iter()
                .map(|meta| (meta.instrument_id, meta.state))
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
            lots: self
                .lots
                .into_iter()
                .map(|lot| (lot.position_key(), lot.lot_id, lot))
                .map(|(key, lot_id, lot)| ((key, lot_id), lot))
                .collect(),
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
    pub fn snapshot(&self) -> Result<SnapshotV2> {
        self.snapshot_with_metadata(SnapshotMetadataOptions::default())
    }

    pub fn snapshot_with_metadata(&self, options: SnapshotMetadataOptions) -> Result<SnapshotV2> {
        let state = CanonicalStateV2::from_engine(self);
        let state_hash = hash_canonical_state(&state);
        Ok(SnapshotV2 {
            metadata: SnapshotMetadataV2 {
                last_applied_event_seq: self.replay_journal.last_seq(),
                state_hash,
                producer: options.producer,
                build_version: options.build_version,
                fixture_identifier: options.fixture_identifier,
                user_notes: options.user_notes,
            },
            state,
        })
    }

    pub fn restore(snapshot: SnapshotV2) -> Result<Self> {
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

    pub fn write_snapshot_with_metadata<W: Write>(
        &self,
        writer: W,
        options: SnapshotMetadataOptions,
    ) -> Result<()> {
        let snapshot = self.snapshot_with_metadata(options)?;
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

    pub fn write_snapshot_json_with_metadata<W: Write>(
        &self,
        writer: W,
        options: SnapshotMetadataOptions,
    ) -> Result<()> {
        let snapshot = self.snapshot_with_metadata(options)?;
        serde_json::to_writer_pretty(writer, &snapshot)?;
        Ok(())
    }
}
