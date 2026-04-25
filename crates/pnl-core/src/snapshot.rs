mod canonical;
mod codec;
mod validation;

use crate::engine::Engine;
use crate::error::{Error, Result};
pub use canonical::CanonicalStateV1;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

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
