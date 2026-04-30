//! Deterministic fixed-point PnL, replay, and snapshot accounting.
//!
//! See `crates/pnl-core/examples/embedding_service.rs` for a minimal service
//! embedding pattern with event ingestion, summaries, snapshot write, and
//! restore.

pub mod account;
mod account_metrics;
mod accounting;
pub mod config;
pub(crate) mod corporate_actions;
pub mod engine;
mod error;
pub mod event;
pub mod metadata;
pub mod position;
mod registry;
pub(crate) mod replay_journal;
mod snapshot;
mod state_hash;
pub mod summary;
mod types;
mod valuation;

pub use crate::account::*;
pub use crate::config::*;
pub use crate::engine::*;
pub use crate::error::{Error, Result};
pub use crate::event::*;
pub use crate::metadata::*;
pub use crate::position::*;
pub use crate::snapshot::{SnapshotMetadataOptions, SnapshotMetadataV2, SnapshotV2};
pub use crate::state_hash::{CanonicalStateV2, StateHash};
pub use crate::summary::*;
pub use crate::types::*;
