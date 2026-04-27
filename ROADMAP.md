# Roadmap

`pnl-core-rs` is currently a V1 deterministic accounting engine for ordered
event replay, fixed-point PnL, snapshots, and state hashes. The roadmap below is
organized by product maturity rather than calendar dates.

## Guiding Constraints

- Keep the core crate deterministic, replayable, and cash-authoritative.
- Preserve strict sequence validation and duplicate event rejection.
- Prefer explicit accounting events over inferred market behavior.
- Keep broker connectivity, order management, strategy execution, and settlement
  outside this crate.
- Maintain stable snapshot compatibility or provide explicit migration paths
  when compatibility cannot be preserved.

## V1 Hardening

- Expand golden tests for binary snapshots, JSON snapshots, and state hashes.
- Add more property tests around position flips, partial closes, fees, rebates,
  and cash-flow reconciliation.
- Improve malformed CLI input diagnostics with row, line, and field context.
- Add regression fixtures for multi-account and multi-book replay.
- Document the accepted event journal and correction/bust replay model in more
  implementation detail.
- Define minimum supported Rust version and CI coverage expectations.

## V1.1 Operational Readiness

- Add configurable output formats for the CLI, including machine-readable JSON
  summaries.
- Support replay resume from an existing `.pnlsnap` plus later event files.
- Add snapshot metadata fields for producer, build version, fixture identifier,
  and optional user notes.
- Add benchmark baselines for replay throughput, correction replay cost, and
  snapshot read/write performance.
- Publish crate-level examples for embedding the engine in a service.

## V2 Accounting Model Candidates

- Lot-based accounting alongside average cost, with explicit method selection.
- FIFO/LIFO realized PnL policies where required by downstream reporting.
- Corporate-action-style adjustment events for splits and symbol/instrument
  lifecycle changes.
- Explicit interest, borrow, funding, and financing events.
- More expressive FX handling, potentially including inverse lookup or routed
  cross rates when explicitly configured.
- Instrument classes beyond the current spot-like model, starting with futures
  multipliers and contract metadata.

## API And Integration Candidates

- Versioned event schema documentation with compatibility rules.
- Stable serde JSON representation for public API events and summaries.
- Optional no-std evaluation for the fixed-point/accounting core if dependency
  shape allows it.
- WASM-compatible build target for browser or edge replay use cases.
- FFI boundary evaluation only after the Rust API stabilizes.

## Out Of Scope

- Broker adapters.
- Order routing or execution management.
- Strategy runtime features.
- Market data subscriptions.
- Settlement, tax-lot reporting, or regulatory reporting workflows.
- Cryptographic audit guarantees beyond deterministic state fingerprints.

## Release Readiness Checklist

- All documented invariants have focused regression tests.
- Snapshot compatibility behavior is documented for the release.
- CLI fixtures cover the main README workflows.
- Benchmarks have been run against the previous release.
- Changelog entries describe user-visible behavior and compatibility changes.
