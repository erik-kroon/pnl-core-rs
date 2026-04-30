# Roadmap

`pnl-core-rs` is currently a pre-0.1.0 deterministic accounting engine for
ordered event replay, fixed-point PnL, lot accounting, snapshots, and state
hashes. The roadmap below is organized by product maturity rather than calendar
dates.

## Guiding Constraints

- Keep the core crate deterministic, replayable, and cash-authoritative.
- Preserve strict sequence validation and duplicate event rejection.
- Prefer explicit accounting events over inferred market behavior.
- Keep broker connectivity, order management, strategy execution, and settlement
  outside this crate.
- Maintain stable snapshot compatibility or provide explicit migration paths
  when compatibility cannot be preserved.

## Completed Foundation

- Deterministic fixed-point accounting with strict ordered replay.
- Binary snapshots, JSON snapshot export, and deterministic state hashes.
- Golden tests for representative snapshot and state hash outputs.
- Property tests around position flips, partial closes, fees, rebates, and
  cash-flow reconciliation.
- Malformed CLI input diagnostics with row, line, and field context.
- Regression fixtures for multi-account and multi-book replay.
- Documented event journal and correction/bust replay model.
- Minimum supported Rust version and CI command expectations.

## Completed Operational Readiness

- Configurable CLI output formats, including machine-readable JSON summaries.
- Replay resume from an existing `.pnlsnap` plus later event files.
- Snapshot metadata fields for producer, build version, fixture identifier, and
  optional user notes.
- Benchmark baselines for replay throughput, correction replay cost, and
  snapshot read/write performance.
- Crate-level example for embedding the engine in a service.
- Structured apply explanations and account reconciliation reports for callers
  that need to inspect cash, equity, and PnL movement.

## Completed Accounting Model Work

- Engine-wide average-cost, FIFO, and LIFO accounting.
- Public open-lot inspection for FIFO/LIFO accounting.
- Opt-in inverse FX lookup and configured one-pivot cross-rate routing.
- Corporate-action-style events for splits, symbol changes, and instrument
  lifecycle changes.
- Lot state included in the current canonical snapshot/hash material.

## Future Accounting Model Candidates

- More expressive FX handling beyond one configured pivot when explicitly
  configured.
- Instrument classes beyond the current spot-like model. Futures must use
  futures-specific equity and exposure semantics rather than spot-like notional
  cash behavior.

## API And Integration Candidates

- Stable serde JSON representation for public API events and summaries.
- Optional no-std evaluation for the fixed-point/accounting core if dependency
  shape allows it.
- WASM-compatible build target for browser or edge replay use cases.
- Python bindings.
- C ABI.
- Arrow/Parquet export.
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
- Snapshot and CLI event schema compatibility behavior are documented for the
  release.
- Snapshot binary, JSON, and state hash outputs have checked-in golden coverage.
- The workspace MSRV and CI command set are documented for the release.
- CLI fixtures cover the main README workflows.
- Benchmarks have been run against the previous release.
- Changelog entries describe user-visible behavior and compatibility changes.
