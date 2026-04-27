# Changelog

## Unreleased

- Added V2 engine-wide accounting method selection with `AverageCost`, `Fifo`, and `Lifo`.
- Added public FIFO/LIFO open-lot inspection and persisted lot state in snapshots.
- Added opt-in inverse FX lookup and configured one-pivot cross-rate routing.
- Added split, symbol change, and instrument lifecycle events.
- Added account-level interest, borrow, funding, and financing events with separate PnL buckets.
- Changed snapshot canonical state and binary format to V2.
- Added CLI `--output json` replay summaries and repeated `--events` inputs.
- Added CLI replay resume from `--snapshot-in` plus later event files.
- Added snapshot producer, build version, fixture identifier, and user notes metadata.
- Added V1.1 benchmark baselines for replay throughput, correction replay cost, and snapshot read/write.
- Expanded performance baselines for replay counts, correction/bust replay, FIFO/LIFO lot-heavy workloads, FX cross-route revaluation, and snapshot IO.
- Changed `apply` to return a lightweight receipt and `apply_many` to return a final replay report with one state hash.
- Optimized replay by removing per-event canonical hashing and avoiding repeated FIFO/LIFO lot-set cloning for opening fills.
- Added a crate-level service embedding example.

## 0.1.0

- Initial Rust workspace for `pnl-core-rs`.
- Added deterministic fixed-point v1 accounting engine.
- Added strict replay, snapshots, state hashes, CLI demo, tests, and benchmarks.
- Added direct FX-rate events for multi-currency instruments, fees, account summaries, and revaluation.
