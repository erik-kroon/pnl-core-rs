# pnl-core-rs

Deterministic fixed-point PnL, position, exposure, equity, leverage, drawdown,
snapshot, and replay accounting for real-time trading systems.

`pnl-core-rs` consumes a strict ordered stream of accounting events and produces
replayable portfolio state. The core crate is intentionally small and
cash-authoritative: cash is the source of truth, equity is derived, and all
public state can be snapshotted, restored, and hashed deterministically.

## Status

This repository contains the V2 Rust implementation:

- `crates/pnl-core`: accounting engine and public Rust interface.
- `crates/pnl-cli`: replay CLI for TOML, CSV, and NDJSON inputs.
- `fixtures/`: sample configuration, instruments, and events.

The crate is not a broker connector, order management system, strategy runtime,
or settlement engine.

See [ROADMAP.md](ROADMAP.md) for planned hardening and future accounting model
candidates.

## What V2 Supports

- Strict contiguous event replay.
- Typed IDs for accounts, books, instruments, currencies, and events.
- Account and instrument registry validation.
- Cash-authoritative accounting.
- Average-cost, FIFO, and LIFO position accounting.
- Public open-lot inspection for FIFO/LIFO accounting.
- Multi-currency instruments and accounts with explicit direct FX conversion.
- Fill fees, including rebates and non-account-currency fees.
- Marks, FX revaluation, gross/net exposure, unrealized PnL, and leverage.
- Trade corrections and busts through deterministic historical replay.
- Versioned binary `.pnlsnap` snapshot/restore.
- JSON snapshot export for inspection and golden tests.
- Deterministic `state_hash()` over canonical accounting state.

## Core Invariants

- Cash is authoritative.
- Equity is always derived as `cash + position_market_value`.
- Replay is strict and contiguous by sequence number.
- Duplicate event IDs are rejected.
- Accounting method selection is engine-wide and explicit.
- FX conversion uses explicit direct rates only.
- Snapshots encode canonical state, not raw implementation internals.
- State hashes are deterministic fingerprints, not cryptographic audit proofs.

## Quickstart

Run the sample replay:

```bash
cargo run -p pnl-cli -- replay \
  --config fixtures/config.toml \
  --instruments fixtures/instruments.csv \
  --events fixtures/events.ndjson \
  --summary \
  --positions \
  --state-hash
```

Write binary and JSON snapshots:

```bash
cargo run -p pnl-cli -- replay \
  --config fixtures/config.toml \
  --instruments fixtures/instruments.csv \
  --events fixtures/events.ndjson \
  --snapshot-out state.pnlsnap \
  --snapshot-json-out state.snapshot.json
```

Emit a machine-readable replay summary:

```bash
cargo run -p pnl-cli -- replay \
  --config fixtures/config.toml \
  --instruments fixtures/instruments.csv \
  --events fixtures/events.ndjson \
  --summary \
  --positions \
  --state-hash \
  --output json
```

Resume replay from an existing snapshot and one or more later event files:

```bash
cargo run -p pnl-cli -- replay \
  --snapshot-in state.pnlsnap \
  --events later-events.ndjson \
  --summary \
  --output json
```

## Rust API Sketch

```rust
use pnl_core::*;

let mut engine = Engine::new(EngineConfig::default());

engine.register_currency(CurrencyMeta {
    currency_id: CurrencyId::usd(),
    code: "USD".to_string(),
    scale: ACCOUNT_MONEY_SCALE,
})?;
engine.register_account(AccountMeta {
    account_id: AccountId(1),
    base_currency: CurrencyId::usd(),
})?;
engine.register_book(BookMeta {
    account_id: AccountId(1),
    book_id: BookId(1),
})?;
engine.register_instrument(InstrumentMeta {
    instrument_id: InstrumentId(1),
    symbol: "AAPL".to_string(),
    currency_id: CurrencyId::usd(),
    price_scale: 4,
    qty_scale: 0,
    multiplier: FixedI128::one(),
})?;

engine.apply(Event {
    seq: 1,
    event_id: EventId(1),
    ts_unix_ns: 1,
    kind: EventKind::InitialCash(InitialCash {
        account_id: AccountId(1),
        currency_id: CurrencyId::usd(),
        amount: Money::parse_decimal("10000.00", CurrencyId::usd(), ACCOUNT_MONEY_SCALE)?,
    }),
})?;

let summary = engine.account_summary(AccountId(1))?;
println!("{summary:?}");
# Ok::<(), pnl_core::Error>(())
```

For a service-style embedding example with ingestion, summary reads, snapshot
write, and restore, see
[`crates/pnl-core/examples/embedding_service.rs`](crates/pnl-core/examples/embedding_service.rs).

## Accounting Model

V2 supports engine-wide average-cost, FIFO, and LIFO accounting. Fees are
converted into account currency before cash and realized PnL updates. Positive
fees are costs; negative fees are rebates. Fees reduce or increase cash
immediately and are recognized immediately in realized PnL. Fees are not
capitalized into average price or lot cost basis.

FIFO and LIFO maintain public open lots. Lot quantities are stored as positive
absolute quantities with a separate long/short side. Lot IDs are deterministic
from the opening fill event and leg index; when a flip closes one side and opens
the residual opposite side, the residual lot uses leg index `1`.

Position cost basis is tracked separately from rounded average price so account
summaries reconcile under fixed-point rounding. If no mark is available,
position market value uses signed cost basis and unrealized PnL is zero. Once a
mark is available, unrealized PnL is:

```text
marked_market_value - signed_cost_basis
```

## Replay Contract

Events must be pre-ordered by `seq`.

- `seq` must start at `expected_start_seq`, default `1`.
- Each next event must use `last_seq + 1`.
- `event_id` must be unique.
- The CLI defaults `event_id` to `seq` when omitted.
- `ts_unix_ns` is recorded but informational in V2.

Supported event types:

- `initial_cash`
- `cash_adjustment`
- `fill`
- `mark`
- `fx_rate`
- `trade_correction`
- `trade_bust`

An `fx_rate` event supplies target currency units per one source currency unit:

```json
{"seq":2,"type":"fx_rate","from_currency":"EUR","to_currency":"USD","rate":"1.10","ts_unix_ns":2}
```

Cross-currency fills, fees, and marked exposures require a direct rate from the
source currency to the account currency unless both currencies are the same.
Inverse and cross-rate routing are not inferred.

Corrections and busts target prior fill events:

```json
{"seq":4,"type":"trade_correction","original_event_id":2,"account_id":1,"book_id":1,"instrument_id":1,"side":"buy","qty":"100","price":"9.00","fee":"0","ts_unix_ns":4}
{"seq":5,"type":"trade_bust","original_event_id":2,"reason":"venue bust","ts_unix_ns":5}
```

A correction may change side, quantity, price, or fee, but must keep the original
account, book, and instrument. The engine retains the accepted event journal and
rebuilds canonical accounting state deterministically after corrections and
busts.

The accepted journal is part of canonical state. Every accepted event remains in
sequence order, including correction and bust events, and every `event_id` stays
reserved after acceptance. During a correction or bust, the engine validates the
target against the already accepted journal, records the new correction/bust
event, resets accounting state, and replays the journal from `expected_start_seq`.
Original fill events remain in the journal, but accounting replay applies the
latest correction replacement or skips the original fill after a bust. Marks, FX
rates, cash events, later fills, drawdown state, summaries, snapshots, and state
hashes are then derived from that rebuilt canonical state.

Replay validation is intentionally strict before accounting changes are applied:
out-of-order sequences, duplicate `event_id` values, unknown correction targets,
non-fill correction targets, and correction replacements that change account,
book, or instrument are rejected.

## CLI Inputs

Configuration is TOML:

```toml
base_currency = "USD"
account_money_scale = 4
accounting_method = "average_cost" # "fifo" or "lifo"
allow_short = true
allow_position_flip = true
expected_start_seq = 1

[[accounts]]
account_id = 1

[[books]]
account_id = 1
book_id = 1
```

Instruments are CSV:

```csv
instrument_id,symbol,currency,price_scale,qty_scale,multiplier
1,AAPL,USD,4,0,1
```

Events are newline-delimited JSON:

```json
{"seq":1,"type":"initial_cash","account_id":1,"currency":"USD","amount":"100000.00","ts_unix_ns":1}
{"seq":2,"type":"fill","account_id":1,"book_id":1,"instrument_id":1,"side":"buy","qty":"100","price":"185.00","fee":"1.00","ts_unix_ns":2}
{"seq":3,"type":"mark","instrument_id":1,"price":"187.50","ts_unix_ns":3}
```

## Snapshots

Binary snapshots use:

- Magic header `PNLRS001`.
- Format version `2`.
- Postcard/Serde payload.
- BLAKE3 payload hash.

Snapshots retain the accepted event journal, so restored engines can apply later
trade corrections and busts.

Snapshot metadata records the last applied event sequence and the canonical state
hash, plus producer, build version, optional fixture identifier, and optional
user notes. The binary codec is covered by checked-in golden fixtures for
header, payload, JSON export, and state hash stability.

## Internal Layout

The codebase is split around the main accounting seams:

- `engine`: orchestration and public engine methods.
- `registry`: account, book, currency, and instrument metadata validation.
- `accounting`: average-cost fill accounting.
- `valuation`: FX conversion, mark valuation, exposure, and unrealized PnL.
- `account_metrics`: account summaries, equity, leverage, drawdown inputs, and reconciliation.
- `replay_journal`: event order, duplicate detection, corrections, busts, and rebuild.
- `state_hash`: canonical state and deterministic hashing.
- `snapshot`: snapshot conversion, binary codec, and restore validation.
- `pnl-cli/input.rs`: CLI config, instrument, and event adapters.

## Known Limitations

- Direct FX rates only; inverse and cross rates are not inferred.
- Average-cost accounting only.
- No FIFO/LIFO lots.
- No settlement model.
- No dividends, funding payments, borrow fees, or financing.
- No broker connectors, order management, or strategy logic.
- No Python, C, or WASM bindings in V2.
- No Arrow or Parquet export in V2.

## Validation

The workspace MSRV is Rust 1.88. CI is expected to run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo bench -p pnl-core
```

Benchmark output is hardware-dependent. Current benchmark targets cover
`apply_fill`, `apply_mark`, `replay_1k_events`, and `snapshot_restore`.

## Roadmap

- FIFO/LIFO accounting.
- Explain APIs and reconciliation reports.
- Python bindings.
- C ABI.
- WASM package.
- Arrow/Parquet export.
