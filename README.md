# pnl-core-rs

Deterministic fixed-point PnL, position, and exposure accounting for real-time trading systems.

`pnl-core-rs` turns strict ordered accounting events into replayable portfolio state: cash, positions, realized PnL, unrealized PnL, exposure, equity, drawdown, snapshots, and deterministic state hashes.

## V1 Scope

V1 is intentionally narrow:

- Rust-only core crate plus a replay CLI.
- Multi-currency instruments and accounts with direct FX conversion into each account currency.
- Typed IDs for accounts, books, instruments, currencies, and events.
- Cash-authoritative accounting.
- Average-cost position accounting.
- Fixed-point `i128` arithmetic with account money scale `4`.
- Strict contiguous event replay.
- Versioned `.pnlsnap` snapshot/restore.
- Public deterministic `state_hash()`.

Not included in v1: FIFO/LIFO, trade corrections/busts, Python/C/WASM bindings, Arrow/Parquet export, broker connectors, order management, or strategy logic.

## Core Invariants

- Cash is authoritative.
- Equity is always derived as `cash + position_market_value`.
- Replay is strict and contiguous by sequence number.
- Duplicate event IDs are rejected.
- Average-cost accounting is the only v1 accounting method.
- State hash is deterministic over canonical accounting state.
- Snapshots encode canonical state, not raw engine internals.

## Known Limitations

- FX uses explicit direct rates only; inverse and cross-rate routing are not inferred.
- Average-cost accounting only.
- Currency metadata must use the configured account money scale.
- No FIFO/LIFO lots.
- No corrections/busts.
- No settlement model.
- No dividends, funding payments, or borrow fees.
- State hash is a deterministic fingerprint, not a cryptographic audit proof.

## Workspace

```text
crates/pnl-core  # accounting engine
crates/pnl-cli   # replay CLI, binary name: pnl-core
fixtures/        # sample config, instruments, and events
```

## CLI Demo

```bash
cargo run -p pnl-cli -- replay \
  --config fixtures/config.toml \
  --instruments fixtures/instruments.csv \
  --events fixtures/events.ndjson \
  --summary \
  --positions \
  --state-hash
```

Snapshot output is available with:

```bash
--snapshot-out state.pnlsnap --snapshot-json-out state.snapshot.json
```

## Core API Sketch

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

## Accounting Notes

- Cash is source of truth.
- Equity is derived as `cash + position_market_value`.
- Positive fee means cost.
- Negative fee means rebate.
- Fees are converted into account currency before cash and realized PnL updates.
- Fees reduce cash immediately.
- Fees are recognized immediately in realized PnL.
- Fees are not capitalized into average price.
- Position cost basis is tracked separately from rounded average price so summaries reconcile under fixed-point rounding.
- If no mark is available, position market value uses signed cost basis and unrealized PnL is zero.
- Once a mark is available, unrealized PnL is account-currency `marked_market_value - signed_cost_basis`.

## Replay Contract

Events must be pre-ordered and deduplicated.

- `seq` must start at `expected_start_seq`, default `1`.
- Each next event must use `last_seq + 1`.
- `event_id` must be unique. The CLI defaults `event_id` to `seq` when omitted.
- Timestamps are informational in v1.

Supported event types are:

- `initial_cash`
- `cash_adjustment`
- `fill`
- `mark`
- `fx_rate`

An `fx_rate` event supplies a direct conversion rate as target currency units per one source currency unit:

```json
{"seq":2,"type":"fx_rate","from_currency":"EUR","to_currency":"USD","rate":"1.10","ts_unix_ns":2}
```

Cross-currency fills, fees, and marked exposures require a direct rate from the source currency to the account currency unless both currencies are the same. Fill fees default to the config `base_currency`; set `fee_currency` on fill events when the fee is charged in another currency.

## Snapshots

Production snapshots use:

- magic header `PNLRS001`
- format version `1`
- Postcard/Serde payload
- BLAKE3 payload hash

The payload stores canonical accounting state, not raw implementation internals. JSON snapshot export is intended for debugging, golden tests, and review.

## Validation

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo bench -p pnl-core
```

Benchmark output is hardware-dependent. Current benchmark targets cover `apply_fill`, `apply_mark`, `replay_1k_events`, and `snapshot_restore`.

## Roadmap

- FIFO/LIFO accounting.
- Corrections and busts.
- Python bindings.
- C ABI.
- WASM package.
- Arrow/Parquet export.
- Explain APIs and reconciliation reports.
