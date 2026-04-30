# Replay Event JSON Schema

CLI replay event files are newline-delimited JSON. Each non-empty line must be a
single JSON object with a common envelope plus one event-specific shape selected
by the `type` field.

## Common Envelope

All events require:

- `seq`: unsigned integer replay sequence number.
- `type`: string event type.

All events accept:

- `event_id`: unsigned integer stable event identifier. Defaults to `seq` when
  omitted.
- `ts_unix_ns`: signed integer timestamp in Unix nanoseconds. Defaults to `0`
  when omitted and is informational in the current engine.

Unknown fields are ignored by the CLI decoder. Producers may add fields for
their own bookkeeping, but those fields are not preserved in engine state,
snapshots, state hashes, or replay output unless they become documented fields
in a later schema version.

Decimal values are encoded as JSON strings, not JSON numbers. This preserves
fixed-point parsing and avoids producer-specific floating-point formatting.

Currency codes use the same format as `CurrencyId::from_code`, for example
`"USD"` or `"EUR"`.

## Event Shapes

### `initial_cash`

Sets initial cash for an account and currency.

Required fields:

- `account_id`: unsigned integer.
- `amount`: decimal string.

Optional fields:

- `currency`: currency code. Defaults to the replay config `base_currency`.

Example:

```json
{"seq":1,"type":"initial_cash","account_id":1,"currency":"USD","amount":"100000.00"}
```

### `cash_adjustment`

Applies a signed cash adjustment.

Required fields:

- `account_id`: unsigned integer.
- `amount`: decimal string.

Optional fields:

- `currency`: currency code. Defaults to the replay config `base_currency`.
- `reason`: string.

Example:

```json
{"seq":2,"type":"cash_adjustment","account_id":1,"amount":"-25.00","reason":"withdrawal"}
```

### `interest`, `borrow`, `funding`, `financing`

Apply signed account-level financing cash and PnL deltas. Positive amounts
credit cash and PnL; negative amounts debit cash and PnL.

Required fields:

- `account_id`: unsigned integer.
- `amount`: decimal string.

Optional fields:

- `currency`: currency code. Defaults to the replay config `base_currency`.
- `reason`: string.

Example:

```json
{"seq":3,"type":"interest","account_id":1,"currency":"USD","amount":"12.50","reason":"cash interest"}
```

### `fill`

Applies a trade fill.

Required fields:

- `account_id`: unsigned integer.
- `book_id`: unsigned integer.
- `instrument_id`: unsigned integer.
- `side`: `"buy"` or `"sell"`.
- `qty`: decimal string.
- `price`: decimal string.

Optional fields:

- `fee`: decimal string. Defaults to `"0"`.
- `fee_currency`: currency code. Defaults to the replay config
  `base_currency`.

Example:

```json
{"seq":4,"type":"fill","account_id":1,"book_id":1,"instrument_id":1,"side":"buy","qty":"100","price":"185.00","fee":"1.00","fee_currency":"USD"}
```

### `mark`

Updates an instrument mark price.

Required fields:

- `instrument_id`: unsigned integer.
- `price`: decimal string.

Example:

```json
{"seq":5,"type":"mark","instrument_id":1,"price":"187.50"}
```

### `fx_rate`

Updates an FX rate. `rate` is target currency units per one source currency
unit.

Required fields:

- `from_currency`: source currency code.
- `to_currency`: target currency code.
- `rate`: decimal string.

Example:

```json
{"seq":6,"type":"fx_rate","from_currency":"EUR","to_currency":"USD","rate":"1.10"}
```

### `split`

Applies an instrument split.

Required fields:

- `instrument_id`: unsigned integer.
- `numerator`: unsigned integer representable as `u32`.
- `denominator`: unsigned integer representable as `u32`.

Optional fields:

- `reason`: string.

Example:

```json
{"seq":7,"type":"split","instrument_id":1,"numerator":2,"denominator":1,"reason":"2-for-1 split"}
```

### `symbol_change`

Updates an instrument symbol.

Required fields:

- `instrument_id`: unsigned integer.
- `symbol`: string.

Optional fields:

- `reason`: string.

Example:

```json
{"seq":8,"type":"symbol_change","instrument_id":1,"symbol":"META"}
```

### `instrument_lifecycle`

Updates an instrument lifecycle state.

Required fields:

- `instrument_id`: unsigned integer.
- `lifecycle_state`: `"active"`, `"halted"`, or `"delisted"`.

Optional fields:

- `reason`: string.

Example:

```json
{"seq":9,"type":"instrument_lifecycle","instrument_id":1,"lifecycle_state":"halted","reason":"exchange halt"}
```

### `trade_correction`

Replaces a prior fill during deterministic journal replay. The replacement uses
the same fields as `fill`, plus a correction target.

Required fields:

- `original_event_id`: unsigned integer event ID of the fill to correct.
- `account_id`: unsigned integer.
- `book_id`: unsigned integer.
- `instrument_id`: unsigned integer.
- `side`: `"buy"` or `"sell"`.
- `qty`: decimal string.
- `price`: decimal string.

Optional fields:

- `fee`: decimal string. Defaults to `"0"`.
- `fee_currency`: currency code. Defaults to the replay config
  `base_currency`.
- `reason`: string.

Example:

```json
{"seq":10,"type":"trade_correction","original_event_id":4,"account_id":1,"book_id":1,"instrument_id":1,"side":"buy","qty":"100","price":"184.90","fee":"1.00","reason":"venue correction"}
```

### `trade_bust`

Removes a prior fill from accounting during deterministic journal replay.

Required fields:

- `original_event_id`: unsigned integer event ID of the fill to bust.

Optional fields:

- `reason`: string.

Example:

```json
{"seq":11,"type":"trade_bust","original_event_id":4,"reason":"venue bust"}
```

## Compatibility Rules

The current CLI event schema is intentionally strict for documented fields and
permissive for extra fields:

- Required fields must be present and must have the documented JSON type.
- String enum fields reject unsupported values.
- Decimal fields must be strings parseable by the fixed-point type used by the
  target field.
- Unknown event `type` values are rejected.
- Unknown object fields are ignored.
- Omitted `event_id` defaults to the event `seq`.
- Omitted `ts_unix_ns` defaults to `0`.
- Omitted cash `currency` fields default to the replay config `base_currency`.
- Omitted fill and correction `fee` defaults to `"0"`.
- Omitted fill and correction `fee_currency` defaults to the replay config
  `base_currency`.

The engine applies replay validation after decoding:

- Events must be ordered before replay.
- The first accepted event sequence must equal `expected_start_seq`, which
  defaults to `1` in CLI config.
- Each next accepted event must use `last_seq + 1`.
- Every `event_id` must be unique, including IDs defaulted from `seq`.
- Corrections and busts must target a previously accepted fill event.
- Correction replacements must keep the original fill account, book, and
  instrument.

Future schema versions should preserve the meaning of documented fields. If a
field is deprecated, producers should continue emitting the replacement field
while consumers keep accepting the deprecated field for a documented transition
period. No fields are currently deprecated.
