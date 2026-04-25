use criterion::{criterion_group, criterion_main, Criterion};
use pnl_core::*;

fn setup_engine() -> Engine {
    let mut engine = Engine::new(EngineConfig::default());
    engine
        .register_currency(CurrencyMeta {
            currency_id: CurrencyId::usd(),
            code: "USD".to_string(),
            scale: ACCOUNT_MONEY_SCALE,
        })
        .unwrap();
    engine
        .register_account(AccountMeta {
            account_id: AccountId(1),
            base_currency: CurrencyId::usd(),
        })
        .unwrap();
    engine
        .register_book(BookMeta {
            account_id: AccountId(1),
            book_id: BookId(1),
        })
        .unwrap();
    engine
        .register_instrument(InstrumentMeta {
            instrument_id: InstrumentId(1),
            symbol: "AAPL".to_string(),
            currency_id: CurrencyId::usd(),
            price_scale: 4,
            qty_scale: 0,
            multiplier: FixedI128::one(),
        })
        .unwrap();
    engine
        .apply(Event {
            seq: 1,
            event_id: EventId(1),
            ts_unix_ns: 1,
            kind: EventKind::InitialCash(InitialCash {
                account_id: AccountId(1),
                currency_id: CurrencyId::usd(),
                amount: Money::new(1_000_000_000, ACCOUNT_MONEY_SCALE, CurrencyId::usd()),
            }),
        })
        .unwrap();
    engine
}

fn apply_fill(c: &mut Criterion) {
    c.bench_function("apply_fill", |b| {
        b.iter_batched(
            setup_engine,
            |mut engine| {
                engine
                    .apply(Event {
                        seq: 2,
                        event_id: EventId(2),
                        ts_unix_ns: 2,
                        kind: EventKind::Fill(Fill {
                            account_id: AccountId(1),
                            book_id: BookId(1),
                            instrument_id: InstrumentId(1),
                            side: Side::Buy,
                            qty: Qty::from_units(100),
                            price: Price::new(1_850_000, 4),
                            fee: Money::new(10_000, ACCOUNT_MONEY_SCALE, CurrencyId::usd()),
                        }),
                    })
                    .unwrap();
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn apply_mark(c: &mut Criterion) {
    c.bench_function("apply_mark", |b| {
        b.iter_batched(
            || {
                let mut engine = setup_engine();
                engine
                    .apply(Event {
                        seq: 2,
                        event_id: EventId(2),
                        ts_unix_ns: 2,
                        kind: EventKind::Fill(Fill {
                            account_id: AccountId(1),
                            book_id: BookId(1),
                            instrument_id: InstrumentId(1),
                            side: Side::Buy,
                            qty: Qty::from_units(100),
                            price: Price::new(1_850_000, 4),
                            fee: Money::new(10_000, ACCOUNT_MONEY_SCALE, CurrencyId::usd()),
                        }),
                    })
                    .unwrap();
                engine
            },
            |mut engine| {
                engine
                    .apply(Event {
                        seq: 3,
                        event_id: EventId(3),
                        ts_unix_ns: 3,
                        kind: EventKind::Mark(MarkPriceUpdate {
                            instrument_id: InstrumentId(1),
                            price: Price::new(1_875_000, 4),
                        }),
                    })
                    .unwrap();
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn replay(c: &mut Criterion) {
    let events: Vec<_> = (0..1_000)
        .map(|idx| Event {
            seq: idx + 2,
            event_id: EventId(idx + 2),
            ts_unix_ns: idx as i64 + 2,
            kind: EventKind::Fill(Fill {
                account_id: AccountId(1),
                book_id: BookId(1),
                instrument_id: InstrumentId(1),
                side: if idx % 2 == 0 { Side::Buy } else { Side::Sell },
                qty: Qty::from_units(1),
                price: Price::new(1_850_000 + idx as i128, 4),
                fee: Money::new(0, ACCOUNT_MONEY_SCALE, CurrencyId::usd()),
            }),
        })
        .collect();
    c.bench_function("replay_1k_events", |b| {
        b.iter_batched(
            setup_engine,
            |mut engine| {
                engine.apply_many(events.clone()).unwrap();
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn snapshot_restore(c: &mut Criterion) {
    c.bench_function("snapshot_restore", |b| {
        b.iter_batched(
            || {
                let mut engine = setup_engine();
                engine
                    .apply(Event {
                        seq: 2,
                        event_id: EventId(2),
                        ts_unix_ns: 2,
                        kind: EventKind::Fill(Fill {
                            account_id: AccountId(1),
                            book_id: BookId(1),
                            instrument_id: InstrumentId(1),
                            side: Side::Buy,
                            qty: Qty::from_units(100),
                            price: Price::new(1_850_000, 4),
                            fee: Money::new(10_000, ACCOUNT_MONEY_SCALE, CurrencyId::usd()),
                        }),
                    })
                    .unwrap();
                let mut bytes = Vec::new();
                engine.write_snapshot(&mut bytes).unwrap();
                bytes
            },
            |bytes| {
                let _engine = Engine::read_snapshot(bytes.as_slice()).unwrap();
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, apply_fill, apply_mark, replay, snapshot_restore);
criterion_main!(benches);
