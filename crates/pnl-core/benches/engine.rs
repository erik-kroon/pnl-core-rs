use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use pnl_core::*;

#[path = "../tests/support.rs"]
mod support;

use support::{eur, gbp, money_in, price, Scenario};

const REPLAY_EVENT_COUNTS: &[u64] = &[1_000, 10_000, 100_000];
const REWRITE_JOURNAL_COUNTS: &[u64] = &[1_000, 10_000];
const LOT_EVENT_COUNTS: &[u64] = &[1_000, 10_000];
const SNAPSHOT_EVENT_COUNTS: &[u64] = &[1_000, 10_000];
const FX_POSITION_COUNTS: &[u64] = &[100, 1_000];

fn setup_engine() -> Engine {
    setup_engine_with_accounting_method(AccountingMethod::AverageCost)
}

fn setup_engine_with_accounting_method(accounting_method: AccountingMethod) -> Engine {
    let scenario = Scenario::default();
    let mut engine = scenario.engine_with_accounting_method(accounting_method);
    engine
        .apply(scenario.initial_cash(1, "1000000000.00"))
        .unwrap();
    engine
}

fn apply_fill(c: &mut Criterion) {
    let fill = Scenario::default().fill(2, Side::Buy, 100, "185.00", "1.00");
    c.bench_function("apply_fill", |b| {
        b.iter_batched(
            setup_engine,
            |mut engine| {
                black_box(engine.apply(fill.clone()).unwrap());
            },
            BatchSize::SmallInput,
        )
    });
}

fn apply_mark(c: &mut Criterion) {
    let fill = Scenario::default().fill(2, Side::Buy, 100, "185.00", "1.00");
    let mark = Scenario::default().mark(3, "187.50");
    c.bench_function("apply_mark", |b| {
        b.iter_batched(
            || {
                let mut engine = setup_engine();
                engine.apply(fill.clone()).unwrap();
                engine
            },
            |mut engine| {
                black_box(engine.apply(mark.clone()).unwrap());
            },
            BatchSize::SmallInput,
        )
    });
}

fn replay(c: &mut Criterion) {
    let events = alternating_fills(1_000);
    c.bench_function("replay_1k_events", |b| {
        b.iter_batched(
            || (setup_engine(), events.clone()),
            |(mut engine, events)| {
                black_box(engine.apply_many(events).unwrap());
            },
            BatchSize::LargeInput,
        )
    });
}

fn average_cost_replay_by_event_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("baseline_average_cost_replay");

    for &event_count in REPLAY_EVENT_COUNTS {
        let events = alternating_fills(event_count);
        group.throughput(Throughput::Elements(event_count));
        group.bench_with_input(
            BenchmarkId::from_parameter(event_count),
            &event_count,
            |b, _| {
                b.iter_batched(
                    || (setup_engine(), events.clone()),
                    |(mut engine, events)| {
                        black_box(engine.apply_many(events).unwrap());
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

fn history_rewrite_replay_cost(c: &mut Criterion) {
    let scenario = Scenario::default();
    let mut group = c.benchmark_group("baseline_history_rewrite_replay");

    for &journal_event_count in REWRITE_JOURNAL_COUNTS {
        let events = alternating_fills(journal_event_count);
        let rewrite_seq = journal_event_count + 2;
        let correction = scenario.correct_fill(
            rewrite_seq,
            EventId(2),
            scenario.replacement_fill(Side::Buy, 1, "184.0000", "0", CurrencyId::usd()),
        );
        let bust = scenario.bust_fill(rewrite_seq, EventId(2));

        group.throughput(Throughput::Elements(journal_event_count));
        group.bench_with_input(
            BenchmarkId::new("correction", journal_event_count),
            &journal_event_count,
            |b, _| {
                b.iter_batched(
                    || {
                        let mut engine = setup_engine();
                        engine.apply_many(events.clone()).unwrap();
                        engine
                    },
                    |mut engine| {
                        black_box(engine.apply(correction.clone()).unwrap());
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        group.bench_with_input(
            BenchmarkId::new("bust", journal_event_count),
            &journal_event_count,
            |b, _| {
                b.iter_batched(
                    || {
                        let mut engine = setup_engine();
                        engine.apply_many(events.clone()).unwrap();
                        engine
                    },
                    |mut engine| {
                        black_box(engine.apply(bust.clone()).unwrap());
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

fn fifo_lifo_lot_heavy_workloads(c: &mut Criterion) {
    let mut group = c.benchmark_group("baseline_lot_heavy_replay");

    for accounting_method in [AccountingMethod::Fifo, AccountingMethod::Lifo] {
        for &open_lot_count in LOT_EVENT_COUNTS {
            let events = lot_heavy_round_trip_events(open_lot_count);
            let replayed_events = events.len() as u64;
            group.throughput(Throughput::Elements(replayed_events));
            group.bench_with_input(
                BenchmarkId::new(accounting_method_name(accounting_method), open_lot_count),
                &open_lot_count,
                |b, _| {
                    b.iter_batched(
                        || {
                            (
                                setup_engine_with_accounting_method(accounting_method),
                                events.clone(),
                            )
                        },
                        |(mut engine, events)| {
                            black_box(engine.apply_many(events).unwrap());
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }
    }

    group.finish();
}

fn fx_cross_route_valuation_workloads(c: &mut Criterion) {
    let mut group = c.benchmark_group("baseline_fx_cross_route_revaluation");

    for &position_count in FX_POSITION_COUNTS {
        let engine = fx_cross_route_engine(position_count);
        let fx_update =
            Scenario::default().fx(position_count + 4, gbp(), CurrencyId::usd(), "1.30");

        group.throughput(Throughput::Elements(position_count));
        group.bench_with_input(
            BenchmarkId::from_parameter(position_count),
            &position_count,
            |b, _| {
                b.iter_batched(
                    || engine.clone(),
                    |mut engine| {
                        black_box(engine.apply(fx_update.clone()).unwrap());
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

fn snapshot_read_write_by_event_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("baseline_snapshot_io");

    for &event_count in SNAPSHOT_EVENT_COUNTS {
        group.throughput(Throughput::Elements(event_count));
        group.bench_with_input(
            BenchmarkId::new("write", event_count),
            &event_count,
            |b, _| {
                b.iter_batched(
                    || snapshot_engine(event_count),
                    |engine| {
                        let mut bytes = Vec::new();
                        engine.write_snapshot(&mut bytes).unwrap();
                        black_box(bytes);
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        let mut bytes = Vec::new();
        snapshot_engine(event_count)
            .write_snapshot(&mut bytes)
            .unwrap();

        group.bench_with_input(
            BenchmarkId::new("read", event_count),
            &event_count,
            |b, _| {
                b.iter_batched(
                    || bytes.clone(),
                    |bytes| {
                        black_box(Engine::read_snapshot(bytes.as_slice()).unwrap());
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

fn snapshot_restore(c: &mut Criterion) {
    let fill = Scenario::default().fill(2, Side::Buy, 100, "185.00", "1.00");
    c.bench_function("snapshot_restore", |b| {
        b.iter_batched(
            || {
                let mut engine = setup_engine();
                engine.apply(fill.clone()).unwrap();
                let mut bytes = Vec::new();
                engine.write_snapshot(&mut bytes).unwrap();
                bytes
            },
            |bytes| {
                black_box(Engine::read_snapshot(bytes.as_slice()).unwrap());
            },
            BatchSize::SmallInput,
        )
    });
}

fn alternating_fills(count: u64) -> Vec<Event> {
    (0..count)
        .map(|idx| {
            Scenario::default().fill(
                idx + 2,
                if idx % 2 == 0 { Side::Buy } else { Side::Sell },
                1,
                &realistic_equity_price(idx),
                "0",
            )
        })
        .collect()
}

fn lot_heavy_round_trip_events(open_lot_count: u64) -> Vec<Event> {
    let mut events = Vec::with_capacity(open_lot_count as usize + 1);
    for idx in 0..open_lot_count {
        events.push(Scenario::default().fill(idx + 2, Side::Buy, 1, "100.0000", "0"));
    }
    events.push(Scenario::default().fill(
        open_lot_count + 2,
        Side::Sell,
        open_lot_count as i128,
        "101.0000",
        "0",
    ));
    events
}

fn snapshot_engine(event_count: u64) -> Engine {
    let mut engine = setup_engine();
    engine.apply_many(alternating_fills(event_count)).unwrap();
    engine
}

fn fx_cross_route_engine(position_count: u64) -> Engine {
    let usd = CurrencyId::usd();
    let eur = eur();
    let gbp = gbp();
    let account_id = AccountId(1);
    let book_id = BookId(1);

    let mut engine = Engine::new(EngineConfig {
        fx_routing: FxRoutingConfig {
            allow_inverse: false,
            cross_rate_pivots: vec![gbp],
        },
        ..EngineConfig::default()
    });

    for currency_id in [usd, eur, gbp] {
        engine
            .register_currency(CurrencyMeta {
                currency_id,
                code: currency_id.code(),
                scale: ACCOUNT_MONEY_SCALE,
            })
            .unwrap();
    }
    engine
        .register_account(AccountMeta {
            account_id,
            base_currency: usd,
        })
        .unwrap();
    engine
        .register_book(BookMeta {
            account_id,
            book_id,
        })
        .unwrap();

    for idx in 0..position_count {
        engine
            .register_instrument(InstrumentMeta {
                instrument_id: InstrumentId(idx + 1),
                symbol: format!("EUR{idx}"),
                currency_id: eur,
                price_scale: 4,
                qty_scale: 0,
                multiplier: FixedI128::one(),
            })
            .unwrap();
    }

    engine
        .apply(Event {
            seq: 1,
            event_id: EventId(1),
            ts_unix_ns: 1,
            kind: EventKind::InitialCash(InitialCash {
                account_id,
                currency_id: usd,
                amount: money_in("1000000000.00", usd),
            }),
        })
        .unwrap();
    engine
        .apply(Scenario::default().fx(2, eur, gbp, "0.80"))
        .unwrap();
    engine
        .apply(Scenario::default().fx(3, gbp, usd, "1.25"))
        .unwrap();

    for idx in 0..position_count {
        engine
            .apply(Event {
                seq: idx + 4,
                event_id: EventId(idx + 4),
                ts_unix_ns: (idx + 4) as i64,
                kind: EventKind::Fill(Fill {
                    account_id,
                    book_id,
                    instrument_id: InstrumentId(idx + 1),
                    side: Side::Buy,
                    qty: Qty::from_units(1),
                    price: price(&realistic_equity_price(idx)),
                    fee: money_in("0", usd),
                }),
            })
            .unwrap();
    }

    engine
}

fn realistic_equity_price(idx: u64) -> String {
    format!("{}.{:04}", 100 + (idx % 200), idx % 10_000)
}

fn accounting_method_name(accounting_method: AccountingMethod) -> &'static str {
    match accounting_method {
        AccountingMethod::AverageCost => "average_cost",
        AccountingMethod::Fifo => "fifo",
        AccountingMethod::Lifo => "lifo",
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets =
        apply_fill,
        apply_mark,
        replay,
        average_cost_replay_by_event_count,
        history_rewrite_replay_cost,
        fifo_lifo_lot_heavy_workloads,
        fx_cross_route_valuation_workloads,
        snapshot_restore,
        snapshot_read_write_by_event_count
}
criterion_main!(benches);
