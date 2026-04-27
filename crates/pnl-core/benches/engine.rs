use criterion::{criterion_group, criterion_main, Criterion};
use pnl_core::*;

#[path = "../tests/support.rs"]
mod support;

use support::Scenario;

fn setup_engine() -> Engine {
    Scenario::default().engine_with_initial_cash(1, "100000.00")
}

fn apply_fill(c: &mut Criterion) {
    let fill = Scenario::default().fill(2, Side::Buy, 100, "185.00", "1.00");
    c.bench_function("apply_fill", |b| {
        b.iter_batched(
            setup_engine,
            |mut engine| {
                engine.apply(fill.clone()).unwrap();
            },
            criterion::BatchSize::SmallInput,
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
                engine.apply(mark.clone()).unwrap();
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn replay(c: &mut Criterion) {
    let events = alternating_fills(1_000);
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

fn replay_throughput_baseline(c: &mut Criterion) {
    let events = alternating_fills(10_000);
    c.bench_function("baseline_replay_throughput_10k_events", |b| {
        b.iter_batched(
            setup_engine,
            |mut engine| {
                engine.apply_many(events.clone()).unwrap();
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn correction_replay_cost_baseline(c: &mut Criterion) {
    let scenario = Scenario::default();
    let events = alternating_fills(1_000);
    let correction = scenario.correct_fill(
        1_002,
        EventId(2),
        scenario.replacement_fill(Side::Buy, 1, "184.0000", "0", CurrencyId::usd()),
    );

    c.bench_function("baseline_correction_replay_cost_1k_event_journal", |b| {
        b.iter_batched(
            || {
                let mut engine = setup_engine();
                engine.apply_many(events.clone()).unwrap();
                engine
            },
            |mut engine| {
                engine.apply(correction.clone()).unwrap();
            },
            criterion::BatchSize::SmallInput,
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
                &format!("185.{idx:04}"),
                "0",
            )
        })
        .collect()
}

fn snapshot_engine() -> Engine {
    let mut engine = setup_engine();
    engine.apply_many(alternating_fills(1_000)).unwrap();
    engine
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
                let _engine = Engine::read_snapshot(bytes.as_slice()).unwrap();
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn snapshot_write_baseline(c: &mut Criterion) {
    c.bench_function("baseline_snapshot_write_1k_event_journal", |b| {
        b.iter_batched(
            snapshot_engine,
            |engine| {
                let mut bytes = Vec::new();
                engine.write_snapshot(&mut bytes).unwrap();
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn snapshot_read_baseline(c: &mut Criterion) {
    c.bench_function("baseline_snapshot_read_1k_event_journal", |b| {
        b.iter_batched(
            || {
                let mut bytes = Vec::new();
                snapshot_engine().write_snapshot(&mut bytes).unwrap();
                bytes
            },
            |bytes| {
                let _engine = Engine::read_snapshot(bytes.as_slice()).unwrap();
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

criterion_group!(
    benches,
    apply_fill,
    apply_mark,
    replay,
    replay_throughput_baseline,
    correction_replay_cost_baseline,
    snapshot_restore,
    snapshot_write_baseline,
    snapshot_read_baseline
);
criterion_main!(benches);
