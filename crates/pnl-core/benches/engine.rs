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
    let events: Vec<_> = (0..1_000)
        .map(|idx| {
            Scenario::default().fill(
                idx + 2,
                if idx % 2 == 0 { Side::Buy } else { Side::Sell },
                1,
                &format!("185.{idx:04}"),
                "0",
            )
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

criterion_group!(benches, apply_fill, apply_mark, replay, snapshot_restore);
criterion_main!(benches);
