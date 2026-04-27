# pnl-core performance baselines

Baseline date: 2026-04-27
Machine context: macOS 26.2 arm64, Criterion 0.5, release bench profile.

These numbers are hardware-dependent and anchor optimization work rather than
portable guarantees. Performance changes should compare against the checked-in
benchmark names below and must keep snapshot/state-hash goldens stable unless a
release intentionally updates them.

## Acceptance targets

- Average-cost replay: at least 100k events/sec on the 10k-event baseline, and
  10M deterministic replay events in 120s or less on release hardware.
- FIFO/LIFO lot-heavy replay: at least 25k events/sec on the 1k-open-lot
  baseline.
- Correction and bust rebuilds: under 25ms for a 10k-event accepted journal.
- Snapshot restore: under 1ms for the 1k-event journal baseline.
- Snapshot write: under 1ms for the 1k-event journal baseline.
- Compatibility: no accidental state-hash or snapshot format drift.

## Original baseline

The first baseline exposed per-event canonical hashing as the replay blocker:

| Benchmark | Median | Throughput |
| --- | ---: | ---: |
| `baseline_average_cost_replay/10000` | 2.6994 s | 3.7046k events/sec |
| `baseline_history_rewrite_replay/correction/10000` | 6.4099 ms | 1.5601M journal-events/sec |
| `baseline_history_rewrite_replay/bust/10000` | 8.2784 ms | 1.2080M journal-events/sec |
| `baseline_lot_heavy_replay/fifo/1000` | 143.99 ms | 6.9519k events/sec |
| `baseline_lot_heavy_replay/lifo/1000` | 106.35 ms | 9.4121k events/sec |
| `baseline_fx_cross_route_revaluation/1000` | 811.12 us | 1.2329M positions/sec |
| `baseline_snapshot_io/write/1000` | 100.71 us | 9.9295M journal-events/sec |
| `baseline_snapshot_io/read/1000` | 160.12 us | 6.2452M journal-events/sec |

The sample profile for `baseline_average_cost_replay/10000` is in
`.context/perf/profile-average-cost-replay-10k.sample.txt`. The dominant stack
was `Engine::apply_many` -> `replay_journal::apply_event` ->
`state_hash::hash_canonical_state`, with postcard serialization of the accepted
journal consuming most samples.

## Optimized baseline

Commands:

```bash
cargo bench -p pnl-core --bench engine -- \
  'baseline_average_cost_replay/10000$' \
  --noplot --warm-up-time 1 --measurement-time 3

cargo bench -p pnl-core --bench engine -- \
  'baseline_history_rewrite_replay/(correction|bust)/10000$|baseline_lot_heavy_replay/(fifo|lifo)/1000$|baseline_snapshot_io/(write|read)/1000$' \
  --noplot --warm-up-time 1 --measurement-time 3

cargo bench -p pnl-core --bench engine -- \
  'baseline_average_cost_replay/100000$|baseline_lot_heavy_replay/(fifo|lifo)/10000$' \
  --noplot --warm-up-time 1 --measurement-time 3
```

Measured medians after optimization:

| Benchmark | Median | Throughput |
| --- | ---: | ---: |
| `baseline_average_cost_replay/10000` | 5.2218 ms | 1.9150M events/sec |
| `baseline_average_cost_replay/100000` | 40.774 ms | 2.4525M events/sec |
| `baseline_history_rewrite_replay/correction/10000` | 4.1721 ms | 2.3969M journal-events/sec |
| `baseline_history_rewrite_replay/bust/10000` | 4.2247 ms | 2.3671M journal-events/sec |
| `baseline_lot_heavy_replay/fifo/1000` | 488.68 us | 2.0484M events/sec |
| `baseline_lot_heavy_replay/lifo/1000` | 504.22 us | 1.9852M events/sec |
| `baseline_lot_heavy_replay/fifo/10000` | 6.0134 ms | 1.6631M events/sec |
| `baseline_lot_heavy_replay/lifo/10000` | 5.6260 ms | 1.7776M events/sec |
| `baseline_snapshot_io/write/1000` | 132.27 us | 7.5602M journal-events/sec |
| `baseline_snapshot_io/read/1000` | 175.25 us | 5.7062M journal-events/sec |

The optimized replay API computes one final state hash for `apply_many` instead
of hashing canonical state after every accepted event. FIFO/LIFO opening fills
also avoid cloning and replacing the complete lot set for the position on every
same-direction fill.
