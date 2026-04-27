use pnl_core::*;
use proptest::prelude::*;

mod support;

use support::*;

#[test]
fn open_long_and_mark_reconciles_cash_equity_and_pnl() {
    let mut engine = setup();
    engine.apply(initial(1, "10000.00")).unwrap();
    engine
        .apply(fill(2, Side::Buy, 100, "10.00", "1.00"))
        .unwrap();
    engine.apply(mark(3, "12.00")).unwrap();

    let summary = engine.account_summary(ACCOUNT).unwrap();
    assert_eq!(summary.cash, money("8999.00"));
    assert_eq!(summary.position_market_value, money("1200.00"));
    assert_eq!(summary.equity, money("10199.00"));
    assert_eq!(summary.realized_pnl, money("-1.00"));
    assert_eq!(summary.unrealized_pnl, money("200.00"));
    assert_eq!(summary.total_pnl, money("199.00"));
    assert_eq!(summary.pnl_reconciliation_delta, money("0.00"));
}

#[test]
fn cross_currency_fill_mark_and_fx_revalue_in_account_currency() {
    let mut engine = setup_eur_instrument_usd_account();
    engine.apply(initial(1, "10000.00")).unwrap();
    engine
        .apply(fx(2, eur(), CurrencyId::usd(), "1.10"))
        .unwrap();
    engine
        .apply(fill_fee_currency(3, Side::Buy, 10, "100.00", "2.00", eur()))
        .unwrap();
    engine.apply(mark(4, "110.00")).unwrap();

    let summary = engine.account_summary(ACCOUNT).unwrap();
    assert_eq!(summary.cash, money("8897.80"));
    assert_eq!(summary.position_market_value, money("1210.00"));
    assert_eq!(summary.equity, money("10107.80"));
    assert_eq!(summary.realized_pnl, money("-2.20"));
    assert_eq!(summary.unrealized_pnl, money("110.00"));
    assert_eq!(summary.pnl_reconciliation_delta, money("0.00"));

    engine
        .apply(fx(5, eur(), CurrencyId::usd(), "1.20"))
        .unwrap();
    let summary = engine.account_summary(ACCOUNT).unwrap();
    assert_eq!(summary.position_market_value, money("1320.00"));
    assert_eq!(summary.equity, money("10217.80"));
    assert_eq!(summary.realized_pnl, money("-2.20"));
    assert_eq!(summary.unrealized_pnl, money("220.00"));
    assert_eq!(summary.total_pnl, money("217.80"));
    assert_eq!(summary.pnl_reconciliation_delta, money("0.00"));
}

#[test]
fn trade_correction_replays_replacement_fill_before_later_events() {
    let mut engine = setup();
    engine.apply(initial(1, "10000.00")).unwrap();
    engine.apply(fill(2, Side::Buy, 100, "10.00", "0")).unwrap();
    engine.apply(fill(3, Side::Sell, 40, "12.00", "0")).unwrap();

    let replacement = replacement_fill(Side::Buy, 100, "9.00", "0", CurrencyId::usd());
    engine
        .apply(correct_fill(4, EventId(2), replacement))
        .unwrap();

    let summary = engine.account_summary(ACCOUNT).unwrap();
    let pos = engine.position(position_key()).unwrap();
    assert_eq!(pos.signed_qty, Qty::from_units(60));
    assert_eq!(pos.cost_basis, money("540.00"));
    assert_eq!(summary.realized_pnl, money("120.00"));
    assert_eq!(summary.cash, money("9580.00"));
    assert_eq!(summary.equity, money("10120.00"));
    assert_eq!(summary.pnl_reconciliation_delta, money("0.00"));
}

#[test]
fn trade_bust_replays_without_original_fill() {
    let mut engine = setup();
    engine.apply(initial(1, "10000.00")).unwrap();
    engine.apply(fill(2, Side::Buy, 100, "10.00", "0")).unwrap();
    engine.apply(mark(3, "12.00")).unwrap();
    engine.apply(bust_fill(4, EventId(2))).unwrap();

    let summary = engine.account_summary(ACCOUNT).unwrap();
    assert!(engine.position(position_key()).is_none());
    assert_eq!(summary.cash, money("10000.00"));
    assert_eq!(summary.realized_pnl, money("0.00"));
    assert_eq!(summary.unrealized_pnl, money("0.00"));
    assert_eq!(summary.pnl_reconciliation_delta, money("0.00"));
}

#[test]
fn restored_snapshot_can_correct_prior_fill() {
    let mut engine = setup();
    engine.apply(initial(1, "10000.00")).unwrap();
    engine.apply(fill(2, Side::Buy, 100, "10.00", "0")).unwrap();

    let mut bytes = Vec::new();
    engine.write_snapshot(&mut bytes).unwrap();
    let mut restored = Engine::read_snapshot(bytes.as_slice()).unwrap();
    let replacement = replacement_fill(Side::Buy, 100, "9.00", "0", CurrencyId::usd());
    restored
        .apply(correct_fill(3, EventId(2), replacement))
        .unwrap();

    let summary = restored.account_summary(ACCOUNT).unwrap();
    assert_eq!(summary.cash, money("9100.00"));
    assert_eq!(summary.equity, money("10000.00"));
    assert_eq!(summary.pnl_reconciliation_delta, money("0.00"));
}

#[test]
fn snapshot_metadata_records_producer_build_fixture_and_notes() {
    let mut engine = setup();
    engine.apply(initial(1, "10000.00")).unwrap();

    let snapshot = engine
        .snapshot_with_metadata(SnapshotMetadataOptions {
            producer: "integration-test".to_string(),
            build_version: "1.2.3-test".to_string(),
            fixture_identifier: Some("fixture-a".to_string()),
            user_notes: Some("captured before resume".to_string()),
        })
        .unwrap();

    assert_eq!(snapshot.metadata.last_applied_event_seq, 1);
    assert_eq!(snapshot.metadata.state_hash, engine.state_hash());
    assert_eq!(snapshot.metadata.producer, "integration-test");
    assert_eq!(snapshot.metadata.build_version, "1.2.3-test");
    assert_eq!(
        snapshot.metadata.fixture_identifier.as_deref(),
        Some("fixture-a")
    );
    assert_eq!(
        snapshot.metadata.user_notes.as_deref(),
        Some("captured before resume")
    );
    assert_eq!(
        Engine::restore(snapshot).unwrap().state_hash(),
        engine.state_hash()
    );
}

#[test]
fn cross_currency_fill_requires_direct_fx_rate() {
    let mut engine = setup_eur_instrument_usd_account();
    engine.apply(initial(1, "10000.00")).unwrap();
    let err = engine
        .apply(fill(2, Side::Buy, 10, "100.00", "0"))
        .unwrap_err();
    assert_eq!(
        err,
        Error::MissingFxRate {
            from_currency: eur(),
            to_currency: CurrencyId::usd(),
        }
    );
}

#[test]
fn cross_currency_close_realizes_pnl_at_current_fx_rate() {
    let mut engine = setup_eur_instrument_usd_account();
    engine.apply(initial(1, "10000.00")).unwrap();
    engine
        .apply(fx(2, eur(), CurrencyId::usd(), "1.10"))
        .unwrap();
    engine.apply(fill(3, Side::Buy, 10, "100.00", "0")).unwrap();
    engine
        .apply(fx(4, eur(), CurrencyId::usd(), "1.20"))
        .unwrap();
    engine.apply(fill(5, Side::Sell, 4, "110.00", "0")).unwrap();

    let summary = engine.account_summary(ACCOUNT).unwrap();
    let pos = engine.position(position_key()).unwrap();
    assert_eq!(pos.signed_qty, Qty::from_units(6));
    assert_eq!(pos.cost_basis, money("660.00"));
    assert_eq!(summary.cash, money("9428.00"));
    assert_eq!(summary.realized_pnl, money("88.00"));
    assert_eq!(summary.equity, money("10088.00"));
    assert_eq!(summary.pnl_reconciliation_delta, money("0.00"));
}

#[test]
fn add_partial_close_full_close_and_flat_avg_price() {
    let mut engine = setup();
    engine.apply(initial(1, "10000.00")).unwrap();
    engine.apply(fill(2, Side::Buy, 100, "10.00", "0")).unwrap();
    engine.apply(fill(3, Side::Buy, 50, "12.00", "0")).unwrap();

    let key = position_key();
    let pos = engine.position(key).unwrap();
    assert_eq!(pos.signed_qty, Qty::from_units(150));
    assert_eq!(pos.avg_price, Some(Price::new(106_667, 4)));

    engine
        .apply(fill(4, Side::Sell, 40, "12.00", "1.00"))
        .unwrap();
    let pos = engine.position(key).unwrap();
    assert_eq!(pos.signed_qty, Qty::from_units(110));
    assert_eq!(pos.avg_price, Some(Price::new(106_667, 4)));
    assert_eq!(pos.realized_pnl, money("52.3333"));

    engine
        .apply(fill(5, Side::Sell, 110, "12.00", "0"))
        .unwrap();
    let pos = engine.position(key).unwrap();
    assert_eq!(pos.signed_qty, Qty::zero(0));
    assert_eq!(pos.avg_price, None);
}

#[test]
fn long_to_short_flip_realizes_closed_quantity() {
    let mut engine = setup();
    engine.apply(initial(1, "10000.00")).unwrap();
    engine.apply(fill(2, Side::Buy, 100, "10.00", "0")).unwrap();
    engine
        .apply(fill(3, Side::Sell, 150, "12.00", "0"))
        .unwrap();

    let summary = engine.account_summary(ACCOUNT).unwrap();
    let pos = engine.position(position_key()).unwrap();
    assert_eq!(pos.signed_qty, Qty::from_units(-50));
    assert_eq!(
        pos.avg_price,
        Some(price("12.00").to_scale(4, RoundingMode::HalfEven).unwrap())
    );
    assert_eq!(summary.realized_pnl, money("200.00"));
    assert_eq!(summary.equity, money("10200.00"));
    assert_eq!(summary.pnl_reconciliation_delta, money("0.00"));
}

#[test]
fn short_partial_close_realizes_profit() {
    let mut engine = setup();
    engine.apply(initial(1, "10000.00")).unwrap();
    engine
        .apply(fill(2, Side::Sell, 100, "20.00", "0"))
        .unwrap();
    engine.apply(fill(3, Side::Buy, 40, "18.00", "0")).unwrap();

    let summary = engine.account_summary(ACCOUNT).unwrap();
    let pos = engine.position(position_key()).unwrap();
    assert_eq!(pos.signed_qty, Qty::from_units(-60));
    assert_eq!(summary.realized_pnl, money("80.00"));
    assert_eq!(summary.equity, money("10080.00"));
}

#[test]
fn fees_and_rebates_flow_through_cash_and_realized_pnl() {
    let mut engine = setup();
    engine.apply(initial(1, "1000.00")).unwrap();
    engine
        .apply(fill(2, Side::Buy, 10, "10.00", "-0.50"))
        .unwrap();

    let summary = engine.account_summary(ACCOUNT).unwrap();
    assert_eq!(summary.cash, money("900.50"));
    assert_eq!(summary.realized_pnl, money("0.50"));
    assert_eq!(summary.equity, money("1000.50"));
}

#[test]
fn cash_adjustment_is_external_flow_not_pnl() {
    let mut engine = setup();
    engine.apply(initial(1, "1000.00")).unwrap();
    engine.apply(cash(2, "250.00")).unwrap();
    let summary = engine.account_summary(ACCOUNT).unwrap();
    assert_eq!(summary.cash, money("1250.00"));
    assert_eq!(summary.total_pnl, money("0.00"));
    assert_eq!(summary.net_external_cash_flows, money("250.00"));
    assert_eq!(summary.pnl_reconciliation_delta, money("0.00"));
}

#[test]
fn fifo_and_lifo_realize_against_different_lots() {
    let mut fifo = setup_with_accounting_method(AccountingMethod::Fifo);
    fifo.apply(initial(1, "10000.00")).unwrap();
    fifo.apply(fill(2, Side::Buy, 100, "10.00", "0")).unwrap();
    fifo.apply(fill(3, Side::Buy, 100, "20.00", "0")).unwrap();
    fifo.apply(fill(4, Side::Sell, 100, "15.00", "0")).unwrap();

    let fifo_summary = fifo.account_summary(ACCOUNT).unwrap();
    let fifo_lots: Vec<_> = fifo.lots_for_position(position_key()).collect();
    assert_eq!(fifo_summary.realized_pnl, money("500.00"));
    assert_eq!(
        fifo.position(position_key()).unwrap().cost_basis,
        money("2000.00")
    );
    assert_eq!(fifo_lots.len(), 1);
    assert_eq!(fifo_lots[0].lot_id.source_event_id, EventId(3));
    assert_eq!(fifo_lots[0].side, PositionSide::Long);
    assert_eq!(fifo_lots[0].remaining_qty, Qty::from_units(100));
    assert_eq!(fifo_lots[0].remaining_cost_basis, money("2000.00"));

    let mut lifo = setup_with_accounting_method(AccountingMethod::Lifo);
    lifo.apply(initial(1, "10000.00")).unwrap();
    lifo.apply(fill(2, Side::Buy, 100, "10.00", "0")).unwrap();
    lifo.apply(fill(3, Side::Buy, 100, "20.00", "0")).unwrap();
    lifo.apply(fill(4, Side::Sell, 100, "15.00", "0")).unwrap();

    let lifo_summary = lifo.account_summary(ACCOUNT).unwrap();
    let lifo_lots: Vec<_> = lifo.lots_for_position(position_key()).collect();
    assert_eq!(lifo_summary.realized_pnl, money("-500.00"));
    assert_eq!(
        lifo.position(position_key()).unwrap().cost_basis,
        money("1000.00")
    );
    assert_eq!(lifo_lots.len(), 1);
    assert_eq!(lifo_lots[0].lot_id.source_event_id, EventId(2));
    assert_eq!(lifo_lots[0].remaining_cost_basis, money("1000.00"));
}

#[test]
fn fifo_flip_opens_residual_short_lot_with_deterministic_identity_and_fee_realized() {
    let mut engine = setup_with_accounting_method(AccountingMethod::Fifo);
    engine.apply(initial(1, "10000.00")).unwrap();
    engine.apply(fill(2, Side::Buy, 100, "10.00", "0")).unwrap();
    engine
        .apply(fill(3, Side::Sell, 150, "12.00", "1.50"))
        .unwrap();

    let summary = engine.account_summary(ACCOUNT).unwrap();
    let position = engine.position(position_key()).unwrap();
    let lots: Vec<_> = engine.lots_for_position(position_key()).collect();

    assert_eq!(position.signed_qty, Qty::from_units(-50));
    assert_eq!(position.cost_basis, money("-600.00"));
    assert_eq!(summary.cash, money("10798.50"));
    assert_eq!(summary.realized_pnl, money("198.50"));
    assert_eq!(summary.pnl_reconciliation_delta, money("0.00"));
    assert_eq!(lots.len(), 1);
    assert_eq!(
        lots[0].lot_id,
        LotId {
            source_event_id: EventId(3),
            leg_index: 1
        }
    );
    assert_eq!(lots[0].side, PositionSide::Short);
    assert_eq!(lots[0].original_qty, Qty::from_units(50));
    assert_eq!(lots[0].remaining_qty, Qty::from_units(50));
    assert_eq!(
        lots[0].entry_price,
        price("12.00").to_scale(4, RoundingMode::HalfEven).unwrap()
    );
    assert_eq!(lots[0].remaining_cost_basis, money("-600.00"));
}

#[test]
fn fifo_lots_rebuild_after_trade_correction_and_snapshot_restore() {
    let mut engine = setup_with_accounting_method(AccountingMethod::Fifo);
    engine.apply(initial(1, "10000.00")).unwrap();
    engine.apply(fill(2, Side::Buy, 100, "10.00", "0")).unwrap();
    engine.apply(fill(3, Side::Buy, 100, "20.00", "0")).unwrap();
    engine
        .apply(fill(4, Side::Sell, 100, "15.00", "0"))
        .unwrap();

    let replacement = replacement_fill(Side::Buy, 100, "8.00", "0", CurrencyId::usd());
    engine
        .apply(correct_fill(5, EventId(2), replacement))
        .unwrap();

    assert_eq!(
        engine.account_summary(ACCOUNT).unwrap().realized_pnl,
        money("700.00")
    );
    let lots: Vec<_> = engine.lots_for_position(position_key()).collect();
    assert_eq!(lots.len(), 1);
    assert_eq!(lots[0].lot_id.source_event_id, EventId(3));
    assert_eq!(lots[0].remaining_cost_basis, money("2000.00"));

    let mut bytes = Vec::new();
    engine.write_snapshot(&mut bytes).unwrap();
    let restored = Engine::read_snapshot(bytes.as_slice()).unwrap();
    let restored_lots: Vec<_> = restored.lots_for_position(position_key()).collect();
    assert_eq!(restored_lots, lots);
    assert_eq!(restored.state_hash(), engine.state_hash());
}

#[test]
fn fifo_direct_fx_lots_store_entry_basis_and_close_at_current_rate() {
    let mut engine =
        setup_eur_instrument_usd_account_with_accounting_method(AccountingMethod::Fifo);
    engine.apply(initial(1, "10000.00")).unwrap();
    engine
        .apply(fx(2, eur(), CurrencyId::usd(), "1.10"))
        .unwrap();
    engine.apply(fill(3, Side::Buy, 10, "100.00", "0")).unwrap();
    engine
        .apply(fx(4, eur(), CurrencyId::usd(), "1.20"))
        .unwrap();
    engine.apply(fill(5, Side::Sell, 4, "110.00", "0")).unwrap();

    let summary = engine.account_summary(ACCOUNT).unwrap();
    let lots: Vec<_> = engine.lots_for_position(position_key()).collect();
    assert_eq!(summary.realized_pnl, money("88.00"));
    assert_eq!(
        engine.position(position_key()).unwrap().cost_basis,
        money("660.00")
    );
    assert_eq!(lots.len(), 1);
    assert_eq!(lots[0].remaining_qty, Qty::from_units(6));
    assert_eq!(lots[0].remaining_cost_basis, money("660.00"));
}

#[test]
fn multi_account_and_multi_book_replay_stays_isolated() {
    let mut engine = setup();
    engine
        .register_account(AccountMeta {
            account_id: AccountId(2),
            base_currency: CurrencyId::usd(),
        })
        .unwrap();
    engine
        .register_book(BookMeta {
            account_id: ACCOUNT,
            book_id: BookId(2),
        })
        .unwrap();
    engine
        .register_book(BookMeta {
            account_id: AccountId(2),
            book_id: BOOK,
        })
        .unwrap();

    engine
        .apply_many([
            initial(1, "1000.00"),
            Event {
                seq: 2,
                event_id: EventId(2),
                ts_unix_ns: 2,
                kind: EventKind::InitialCash(InitialCash {
                    account_id: AccountId(2),
                    currency_id: CurrencyId::usd(),
                    amount: money("2000.00"),
                }),
            },
            fill(3, Side::Buy, 10, "10.00", "0"),
            Event {
                seq: 4,
                event_id: EventId(4),
                ts_unix_ns: 4,
                kind: EventKind::Fill(Fill {
                    account_id: ACCOUNT,
                    book_id: BookId(2),
                    instrument_id: INSTRUMENT,
                    side: Side::Buy,
                    qty: Qty::from_units(5),
                    price: price("20.00"),
                    fee: money("0"),
                }),
            },
            Event {
                seq: 5,
                event_id: EventId(5),
                ts_unix_ns: 5,
                kind: EventKind::Fill(Fill {
                    account_id: AccountId(2),
                    book_id: BOOK,
                    instrument_id: INSTRUMENT,
                    side: Side::Sell,
                    qty: Qty::from_units(3),
                    price: price("30.00"),
                    fee: money("0"),
                }),
            },
            mark(6, "12.00"),
        ])
        .unwrap();

    let account_one = engine.account_summary(ACCOUNT).unwrap();
    let account_two = engine.account_summary(AccountId(2)).unwrap();
    assert_eq!(account_one.cash, money("800.00"));
    assert_eq!(account_one.open_positions, 2);
    assert_eq!(account_one.position_market_value, money("180.00"));
    assert_eq!(account_one.unrealized_pnl, money("-20.00"));
    assert_eq!(account_two.cash, money("2090.00"));
    assert_eq!(account_two.open_positions, 1);
    assert_eq!(account_two.position_market_value, money("-36.00"));
    assert_eq!(account_two.unrealized_pnl, money("54.00"));
    assert_eq!(account_one.pnl_reconciliation_delta, money("0.00"));
    assert_eq!(account_two.pnl_reconciliation_delta, money("0.00"));

    assert_eq!(
        engine
            .position(PositionKey {
                account_id: ACCOUNT,
                book_id: BOOK,
                instrument_id: INSTRUMENT,
            })
            .unwrap()
            .signed_qty,
        Qty::from_units(10)
    );
    assert_eq!(
        engine
            .position(PositionKey {
                account_id: ACCOUNT,
                book_id: BookId(2),
                instrument_id: INSTRUMENT,
            })
            .unwrap()
            .signed_qty,
        Qty::from_units(5)
    );
    assert_eq!(
        engine
            .position(PositionKey {
                account_id: AccountId(2),
                book_id: BOOK,
                instrument_id: INSTRUMENT,
            })
            .unwrap()
            .signed_qty,
        Qty::from_units(-3)
    );
}

#[test]
fn account_summary_reports_leverage_and_open_positions() {
    let mut engine = setup();
    engine.apply(initial(1, "1000.00")).unwrap();
    engine.apply(fill(2, Side::Buy, 100, "10.00", "0")).unwrap();

    let summary = engine.account_summary(ACCOUNT).unwrap();
    assert_eq!(
        summary.leverage,
        Some(Ratio {
            value: 10_000,
            scale: ACCOUNT_RATIO_SCALE
        })
    );
    assert_eq!(summary.open_positions, 1);

    engine
        .apply(fill(3, Side::Sell, 100, "10.00", "0"))
        .unwrap();
    let summary = engine.account_summary(ACCOUNT).unwrap();
    assert_eq!(summary.leverage, Some(Ratio::zero(ACCOUNT_RATIO_SCALE)));
    assert_eq!(summary.open_positions, 0);
}

#[test]
fn drawdown_updates_after_marks_and_recovers_peak() {
    let mut engine = setup();
    engine.apply(initial(1, "10000.00")).unwrap();
    engine.apply(fill(2, Side::Buy, 100, "10.00", "0")).unwrap();
    engine.apply(mark(3, "9.00")).unwrap();
    let summary = engine.account_summary(ACCOUNT).unwrap();
    assert_eq!(summary.current_drawdown, money("-100.00"));
    assert_eq!(summary.max_drawdown, money("-100.00"));

    engine.apply(mark(4, "11.00")).unwrap();
    let summary = engine.account_summary(ACCOUNT).unwrap();
    assert_eq!(summary.peak_equity, money("10100.00"));
    assert_eq!(summary.current_drawdown, money("0.00"));
    assert_eq!(summary.max_drawdown, money("-100.00"));
}

#[test]
fn reconciliation_invariant_holds_after_every_successful_event() {
    let mut engine = setup();
    for event in [
        initial(1, "10000.00"),
        fill(2, Side::Buy, 100, "10.00", "1.00"),
        mark(3, "12.00"),
        fill(4, Side::Sell, 40, "12.00", "1.00"),
        cash(5, "250.00"),
        fill(6, Side::Sell, 100, "11.00", "0.50"),
        mark(7, "9.00"),
        fill(8, Side::Buy, 10, "8.00", "-0.25"),
    ] {
        let seq = event.seq;
        engine.apply(event).unwrap();
        let summary = engine.account_summary(ACCOUNT).unwrap();
        assert_eq!(
            summary.pnl_reconciliation_delta,
            money("0.00"),
            "reconciliation failed after sequence {seq}"
        );
    }
}

#[test]
fn replay_is_strict_and_duplicate_event_ids_fail() {
    let mut engine = setup();
    engine.apply(initial(1, "1000.00")).unwrap();
    let err = engine
        .apply(fill(3, Side::Buy, 1, "10.00", "0"))
        .unwrap_err();
    assert_eq!(
        err,
        Error::OutOfOrderEvent {
            expected: 2,
            received: 3
        }
    );

    let mut engine = setup();
    engine.apply(initial(1, "1000.00")).unwrap();
    let mut event = fill(2, Side::Buy, 1, "10.00", "0");
    event.event_id = EventId(1);
    let err = engine.apply(event).unwrap_err();
    assert_eq!(err, Error::DuplicateEvent(EventId(1)));
}

#[test]
fn snapshot_restore_preserves_hash_and_corruption_fails() {
    let mut engine = setup();
    engine
        .apply_many([
            initial(1, "10000.00"),
            fill(2, Side::Buy, 100, "10.00", "1.00"),
            mark(3, "12.00"),
        ])
        .unwrap();
    let hash = engine.state_hash();
    let mut bytes = Vec::new();
    engine.write_snapshot(&mut bytes).unwrap();
    let restored = Engine::read_snapshot(bytes.as_slice()).unwrap();
    assert_eq!(restored.state_hash(), hash);
    assert_eq!(
        restored.account_summary(ACCOUNT).unwrap(),
        engine.account_summary(ACCOUNT).unwrap()
    );

    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    assert!(matches!(
        Engine::read_snapshot(bytes.as_slice()),
        Err(Error::SnapshotHashMismatch)
    ));
}

#[test]
fn json_snapshot_export_is_canonical_for_same_state() {
    let events = [
        initial(1, "10000.00"),
        fill(2, Side::Buy, 100, "10.00", "0"),
        mark(3, "12.00"),
    ];
    let mut a = setup();
    a.apply_many(events.clone()).unwrap();
    let mut b = setup();
    b.apply_many(events).unwrap();
    let mut aj = Vec::new();
    let mut bj = Vec::new();
    a.write_snapshot_json(&mut aj).unwrap();
    b.write_snapshot_json(&mut bj).unwrap();
    assert_eq!(aj, bj);
    assert_eq!(a.state_hash(), b.state_hash());
}

proptest! {
    #[test]
    fn generated_fill_sequences_do_not_panic_and_qty_matches(
        fills in proptest::collection::vec((any::<bool>(), 1_u16..50, 1_u16..200), 1..40)
    ) {
        let mut engine = setup();
        engine.apply(initial(1, "1000000.00")).unwrap();
        let mut expected_qty = 0_i128;
        for (idx, (is_buy, qty, whole_price)) in fills.iter().enumerate() {
            let side = if *is_buy { Side::Buy } else { Side::Sell };
            let signed = if *is_buy { *qty as i128 } else { -(*qty as i128) };
            expected_qty += signed;
            let seq = idx as u64 + 2;
            let px = format!("{whole_price}.00");
            engine.apply(fill(seq, side, *qty as i128, &px, "0")).unwrap();
        }
        let key = position_key();
        let position = engine.position(key).unwrap();
        prop_assert_eq!(position.signed_qty.value, expected_qty);
        if expected_qty == 0 {
            prop_assert_eq!(position.avg_price, None);
        } else {
            prop_assert!(position.avg_price.is_some());
        }
        prop_assert_eq!(
            engine.account_summary(ACCOUNT).unwrap().pnl_reconciliation_delta,
            money("0.00")
        );
    }

    #[test]
    fn generated_fill_sequences_with_fees_rebates_and_flips_reconcile(
        fills in proptest::collection::vec(
            (any::<bool>(), 1_i128..75, 1_u16..250, -75_i16..150),
            1..50,
        )
    ) {
        let mut engine = setup();
        engine.apply(initial(1, "1000000.00")).unwrap();
        let mut expected_qty = 0_i128;

        for (idx, (is_buy, qty, whole_price, fee_cents)) in fills.iter().enumerate() {
            let side = if *is_buy { Side::Buy } else { Side::Sell };
            expected_qty += if *is_buy { *qty } else { -*qty };
            let seq = idx as u64 + 2;
            let px = format!("{whole_price}.00");
            let fee = cents_to_decimal(*fee_cents);
            engine.apply(fill(seq, side, *qty, &px, &fee)).unwrap();

            prop_assert_eq!(
                engine.account_summary(ACCOUNT).unwrap().pnl_reconciliation_delta,
                money("0.00"),
                "reconciliation failed after sequence {}",
                seq
            );
        }

        let position = engine.position(position_key()).unwrap();
        prop_assert_eq!(position.signed_qty.value, expected_qty);
        if expected_qty == 0 {
            prop_assert_eq!(position.avg_price, None);
        } else {
            prop_assert!(position.avg_price.is_some());
        }
    }
}

fn cents_to_decimal(cents: i16) -> String {
    let sign = if cents < 0 { "-" } else { "" };
    let abs = cents.unsigned_abs();
    format!("{sign}{}.{:02}", abs / 100, abs % 100)
}
