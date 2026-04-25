use pnl_core::*;
use proptest::prelude::*;

fn money(value: &str) -> Money {
    Money::parse_decimal(value, CurrencyId::usd(), ACCOUNT_MONEY_SCALE).unwrap()
}

fn price(value: &str) -> Price {
    Price::parse_decimal(value).unwrap()
}

fn setup() -> Engine {
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
}

fn initial(seq: u64, amount: &str) -> Event {
    Event {
        seq,
        event_id: EventId(seq),
        ts_unix_ns: seq as i64,
        kind: EventKind::InitialCash(InitialCash {
            account_id: AccountId(1),
            currency_id: CurrencyId::usd(),
            amount: money(amount),
        }),
    }
}

fn cash(seq: u64, amount: &str) -> Event {
    Event {
        seq,
        event_id: EventId(seq),
        ts_unix_ns: seq as i64,
        kind: EventKind::CashAdjustment(CashAdjustment {
            account_id: AccountId(1),
            currency_id: CurrencyId::usd(),
            amount: money(amount),
            reason: Some("test".to_string()),
        }),
    }
}

fn fill(seq: u64, side: Side, qty: i128, px: &str, fee: &str) -> Event {
    Event {
        seq,
        event_id: EventId(seq),
        ts_unix_ns: seq as i64,
        kind: EventKind::Fill(Fill {
            account_id: AccountId(1),
            book_id: BookId(1),
            instrument_id: InstrumentId(1),
            side,
            qty: Qty::from_units(qty),
            price: price(px),
            fee: money(fee),
        }),
    }
}

fn mark(seq: u64, px: &str) -> Event {
    Event {
        seq,
        event_id: EventId(seq),
        ts_unix_ns: seq as i64,
        kind: EventKind::Mark(MarkPriceUpdate {
            instrument_id: InstrumentId(1),
            price: price(px),
        }),
    }
}

#[test]
fn open_long_and_mark_reconciles_cash_equity_and_pnl() {
    let mut engine = setup();
    engine.apply(initial(1, "10000.00")).unwrap();
    engine
        .apply(fill(2, Side::Buy, 100, "10.00", "1.00"))
        .unwrap();
    engine.apply(mark(3, "12.00")).unwrap();

    let summary = engine.account_summary(AccountId(1)).unwrap();
    assert_eq!(summary.cash, money("8999.00"));
    assert_eq!(summary.position_market_value, money("1200.00"));
    assert_eq!(summary.equity, money("10199.00"));
    assert_eq!(summary.realized_pnl, money("-1.00"));
    assert_eq!(summary.unrealized_pnl, money("200.00"));
    assert_eq!(summary.total_pnl, money("199.00"));
    assert_eq!(summary.pnl_reconciliation_delta, money("0.00"));
}

#[test]
fn add_partial_close_full_close_and_flat_avg_price() {
    let mut engine = setup();
    engine.apply(initial(1, "10000.00")).unwrap();
    engine.apply(fill(2, Side::Buy, 100, "10.00", "0")).unwrap();
    engine.apply(fill(3, Side::Buy, 50, "12.00", "0")).unwrap();

    let key = PositionKey {
        account_id: AccountId(1),
        book_id: BookId(1),
        instrument_id: InstrumentId(1),
    };
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

    let summary = engine.account_summary(AccountId(1)).unwrap();
    let pos = engine
        .position(PositionKey {
            account_id: AccountId(1),
            book_id: BookId(1),
            instrument_id: InstrumentId(1),
        })
        .unwrap();
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

    let summary = engine.account_summary(AccountId(1)).unwrap();
    let pos = engine
        .position(PositionKey {
            account_id: AccountId(1),
            book_id: BookId(1),
            instrument_id: InstrumentId(1),
        })
        .unwrap();
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

    let summary = engine.account_summary(AccountId(1)).unwrap();
    assert_eq!(summary.cash, money("900.50"));
    assert_eq!(summary.realized_pnl, money("0.50"));
    assert_eq!(summary.equity, money("1000.50"));
}

#[test]
fn cash_adjustment_is_external_flow_not_pnl() {
    let mut engine = setup();
    engine.apply(initial(1, "1000.00")).unwrap();
    engine.apply(cash(2, "250.00")).unwrap();
    let summary = engine.account_summary(AccountId(1)).unwrap();
    assert_eq!(summary.cash, money("1250.00"));
    assert_eq!(summary.total_pnl, money("0.00"));
    assert_eq!(summary.net_external_cash_flows, money("250.00"));
    assert_eq!(summary.pnl_reconciliation_delta, money("0.00"));
}

#[test]
fn drawdown_updates_after_marks_and_recovers_peak() {
    let mut engine = setup();
    engine.apply(initial(1, "10000.00")).unwrap();
    engine.apply(fill(2, Side::Buy, 100, "10.00", "0")).unwrap();
    engine.apply(mark(3, "9.00")).unwrap();
    let summary = engine.account_summary(AccountId(1)).unwrap();
    assert_eq!(summary.current_drawdown, money("-100.00"));
    assert_eq!(summary.max_drawdown, money("-100.00"));

    engine.apply(mark(4, "11.00")).unwrap();
    let summary = engine.account_summary(AccountId(1)).unwrap();
    assert_eq!(summary.peak_equity, money("10100.00"));
    assert_eq!(summary.current_drawdown, money("0.00"));
    assert_eq!(summary.max_drawdown, money("-100.00"));
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
        restored.account_summary(AccountId(1)).unwrap(),
        engine.account_summary(AccountId(1)).unwrap()
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
        let key = PositionKey {
            account_id: AccountId(1),
            book_id: BookId(1),
            instrument_id: InstrumentId(1),
        };
        let position = engine.position(key).unwrap();
        prop_assert_eq!(position.signed_qty.value, expected_qty);
        if expected_qty == 0 {
            prop_assert_eq!(position.avg_price, None);
        } else {
            prop_assert!(position.avg_price.is_some());
        }
        prop_assert_eq!(
            engine.account_summary(AccountId(1)).unwrap().pnl_reconciliation_delta,
            money("0.00")
        );
    }
}
