use pnl_core::*;
use std::fs;
use std::path::{Path, PathBuf};

const GOLDEN_UPDATE_ENV: &str = "UPDATE_PNL_GOLDENS";

#[test]
fn representative_snapshot_hash_and_json_match_goldens() {
    assert_representative_golden(
        AccountingMethod::AverageCost,
        "v2_average_cost_representative",
    );
    assert_representative_golden(AccountingMethod::Fifo, "v2_fifo_representative");
    assert_representative_golden(AccountingMethod::Lifo, "v2_lifo_representative");
}

fn assert_representative_golden(accounting_method: AccountingMethod, fixture_name: &str) {
    let engine = representative_engine(accounting_method);
    let binary = snapshot_bytes(&engine);
    let json = snapshot_json_bytes(&engine);
    let state_hash = format!("{}\n", engine.state_hash().to_hex());

    let binary_again = snapshot_bytes(&engine);
    let json_again = snapshot_json_bytes(&engine);
    assert_eq!(binary_again, binary);
    assert_eq!(json_again, json);

    let dir = golden_dir();
    let binary_path = dir.join(format!("{fixture_name}.pnlsnap"));
    let json_path = dir.join(format!("{fixture_name}.snapshot.json"));
    let hash_path = dir.join(format!("{fixture_name}.state_hash"));

    if std::env::var_os(GOLDEN_UPDATE_ENV).is_some() {
        fs::create_dir_all(&dir).unwrap();
        fs::write(&binary_path, &binary).unwrap();
        fs::write(&json_path, &json).unwrap();
        fs::write(&hash_path, &state_hash).unwrap();
    }

    assert_eq!(fs::read(&binary_path).unwrap(), binary);
    assert_eq!(fs::read(&json_path).unwrap(), json);
    assert_eq!(fs::read_to_string(&hash_path).unwrap(), state_hash);

    let restored = Engine::read_snapshot(fs::File::open(&binary_path).unwrap()).unwrap();
    assert_eq!(restored.state_hash(), engine.state_hash());
    assert_eq!(
        restored.account_summary(AccountId(1)).unwrap(),
        engine.account_summary(AccountId(1)).unwrap()
    );
    assert_eq!(
        restored.account_summary(AccountId(2)).unwrap(),
        engine.account_summary(AccountId(2)).unwrap()
    );
}

fn representative_engine(accounting_method: AccountingMethod) -> Engine {
    let mut engine = Engine::new(EngineConfig {
        accounting_method,
        ..EngineConfig::default()
    });
    register_currency(&mut engine, "USD");
    register_currency(&mut engine, "EUR");
    register_account(&mut engine, 1, CurrencyId::usd());
    register_account(&mut engine, 2, CurrencyId::usd());
    register_book(&mut engine, 1, 1);
    register_book(&mut engine, 1, 2);
    register_book(&mut engine, 2, 1);
    register_instrument(&mut engine, 1, "AAPL", CurrencyId::usd(), 4, 0);
    register_instrument(&mut engine, 2, "SAP", eur(), 4, 0);

    engine
        .apply_many([
            initial_cash(1, 1, "10000.00"),
            initial_cash(2, 2, "5000.00"),
            fill(
                3,
                1,
                1,
                1,
                Side::Buy,
                "100",
                "10.00",
                "1.00",
                CurrencyId::usd(),
            ),
            mark(4, 1, "12.00"),
            fx(5, eur(), CurrencyId::usd(), "1.10"),
            fill(6, 1, 2, 2, Side::Buy, "10", "100.00", "2.00", eur()),
            fill(
                7,
                2,
                1,
                1,
                Side::Sell,
                "50",
                "11.00",
                "-0.25",
                CurrencyId::usd(),
            ),
            cash_adjustment(8, 2, "100.00"),
            correction(
                9,
                EventId(3),
                replacement_fill(1, 1, 1, Side::Buy, "100", "9.00", "1.00", CurrencyId::usd()),
            ),
            bust(10, EventId(7)),
            mark(11, 2, "110.00"),
        ])
        .unwrap();

    engine
}

fn snapshot_bytes(engine: &Engine) -> Vec<u8> {
    let mut bytes = Vec::new();
    engine.write_snapshot(&mut bytes).unwrap();
    bytes
}

fn snapshot_json_bytes(engine: &Engine) -> Vec<u8> {
    let mut bytes = Vec::new();
    engine.write_snapshot_json(&mut bytes).unwrap();
    bytes
}

fn register_currency(engine: &mut Engine, code: &str) {
    engine
        .register_currency(CurrencyMeta {
            currency_id: CurrencyId::from_code(code).unwrap(),
            code: code.to_string(),
            scale: ACCOUNT_MONEY_SCALE,
        })
        .unwrap();
}

fn register_account(engine: &mut Engine, account_id: u64, base_currency: CurrencyId) {
    engine
        .register_account(AccountMeta {
            account_id: AccountId(account_id),
            base_currency,
        })
        .unwrap();
}

fn register_book(engine: &mut Engine, account_id: u64, book_id: u64) {
    engine
        .register_book(BookMeta {
            account_id: AccountId(account_id),
            book_id: BookId(book_id),
        })
        .unwrap();
}

fn register_instrument(
    engine: &mut Engine,
    instrument_id: u64,
    symbol: &str,
    currency_id: CurrencyId,
    price_scale: u8,
    qty_scale: u8,
) {
    engine
        .register_instrument(InstrumentMeta {
            instrument_id: InstrumentId(instrument_id),
            symbol: symbol.to_string(),
            currency_id,
            price_scale,
            qty_scale,
            multiplier: FixedI128::one(),
        })
        .unwrap();
}

fn event(seq: u64, kind: EventKind) -> Event {
    Event {
        seq,
        event_id: EventId(seq),
        ts_unix_ns: seq as i64,
        kind,
    }
}

fn initial_cash(seq: u64, account_id: u64, amount: &str) -> Event {
    event(
        seq,
        EventKind::InitialCash(InitialCash {
            account_id: AccountId(account_id),
            currency_id: CurrencyId::usd(),
            amount: money(amount, CurrencyId::usd()),
        }),
    )
}

fn cash_adjustment(seq: u64, account_id: u64, amount: &str) -> Event {
    event(
        seq,
        EventKind::CashAdjustment(CashAdjustment {
            account_id: AccountId(account_id),
            currency_id: CurrencyId::usd(),
            amount: money(amount, CurrencyId::usd()),
            reason: Some("external transfer".to_string()),
        }),
    )
}

#[allow(clippy::too_many_arguments)]
fn fill(
    seq: u64,
    account_id: u64,
    book_id: u64,
    instrument_id: u64,
    side: Side,
    qty: &str,
    price: &str,
    fee: &str,
    fee_currency_id: CurrencyId,
) -> Event {
    event(
        seq,
        EventKind::Fill(replacement_fill(
            account_id,
            book_id,
            instrument_id,
            side,
            qty,
            price,
            fee,
            fee_currency_id,
        )),
    )
}

#[allow(clippy::too_many_arguments)]
fn replacement_fill(
    account_id: u64,
    book_id: u64,
    instrument_id: u64,
    side: Side,
    qty: &str,
    price: &str,
    fee: &str,
    fee_currency_id: CurrencyId,
) -> Fill {
    Fill {
        account_id: AccountId(account_id),
        book_id: BookId(book_id),
        instrument_id: InstrumentId(instrument_id),
        side,
        qty: Qty::parse_decimal(qty).unwrap(),
        price: Price::parse_decimal(price).unwrap(),
        fee: money(fee, fee_currency_id),
    }
}

fn correction(seq: u64, original_event_id: EventId, replacement: Fill) -> Event {
    event(
        seq,
        EventKind::TradeCorrection(TradeCorrection {
            original_event_id,
            replacement,
            reason: Some("price correction".to_string()),
        }),
    )
}

fn bust(seq: u64, original_event_id: EventId) -> Event {
    event(
        seq,
        EventKind::TradeBust(TradeBust {
            original_event_id,
            reason: Some("venue bust".to_string()),
        }),
    )
}

fn mark(seq: u64, instrument_id: u64, value: &str) -> Event {
    event(
        seq,
        EventKind::Mark(MarkPriceUpdate {
            instrument_id: InstrumentId(instrument_id),
            price: Price::parse_decimal(value).unwrap(),
        }),
    )
}

fn fx(seq: u64, from_currency_id: CurrencyId, to_currency_id: CurrencyId, rate: &str) -> Event {
    event(
        seq,
        EventKind::FxRate(FxRateUpdate {
            from_currency_id,
            to_currency_id,
            rate: Price::parse_decimal(rate).unwrap(),
        }),
    )
}

fn money(value: &str, currency_id: CurrencyId) -> Money {
    Money::parse_decimal(value, currency_id, ACCOUNT_MONEY_SCALE).unwrap()
}

fn eur() -> CurrencyId {
    CurrencyId::from_code("EUR").unwrap()
}

fn golden_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/goldens")
}
