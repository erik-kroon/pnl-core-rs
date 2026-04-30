use anyhow::{Context, Result};
use pnl_core::*;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

mod config;
mod event_decode;
mod events;
mod instruments;

use config::{build_engine, load_config};
use events::open_replay_events;
pub use events::EventIter;
use instruments::load_instruments;

pub struct ReplayInput {
    pub engine: Engine,
    pub events: Vec<EventIter<BufReader<File>>>,
}

pub fn open_replay_input(
    config: &Path,
    instruments: &Path,
    events: &[PathBuf],
) -> Result<ReplayInput> {
    let config = load_config(config)?;
    let base_currency = CurrencyId::from_code(&config.base_currency)?;
    let mut engine = build_engine(config, base_currency)?;

    load_instruments(&mut engine, instruments)?;
    let events = open_replay_events(events, base_currency, engine.config().account_money_scale)?;

    Ok(ReplayInput { engine, events })
}

pub fn open_replay_input_from_snapshot(snapshot: &Path, events: &[PathBuf]) -> Result<ReplayInput> {
    let file = File::open(snapshot).with_context(|| format!("reading {}", snapshot.display()))?;
    let engine = Engine::read_snapshot(file)
        .with_context(|| format!("restoring snapshot {}", snapshot.display()))?;
    let events = open_replay_events(
        events,
        engine.config().base_currency,
        engine.config().account_money_scale,
    )?;

    Ok(ReplayInput { engine, events })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::config::default_config;
    use crate::input::events::{event_lines, open_events_for_test};
    use crate::input::instruments::load_instrument_rows;
    use std::io::Cursor;
    use std::path::PathBuf;

    #[test]
    fn opens_fixture_replay_input() {
        let fixtures = fixture_dir();
        let event_paths = [fixtures.join("events.ndjson")];
        let ReplayInput { mut engine, events } = open_replay_input(
            &fixtures.join("config.toml"),
            &fixtures.join("instruments.csv"),
            &event_paths,
        )
        .unwrap();

        let mut replayed_events = 0_u64;
        for events in events {
            for event in events {
                engine.apply(event.unwrap()).unwrap();
                replayed_events += 1;
            }
        }

        assert_eq!(replayed_events, 6);
        assert_eq!(
            engine.account_summary(AccountId(1)).unwrap().state_hash,
            engine.state_hash()
        );
    }

    #[test]
    fn opens_replay_input_from_snapshot_for_later_events() {
        let fixtures = fixture_dir();
        let event_paths = [fixtures.join("events.ndjson")];
        let ReplayInput { mut engine, events } = open_replay_input(
            &fixtures.join("config.toml"),
            &fixtures.join("instruments.csv"),
            &event_paths,
        )
        .unwrap();
        for events in events {
            for event in events {
                engine.apply(event.unwrap()).unwrap();
            }
        }

        let snapshot_path = temp_file("resume.pnlsnap", "");
        engine
            .write_snapshot(std::fs::File::create(&snapshot_path).unwrap())
            .unwrap();
        let later_events_path = temp_file(
            "later-events.ndjson",
            "{\"seq\":7,\"type\":\"mark\",\"instrument_id\":1,\"price\":\"190.00\",\"ts_unix_ns\":7}\n",
        );

        let ReplayInput { mut engine, events } =
            open_replay_input_from_snapshot(&snapshot_path, &[later_events_path.clone()]).unwrap();
        for events in events {
            for event in events {
                engine.apply(event.unwrap()).unwrap();
            }
        }

        assert_eq!(
            engine.snapshot().unwrap().metadata.last_applied_event_seq,
            7
        );

        let _ = std::fs::remove_file(snapshot_path);
        let _ = std::fs::remove_file(later_events_path);
    }

    #[test]
    fn reports_event_conversion_line_context() {
        let error = event_lines(
            Cursor::new("{\"seq\":1,\"type\":\"fill\",\"side\":\"hold\"}\n"),
            CurrencyId::usd(),
            ACCOUNT_MONEY_SCALE,
        )
        .next()
        .unwrap()
        .unwrap_err();

        let message = format!("{error:#}");
        assert!(message.contains("converting events line 1 type \"fill\""));
        assert!(message.contains("unsupported field side value \"hold\""));
    }

    #[test]
    fn parses_financing_event_types() {
        let input = Cursor::new(
            "{\"seq\":1,\"type\":\"interest\",\"account_id\":1,\"currency\":\"USD\",\"amount\":\"1.25\",\"reason\":\"credit\"}\n\
             {\"seq\":2,\"type\":\"borrow\",\"account_id\":1,\"currency\":\"USD\",\"amount\":\"-2.00\"}\n\
             {\"seq\":3,\"type\":\"funding\",\"account_id\":1,\"amount\":\"-0.50\"}\n\
             {\"seq\":4,\"type\":\"financing\",\"account_id\":1,\"amount\":\"3.00\"}\n",
        );
        let events: Vec<_> = event_lines(input, CurrencyId::usd(), ACCOUNT_MONEY_SCALE)
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert!(matches!(events[0].kind, EventKind::Interest(_)));
        assert!(matches!(events[1].kind, EventKind::Borrow(_)));
        assert!(matches!(events[2].kind, EventKind::Funding(_)));
        assert!(matches!(events[3].kind, EventKind::Financing(_)));
        match &events[0].kind {
            EventKind::Interest(event) => {
                assert_eq!(
                    event.amount,
                    Money::parse_decimal("1.25", CurrencyId::usd(), ACCOUNT_MONEY_SCALE).unwrap()
                );
                assert_eq!(event.reason.as_deref(), Some("credit"));
            }
            _ => unreachable!("checked above"),
        }
    }

    #[test]
    fn reports_missing_event_fields_before_replay() {
        let error = event_lines(
            Cursor::new("{\"seq\":1,\"type\":\"initial_cash\",\"amount\":\"10\"}\n"),
            CurrencyId::usd(),
            ACCOUNT_MONEY_SCALE,
        )
        .next()
        .unwrap()
        .unwrap_err();

        let message = format!("{error:#}");
        assert!(message.contains("converting events line 1 type \"initial_cash\""));
        assert!(message.contains("missing required field account_id"));
    }

    #[test]
    fn reports_event_field_value_context() {
        let error = event_lines(
            Cursor::new(
                "{\"seq\":1,\"type\":\"fill\",\"account_id\":1,\"book_id\":1,\"instrument_id\":1,\"side\":\"buy\",\"qty\":\"bad\",\"price\":\"10\"}\n",
            ),
            CurrencyId::usd(),
            ACCOUNT_MONEY_SCALE,
        )
        .next()
        .unwrap()
        .unwrap_err();

        let message = format!("{error:#}");
        assert!(message.contains("converting events line 1 type \"fill\""));
        assert!(message.contains("invalid field qty value \"bad\""));
    }

    #[test]
    fn reports_unsupported_event_type_field_value_context() {
        let error = event_lines(
            Cursor::new("{\"seq\":1,\"type\":\"dividend\"}\n"),
            CurrencyId::usd(),
            ACCOUNT_MONEY_SCALE,
        )
        .next()
        .unwrap()
        .unwrap_err();

        let message = format!("{error:#}");
        assert!(message.contains("converting events line 1 type \"dividend\""));
        assert!(message.contains("unsupported field type value \"dividend\""));
    }

    #[test]
    fn parses_corporate_action_events() {
        let input = Cursor::new(
            "{\"seq\":1,\"type\":\"split\",\"instrument_id\":1,\"numerator\":2,\"denominator\":1}\n\
             {\"seq\":2,\"type\":\"symbol_change\",\"instrument_id\":1,\"symbol\":\"META\"}\n\
             {\"seq\":3,\"type\":\"instrument_lifecycle\",\"instrument_id\":1,\"lifecycle_state\":\"delisted\"}\n",
        );

        let events = event_lines(input, CurrencyId::usd(), ACCOUNT_MONEY_SCALE)
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert!(matches!(
            events[0].kind,
            EventKind::Split(InstrumentSplit {
                instrument_id: InstrumentId(1),
                numerator: 2,
                denominator: 1,
                ..
            })
        ));
        assert!(matches!(
            &events[1].kind,
            EventKind::SymbolChange(InstrumentSymbolChange {
                instrument_id: InstrumentId(1),
                symbol,
                ..
            }) if symbol == "META"
        ));
        assert!(matches!(
            events[2].kind,
            EventKind::InstrumentLifecycle(InstrumentLifecycle {
                instrument_id: InstrumentId(1),
                state: InstrumentLifecycleState::Delisted,
                ..
            })
        ));
    }

    #[test]
    fn event_schema_defaults_event_id_timestamp_currency_and_fee() {
        let input = Cursor::new(
            "{\"seq\":42,\"type\":\"initial_cash\",\"account_id\":1,\"amount\":\"100.00\"}\n\
             {\"seq\":43,\"type\":\"fill\",\"account_id\":1,\"book_id\":1,\"instrument_id\":1,\"side\":\"buy\",\"qty\":\"2\",\"price\":\"10.00\"}\n",
        );

        let events = event_lines(input, CurrencyId::usd(), ACCOUNT_MONEY_SCALE)
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(events[0].event_id, EventId(42));
        assert_eq!(events[0].ts_unix_ns, 0);
        match &events[0].kind {
            EventKind::InitialCash(initial_cash) => {
                assert_eq!(initial_cash.currency_id, CurrencyId::usd());
            }
            _ => unreachable!("expected initial cash"),
        }
        assert_eq!(events[1].event_id, EventId(43));
        assert_eq!(events[1].ts_unix_ns, 0);
        match &events[1].kind {
            EventKind::Fill(fill) => {
                assert_eq!(
                    fill.fee,
                    Money::parse_decimal("0", CurrencyId::usd(), ACCOUNT_MONEY_SCALE).unwrap()
                );
            }
            _ => unreachable!("expected fill"),
        }
    }

    #[test]
    fn event_schema_ignores_unknown_fields() {
        let input = Cursor::new(
            "{\"seq\":1,\"event_id\":99,\"ts_unix_ns\":123,\"type\":\"mark\",\"instrument_id\":1,\"price\":\"10.00\",\"producer_note\":\"ignored\",\"nested\":{\"also\":\"ignored\"}}\n",
        );

        let event = event_lines(input, CurrencyId::usd(), ACCOUNT_MONEY_SCALE)
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(event.event_id, EventId(99));
        assert_eq!(event.ts_unix_ns, 123);
        assert!(matches!(
            event.kind,
            EventKind::Mark(MarkPriceUpdate {
                instrument_id: InstrumentId(1),
                ..
            })
        ));
    }

    #[test]
    fn event_schema_parses_fill_fee_when_present() {
        let input = Cursor::new(
            "{\"seq\":1,\"type\":\"fill\",\"account_id\":1,\"book_id\":1,\"instrument_id\":1,\"side\":\"buy\",\"qty\":\"2\",\"price\":\"10.00\",\"fee\":\"1.25\",\"fee_currency\":\"USD\"}\n",
        );

        let event = event_lines(input, CurrencyId::usd(), ACCOUNT_MONEY_SCALE)
            .next()
            .unwrap()
            .unwrap();

        match event.kind {
            EventKind::Fill(fill) => {
                assert_eq!(
                    fill.fee,
                    Money::parse_decimal("1.25", CurrencyId::usd(), ACCOUNT_MONEY_SCALE).unwrap()
                );
            }
            _ => unreachable!("expected fill"),
        }
    }

    #[test]
    fn event_schema_parses_trade_correction_fee_when_present() {
        let input = Cursor::new(
            "{\"seq\":2,\"type\":\"trade_correction\",\"original_event_id\":1,\"account_id\":1,\"book_id\":1,\"instrument_id\":1,\"side\":\"sell\",\"qty\":\"2\",\"price\":\"11.00\",\"fee\":\"0.75\",\"fee_currency\":\"USD\"}\n",
        );

        let event = event_lines(input, CurrencyId::usd(), ACCOUNT_MONEY_SCALE)
            .next()
            .unwrap()
            .unwrap();

        match event.kind {
            EventKind::TradeCorrection(correction) => {
                assert_eq!(
                    correction.replacement.fee,
                    Money::parse_decimal("0.75", CurrencyId::usd(), ACCOUNT_MONEY_SCALE).unwrap()
                );
            }
            _ => unreachable!("expected trade correction"),
        }
    }

    #[test]
    fn reports_missing_instrument_fields_before_replay() {
        let mut engine = build_engine(default_config(), CurrencyId::usd()).unwrap();
        let error = load_instrument_rows(
            &mut engine,
            csv::Reader::from_reader(Cursor::new(
                "instrument_id,symbol,currency,price_scale,qty_scale,multiplier\n1,AAPL,USD,4,0\n",
            )),
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("parsing instruments row 1"));
    }

    #[test]
    fn reports_instrument_path_row_field_and_value_context() {
        let mut engine = build_engine(default_config(), CurrencyId::usd()).unwrap();
        let path = temp_file(
            "bad-instruments.csv",
            "instrument_id,symbol,currency,price_scale,qty_scale,multiplier\n1,AAPL,USD,4,0,bad\n",
        );

        let error = load_instruments(&mut engine, &path).unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains(&format!("loading instruments from {}", path.display())));
        assert!(message.contains("parsing instruments row 1 field multiplier value \"bad\""));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn reports_event_path_line_type_field_and_value_context() {
        let path = temp_file(
            "bad-events.ndjson",
            "{\"seq\":1,\"type\":\"mark\",\"instrument_id\":1,\"price\":\"bad\"}\n",
        );

        let error = open_events_for_test(&path, CurrencyId::usd(), ACCOUNT_MONEY_SCALE)
            .unwrap()
            .next()
            .unwrap()
            .unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains(&format!("events line 1 ({})", path.display())));
        assert!(message.contains("type \"mark\""));
        assert!(message.contains("invalid field price value \"bad\""));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn config_defaults_account_and_book() {
        let mut engine = build_engine(default_config(), CurrencyId::usd()).unwrap();

        engine
            .register_instrument(InstrumentMeta {
                instrument_id: InstrumentId(1),
                symbol: "TEST".to_string(),
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
                kind: EventKind::Fill(Fill {
                    account_id: AccountId(1),
                    book_id: BookId(1),
                    instrument_id: InstrumentId(1),
                    side: Side::Buy,
                    qty: Qty::parse_decimal("1").unwrap(),
                    price: Price::parse_decimal("1").unwrap(),
                    fee: Money::zero(CurrencyId::usd(), ACCOUNT_MONEY_SCALE),
                }),
            })
            .unwrap();
    }

    #[test]
    fn config_parses_accounting_method() {
        let mut config = default_config();
        config.accounting_method = Some("fifo".to_string());
        let engine = build_engine(config, CurrencyId::usd()).unwrap();

        assert_eq!(engine.config().accounting_method, AccountingMethod::Fifo);

        let mut config = default_config();
        config.accounting_method = Some("lifo".to_string());
        let engine = build_engine(config, CurrencyId::usd()).unwrap();

        assert_eq!(engine.config().accounting_method, AccountingMethod::Lifo);
    }

    #[test]
    fn config_rejects_unknown_accounting_method() {
        let mut config = default_config();
        config.accounting_method = Some("specific_id".to_string());

        let error = build_engine(config, CurrencyId::usd()).unwrap_err();

        assert!(format!("{error:#}")
            .contains("unsupported field accounting_method value \"specific_id\""));
    }

    #[test]
    fn config_parses_fx_routing_options() {
        let mut config = default_config();
        config.fx_allow_inverse = Some(true);
        config.fx_cross_rate_pivots = Some(vec!["EUR".to_string(), "GBP".to_string()]);

        let engine = build_engine(config, CurrencyId::usd()).unwrap();

        assert!(engine.config().fx_routing.allow_inverse);
        assert_eq!(
            engine.config().fx_routing.cross_rate_pivots,
            vec![
                CurrencyId::from_code("EUR").unwrap(),
                CurrencyId::from_code("GBP").unwrap()
            ]
        );
    }

    #[test]
    fn config_rejects_invalid_fx_cross_rate_pivot() {
        let mut config = default_config();
        config.fx_cross_rate_pivots = Some(vec!["eur".to_string()]);

        let error = build_engine(config, CurrencyId::usd()).unwrap_err();

        assert!(format!("{error:#}").contains("invalid field fx_cross_rate_pivots value \"eur\""));
    }

    fn fixture_dir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("fixtures")
    }

    fn temp_file(name: &str, contents: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("pnl-core-input-test-{}-{name}", std::process::id()));
        std::fs::write(&path, contents).unwrap();
        path
    }
}
