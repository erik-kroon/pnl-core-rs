use anyhow::{Context, Result};
use pnl_core::*;
use serde::Deserialize;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

mod event_decode;

use event_decode::{decode_event_line, EventDecodeConfig};

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

pub struct EventIter<R> {
    lines: std::io::Lines<R>,
    base_currency: CurrencyId,
    money_scale: u8,
    line_number: usize,
    source: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CliConfig {
    base_currency: String,
    account_money_scale: Option<u8>,
    accounting_method: Option<String>,
    fx_allow_inverse: Option<bool>,
    fx_cross_rate_pivots: Option<Vec<String>>,
    allow_short: Option<bool>,
    allow_position_flip: Option<bool>,
    expected_start_seq: Option<u64>,
    currencies: Option<Vec<CliCurrency>>,
    accounts: Option<Vec<CliAccount>>,
    books: Option<Vec<CliBook>>,
}

#[derive(Debug, Deserialize)]
struct CliCurrency {
    code: String,
    scale: Option<u8>,
}

#[derive(Debug, Deserialize)]
struct CliAccount {
    account_id: u64,
    base_currency: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CliBook {
    account_id: u64,
    book_id: u64,
}

#[derive(Debug, Deserialize)]
struct InstrumentRow {
    instrument_id: u64,
    symbol: String,
    currency: String,
    price_scale: u8,
    qty_scale: u8,
    multiplier: String,
}

fn load_config(path: &Path) -> Result<CliConfig> {
    let config_text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    toml::from_str(&config_text).with_context(|| format!("parsing {}", path.display()))
}

fn build_engine(config: CliConfig, base_currency: CurrencyId) -> Result<Engine> {
    let mut engine = Engine::new(EngineConfig {
        base_currency,
        account_money_scale: config.account_money_scale.unwrap_or(ACCOUNT_MONEY_SCALE),
        accounting_method: parse_accounting_method(config.accounting_method.as_deref())?,
        fx_routing: parse_fx_routing(&config)?,
        allow_short: config.allow_short.unwrap_or(true),
        allow_position_flip: config.allow_position_flip.unwrap_or(true),
        expected_start_seq: config.expected_start_seq.unwrap_or(1),
        ..EngineConfig::default()
    });

    engine.register_currency(CurrencyMeta {
        currency_id: base_currency,
        code: config.base_currency.clone(),
        scale: engine.config().account_money_scale,
    })?;

    for currency in config.currencies.unwrap_or_default() {
        engine.register_currency(CurrencyMeta {
            currency_id: CurrencyId::from_code(&currency.code)?,
            code: currency.code,
            scale: currency
                .scale
                .unwrap_or(engine.config().account_money_scale),
        })?;
    }

    let accounts = config.accounts.unwrap_or_else(|| {
        vec![CliAccount {
            account_id: 1,
            base_currency: None,
        }]
    });
    for account in accounts {
        let account_currency = match account.base_currency {
            Some(code) => {
                let currency_id = CurrencyId::from_code(&code)?;
                engine.register_currency(CurrencyMeta {
                    currency_id,
                    code,
                    scale: engine.config().account_money_scale,
                })?;
                currency_id
            }
            None => base_currency,
        };
        engine.register_account(AccountMeta {
            account_id: AccountId(account.account_id),
            base_currency: account_currency,
        })?;
    }

    let books = config.books.unwrap_or_else(|| {
        vec![CliBook {
            account_id: 1,
            book_id: 1,
        }]
    });
    for book in books {
        engine.register_book(BookMeta {
            account_id: AccountId(book.account_id),
            book_id: BookId(book.book_id),
        })?;
    }

    Ok(engine)
}

fn load_instruments(engine: &mut Engine, path: &Path) -> Result<()> {
    let rdr =
        csv::Reader::from_path(path).with_context(|| format!("reading {}", path.display()))?;
    load_instrument_rows(engine, rdr)
        .with_context(|| format!("loading instruments from {}", path.display()))
}

fn load_instrument_rows<R: Read>(engine: &mut Engine, mut rdr: csv::Reader<R>) -> Result<()> {
    for (idx, row) in rdr.deserialize::<InstrumentRow>().enumerate() {
        let row = row.with_context(|| format!("parsing instruments row {}", idx + 1))?;
        let currency_id = parse_currency_field("currency", &row.currency)
            .with_context(|| format!("parsing instruments row {}", idx + 1))?;
        engine.register_currency(CurrencyMeta {
            currency_id,
            code: row.currency,
            scale: engine.config().account_money_scale,
        })?;
        engine.register_instrument(InstrumentMeta {
            instrument_id: InstrumentId(row.instrument_id),
            symbol: row.symbol,
            currency_id,
            price_scale: row.price_scale,
            qty_scale: row.qty_scale,
            multiplier: FixedI128::parse_decimal(&row.multiplier).with_context(|| {
                format!(
                    "parsing instruments row {} field multiplier value {:?}",
                    idx + 1,
                    row.multiplier
                )
            })?,
        })?;
    }
    Ok(())
}

fn open_events(
    path: &Path,
    base_currency: CurrencyId,
    money_scale: u8,
) -> Result<EventIter<BufReader<File>>> {
    let file = File::open(path).with_context(|| format!("reading {}", path.display()))?;
    let reader = BufReader::new(file);
    Ok(event_lines_with_source(
        reader,
        base_currency,
        money_scale,
        Some(path.display().to_string()),
    ))
}

pub fn open_replay_events(
    paths: &[PathBuf],
    base_currency: CurrencyId,
    money_scale: u8,
) -> Result<Vec<EventIter<BufReader<File>>>> {
    paths
        .iter()
        .map(|path| open_events(path, base_currency, money_scale))
        .collect()
}

#[cfg(test)]
fn event_lines<R: BufRead>(reader: R, base_currency: CurrencyId, money_scale: u8) -> EventIter<R> {
    event_lines_with_source(reader, base_currency, money_scale, None)
}

fn event_lines_with_source<R: BufRead>(
    reader: R,
    base_currency: CurrencyId,
    money_scale: u8,
    source: Option<String>,
) -> EventIter<R> {
    EventIter {
        lines: reader.lines(),
        base_currency,
        money_scale,
        line_number: 0,
        source,
    }
}

impl<R: BufRead> Iterator for EventIter<R> {
    type Item = Result<Event>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let line = self.lines.next()?;
            self.line_number += 1;
            let line_number = self.line_number;
            let line_context = event_line_context(line_number, self.source.as_deref());
            let line = match line.with_context(|| format!("reading {line_context}")) {
                Ok(line) => line,
                Err(error) => return Some(Err(error)),
            };
            if line.trim().is_empty() {
                continue;
            }
            return Some(decode_event_line(
                &line,
                EventDecodeConfig {
                    base_currency: self.base_currency,
                    money_scale: self.money_scale,
                },
                &line_context,
            ));
        }
    }
}

fn parse_currency_field(field: &str, value: &str) -> Result<CurrencyId> {
    CurrencyId::from_code(value).with_context(|| format!("invalid field {field} value {value:?}"))
}

fn parse_accounting_method(value: Option<&str>) -> Result<AccountingMethod> {
    match value.unwrap_or("average_cost") {
        "average_cost" => Ok(AccountingMethod::AverageCost),
        "fifo" => Ok(AccountingMethod::Fifo),
        "lifo" => Ok(AccountingMethod::Lifo),
        other => anyhow::bail!("unsupported field accounting_method value {other:?}"),
    }
}

fn parse_fx_routing(config: &CliConfig) -> Result<FxRoutingConfig> {
    let mut cross_rate_pivots = Vec::new();
    for value in config.fx_cross_rate_pivots.as_deref().unwrap_or(&[]) {
        cross_rate_pivots.push(parse_currency_field("fx_cross_rate_pivots", value)?);
    }
    Ok(FxRoutingConfig {
        allow_inverse: config.fx_allow_inverse.unwrap_or(false),
        cross_rate_pivots,
    })
}

fn event_line_context(line_number: usize, source: Option<&str>) -> String {
    match source {
        Some(source) => format!("events line {line_number} ({source})"),
        None => format!("events line {line_number}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

        let error = open_events(&path, CurrencyId::usd(), ACCOUNT_MONEY_SCALE)
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

    fn default_config() -> CliConfig {
        CliConfig {
            base_currency: "USD".to_string(),
            account_money_scale: None,
            accounting_method: None,
            fx_allow_inverse: None,
            fx_cross_rate_pivots: None,
            allow_short: None,
            allow_position_flip: None,
            expected_start_seq: None,
            currencies: None,
            accounts: None,
            books: None,
        }
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
