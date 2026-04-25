use anyhow::{Context, Result};
use pnl_core::*;
use serde::Deserialize;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

#[derive(Debug)]
pub struct ReplayLoadResult {
    pub engine: Engine,
    pub replayed_events: u64,
}

pub fn load_replay(config: &Path, instruments: &Path, events: &Path) -> Result<ReplayLoadResult> {
    let config = load_config(config)?;
    let base_currency = CurrencyId::from_code(&config.base_currency)?;
    let mut engine = build_engine(config, base_currency)?;

    load_instruments(&mut engine, instruments)?;
    let replayed_events = apply_events(&mut engine, events, base_currency)?;

    Ok(ReplayLoadResult {
        engine,
        replayed_events,
    })
}

#[derive(Debug, Deserialize)]
struct CliConfig {
    base_currency: String,
    account_money_scale: Option<u8>,
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

#[derive(Debug, Deserialize)]
struct RawEvent {
    seq: u64,
    event_id: Option<u64>,
    #[serde(default)]
    ts_unix_ns: i64,
    #[serde(rename = "type")]
    event_type: String,
    original_event_id: Option<u64>,
    account_id: Option<u64>,
    book_id: Option<u64>,
    instrument_id: Option<u64>,
    currency: Option<String>,
    fee_currency: Option<String>,
    from_currency: Option<String>,
    to_currency: Option<String>,
    side: Option<String>,
    qty: Option<String>,
    price: Option<String>,
    rate: Option<String>,
    fee: Option<String>,
    amount: Option<String>,
    reason: Option<String>,
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
    let mut rdr =
        csv::Reader::from_path(path).with_context(|| format!("reading {}", path.display()))?;
    for row in rdr.deserialize::<InstrumentRow>() {
        let row = row?;
        let currency_id = CurrencyId::from_code(&row.currency)?;
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
            multiplier: FixedI128::parse_decimal(&row.multiplier)?,
        })?;
    }
    Ok(())
}

fn apply_events(engine: &mut Engine, path: &Path, base_currency: CurrencyId) -> Result<u64> {
    let file = File::open(path).with_context(|| format!("reading {}", path.display()))?;
    let reader = BufReader::new(file);
    apply_event_lines(engine, reader, base_currency)
}

fn apply_event_lines<R: BufRead>(
    engine: &mut Engine,
    reader: R,
    base_currency: CurrencyId,
) -> Result<u64> {
    let mut replayed = 0_u64;
    for (idx, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("reading events line {}", idx + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let raw: RawEvent = serde_json::from_str(&line)
            .with_context(|| format!("parsing events line {}", idx + 1))?;
        let event = raw_event_to_core(raw, base_currency, engine.config().account_money_scale)
            .with_context(|| format!("converting events line {}", idx + 1))?;
        engine.apply(event)?;
        replayed += 1;
    }
    Ok(replayed)
}

fn raw_event_to_core(raw: RawEvent, base_currency: CurrencyId, money_scale: u8) -> Result<Event> {
    let event_id = EventId(raw.event_id.unwrap_or(raw.seq));
    let kind = match raw.event_type.as_str() {
        "initial_cash" => {
            let account_id = AccountId(required(raw.account_id, "account_id")?);
            let currency_id = currency(raw.currency.as_deref(), base_currency)?;
            EventKind::InitialCash(InitialCash {
                account_id,
                currency_id,
                amount: Money::parse_decimal(
                    required(raw.amount.as_deref(), "amount")?,
                    currency_id,
                    money_scale,
                )?,
            })
        }
        "cash_adjustment" => {
            let account_id = AccountId(required(raw.account_id, "account_id")?);
            let currency_id = currency(raw.currency.as_deref(), base_currency)?;
            EventKind::CashAdjustment(CashAdjustment {
                account_id,
                currency_id,
                amount: Money::parse_decimal(
                    required(raw.amount.as_deref(), "amount")?,
                    currency_id,
                    money_scale,
                )?,
                reason: raw.reason,
            })
        }
        "fill" => EventKind::Fill(raw_fill(&raw, base_currency, money_scale)?),
        "trade_correction" => EventKind::TradeCorrection(TradeCorrection {
            original_event_id: EventId(required(raw.original_event_id, "original_event_id")?),
            replacement: raw_fill(&raw, base_currency, money_scale)?,
            reason: raw.reason,
        }),
        "trade_bust" => EventKind::TradeBust(TradeBust {
            original_event_id: EventId(required(raw.original_event_id, "original_event_id")?),
            reason: raw.reason,
        }),
        "mark" => EventKind::Mark(MarkPriceUpdate {
            instrument_id: InstrumentId(required(raw.instrument_id, "instrument_id")?),
            price: Price::parse_decimal(required(raw.price.as_deref(), "price")?)?,
        }),
        "fx_rate" => EventKind::FxRate(FxRateUpdate {
            from_currency_id: CurrencyId::from_code(required(
                raw.from_currency.as_deref(),
                "from_currency",
            )?)?,
            to_currency_id: CurrencyId::from_code(required(
                raw.to_currency.as_deref(),
                "to_currency",
            )?)?,
            rate: Price::parse_decimal(required(raw.rate.as_deref(), "rate")?)?,
        }),
        other => anyhow::bail!("unsupported event type {other}"),
    };
    Ok(Event {
        seq: raw.seq,
        event_id,
        ts_unix_ns: raw.ts_unix_ns,
        kind,
    })
}

fn raw_fill(raw: &RawEvent, base_currency: CurrencyId, money_scale: u8) -> Result<Fill> {
    let side = match required(raw.side.as_deref(), "side")? {
        "buy" => Side::Buy,
        "sell" => Side::Sell,
        other => anyhow::bail!("unsupported side {other}"),
    };
    Ok(Fill {
        account_id: AccountId(required(raw.account_id, "account_id")?),
        book_id: BookId(required(raw.book_id, "book_id")?),
        instrument_id: InstrumentId(required(raw.instrument_id, "instrument_id")?),
        side,
        qty: Qty::parse_decimal(required(raw.qty.as_deref(), "qty")?)?,
        price: Price::parse_decimal(required(raw.price.as_deref(), "price")?)?,
        fee: Money::parse_decimal(
            raw.fee.as_deref().unwrap_or("0"),
            currency(raw.fee_currency.as_deref(), base_currency)?,
            money_scale,
        )?,
    })
}

fn currency(value: Option<&str>, fallback: CurrencyId) -> Result<CurrencyId> {
    match value {
        Some(code) => Ok(CurrencyId::from_code(code)?),
        None => Ok(fallback),
    }
}

fn required<T>(value: Option<T>, name: &str) -> Result<T> {
    value.with_context(|| format!("missing required field {name}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::path::PathBuf;

    #[test]
    fn loads_fixture_replay() {
        let fixtures = fixture_dir();
        let result = load_replay(
            &fixtures.join("config.toml"),
            &fixtures.join("instruments.csv"),
            &fixtures.join("events.ndjson"),
        )
        .unwrap();

        assert_eq!(result.replayed_events, 5);
        assert_eq!(
            result
                .engine
                .account_summary(AccountId(1))
                .unwrap()
                .state_hash,
            result.engine.state_hash()
        );
    }

    #[test]
    fn reports_event_conversion_line_context() {
        let mut engine = build_engine(
            CliConfig {
                base_currency: "USD".to_string(),
                account_money_scale: Some(4),
                allow_short: None,
                allow_position_flip: None,
                expected_start_seq: None,
                currencies: None,
                accounts: None,
                books: None,
            },
            CurrencyId::usd(),
        )
        .unwrap();

        let error = apply_event_lines(
            &mut engine,
            Cursor::new("{\"seq\":1,\"type\":\"fill\",\"side\":\"hold\"}\n"),
            CurrencyId::usd(),
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("converting events line 1"));
    }

    fn fixture_dir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("fixtures")
    }
}
