use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use pnl_core::*;
use serde::Deserialize;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "pnl-core")]
#[command(about = "Deterministic fixed-point PnL replay")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Replay {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        instruments: PathBuf,
        #[arg(long)]
        events: PathBuf,
        #[arg(long)]
        summary: bool,
        #[arg(long)]
        positions: bool,
        #[arg(long = "state-hash")]
        state_hash: bool,
        #[arg(long)]
        snapshot_out: Option<PathBuf>,
        #[arg(long)]
        snapshot_json_out: Option<PathBuf>,
    },
}

struct ReplayArgs {
    config: PathBuf,
    instruments: PathBuf,
    events: PathBuf,
    show_summary: bool,
    show_positions: bool,
    show_state_hash: bool,
    snapshot_out: Option<PathBuf>,
    snapshot_json_out: Option<PathBuf>,
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

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Replay {
            config,
            instruments,
            events,
            summary,
            positions,
            state_hash,
            snapshot_out,
            snapshot_json_out,
        } => replay(ReplayArgs {
            config,
            instruments,
            events,
            show_summary: summary,
            show_positions: positions,
            show_state_hash: state_hash,
            snapshot_out,
            snapshot_json_out,
        }),
    }
}

fn replay(args: ReplayArgs) -> Result<()> {
    let config_path = args.config;
    let instruments_path = args.instruments;
    let events_path = args.events;
    let config_text = std::fs::read_to_string(&config_path)
        .with_context(|| format!("reading {}", config_path.display()))?;
    let cli_config: CliConfig = toml::from_str(&config_text)
        .with_context(|| format!("parsing {}", config_path.display()))?;
    let base_currency = CurrencyId::from_code(&cli_config.base_currency)?;
    let mut engine = Engine::new(EngineConfig {
        base_currency,
        account_money_scale: cli_config
            .account_money_scale
            .unwrap_or(ACCOUNT_MONEY_SCALE),
        allow_short: cli_config.allow_short.unwrap_or(true),
        allow_position_flip: cli_config.allow_position_flip.unwrap_or(true),
        expected_start_seq: cli_config.expected_start_seq.unwrap_or(1),
        ..EngineConfig::default()
    });
    engine.register_currency(CurrencyMeta {
        currency_id: base_currency,
        code: cli_config.base_currency.clone(),
        scale: engine.config().account_money_scale,
    })?;
    for currency in cli_config.currencies.unwrap_or_default() {
        engine.register_currency(CurrencyMeta {
            currency_id: CurrencyId::from_code(&currency.code)?,
            code: currency.code,
            scale: currency
                .scale
                .unwrap_or(engine.config().account_money_scale),
        })?;
    }

    let accounts = cli_config.accounts.unwrap_or_else(|| {
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
    let books = cli_config.books.unwrap_or_else(|| {
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

    let mut rdr = csv::Reader::from_path(&instruments_path)
        .with_context(|| format!("reading {}", instruments_path.display()))?;
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

    let file =
        File::open(&events_path).with_context(|| format!("reading {}", events_path.display()))?;
    let reader = BufReader::new(file);
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

    let account_ids: Vec<_> = engine
        .positions()
        .map(|p| p.key.account_id)
        .chain([AccountId(1)])
        .collect();
    let primary_account = account_ids.into_iter().min().unwrap_or(AccountId(1));

    if args.show_summary {
        let summary = engine.account_summary(primary_account)?;
        println!("Events replayed:        {replayed}");
        println!("Final cash:             {}", summary.cash);
        println!("Position value:         {}", summary.position_market_value);
        println!("Equity:                 {}", summary.equity);
        println!("Realized PnL:           {}", summary.realized_pnl);
        println!("Unrealized PnL:         {}", summary.unrealized_pnl);
        println!("Total PnL:              {}", summary.total_pnl);
        println!("Gross exposure:         {}", summary.gross_exposure);
        println!("Net exposure:           {}", summary.net_exposure);
        match summary.leverage {
            Some(leverage) => println!("Leverage:               {leverage}"),
            None => println!("Leverage:               n/a"),
        }
        println!("Open positions:         {}", summary.open_positions);
        println!("Current drawdown:       {}", summary.current_drawdown);
        println!("Max drawdown:           {}", summary.max_drawdown);
        println!(
            "PnL reconciliation:     {}",
            summary.pnl_reconciliation_delta
        );
    }
    if args.show_positions {
        println!("Positions:");
        for position in engine.positions() {
            println!(
                "  account={} book={} instrument={} qty={} avg={:?} realized={} unrealized={} net={}",
                position.key.account_id.0,
                position.key.book_id.0,
                position.key.instrument_id.0,
                position.signed_qty.value,
                position.avg_price,
                position.realized_pnl,
                position.unrealized_pnl,
                position.net_exposure
            );
        }
    }
    if args.show_state_hash {
        println!("State hash:             {}", engine.state_hash().to_hex());
    }
    if let Some(path) = args.snapshot_out {
        let file = File::create(&path).with_context(|| format!("creating {}", path.display()))?;
        engine.write_snapshot(file)?;
    }
    if let Some(path) = args.snapshot_json_out {
        let file = File::create(&path).with_context(|| format!("creating {}", path.display()))?;
        engine.write_snapshot_json(file)?;
    }
    Ok(())
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
