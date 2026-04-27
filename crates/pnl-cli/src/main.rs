use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use pnl_core::*;
use serde::Serialize;
use std::fs::File;
use std::path::PathBuf;

mod input;

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
        #[arg(long, required_unless_present = "snapshot_in")]
        config: Option<PathBuf>,
        #[arg(long, required_unless_present = "snapshot_in")]
        instruments: Option<PathBuf>,
        #[arg(long, required = true)]
        events: Vec<PathBuf>,
        #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
        output: OutputFormat,
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
        #[arg(long)]
        snapshot_in: Option<PathBuf>,
        #[arg(long, default_value = "pnl-cli")]
        snapshot_producer: String,
        #[arg(long, default_value = env!("CARGO_PKG_VERSION"))]
        snapshot_build_version: String,
        #[arg(long)]
        snapshot_fixture_identifier: Option<String>,
        #[arg(long)]
        snapshot_user_notes: Option<String>,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum OutputFormat {
    Text,
    Json,
}

struct ReplayArgs {
    config: Option<PathBuf>,
    instruments: Option<PathBuf>,
    events: Vec<PathBuf>,
    output: OutputFormat,
    show_summary: bool,
    show_positions: bool,
    show_state_hash: bool,
    snapshot_out: Option<PathBuf>,
    snapshot_json_out: Option<PathBuf>,
    snapshot_in: Option<PathBuf>,
    snapshot_metadata: SnapshotMetadataOptions,
}

#[derive(Debug, Serialize)]
struct ReplayOutput {
    replayed_events: u64,
    last_applied_event_seq: u64,
    primary_account_id: u64,
    summary: Option<AccountSummary>,
    positions: Option<Vec<Position>>,
    state_hash: Option<String>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Replay {
            config,
            instruments,
            events,
            output,
            summary,
            positions,
            state_hash,
            snapshot_out,
            snapshot_json_out,
            snapshot_in,
            snapshot_producer,
            snapshot_build_version,
            snapshot_fixture_identifier,
            snapshot_user_notes,
        } => replay(ReplayArgs {
            config,
            instruments,
            events,
            output,
            show_summary: summary,
            show_positions: positions,
            show_state_hash: state_hash,
            snapshot_out,
            snapshot_json_out,
            snapshot_in,
            snapshot_metadata: SnapshotMetadataOptions {
                producer: snapshot_producer,
                build_version: snapshot_build_version,
                fixture_identifier: snapshot_fixture_identifier,
                user_notes: snapshot_user_notes,
            },
        }),
    }
}

fn replay(args: ReplayArgs) -> Result<()> {
    let input::ReplayInput { mut engine, events } =
        match (&args.snapshot_in, &args.config, &args.instruments) {
            (Some(snapshot), _, _) => {
                input::open_replay_input_from_snapshot(snapshot, &args.events)?
            }
            (None, Some(config), Some(instruments)) => {
                input::open_replay_input(config, instruments, &args.events)?
            }
            (None, _, _) => {
                anyhow::bail!("--config and --instruments are required unless --snapshot-in is set")
            }
        };
    let replayed_events = replay_event_files(&mut engine, events)?;

    let account_ids: Vec<_> = engine
        .positions()
        .map(|p| p.key.account_id)
        .chain([AccountId(1)])
        .collect();
    let primary_account = account_ids.into_iter().min().unwrap_or(AccountId(1));

    emit_output(&engine, primary_account, replayed_events, &args)?;

    if let Some(path) = args.snapshot_out {
        let file = File::create(&path).with_context(|| format!("creating {}", path.display()))?;
        engine.write_snapshot_with_metadata(file, args.snapshot_metadata.clone())?;
    }
    if let Some(path) = args.snapshot_json_out {
        let file = File::create(&path).with_context(|| format!("creating {}", path.display()))?;
        engine.write_snapshot_json_with_metadata(file, args.snapshot_metadata)?;
    }
    Ok(())
}

fn emit_output(
    engine: &Engine,
    primary_account: AccountId,
    replayed_events: u64,
    args: &ReplayArgs,
) -> Result<()> {
    let summary = if args.show_summary {
        Some(engine.account_summary(primary_account)?)
    } else {
        None
    };
    let positions = if args.show_positions {
        Some(engine.positions().cloned().collect::<Vec<_>>())
    } else {
        None
    };
    let state_hash = args.show_state_hash.then(|| engine.state_hash().to_hex());

    match args.output {
        OutputFormat::Text => emit_text_output(
            replayed_events,
            summary.as_ref(),
            positions.as_deref(),
            state_hash.as_deref(),
        ),
        OutputFormat::Json => {
            serde_json::to_writer_pretty(
                std::io::stdout(),
                &ReplayOutput {
                    replayed_events,
                    last_applied_event_seq: engine.snapshot()?.metadata.last_applied_event_seq,
                    primary_account_id: primary_account.0,
                    summary,
                    positions,
                    state_hash,
                },
            )?;
            println!();
        }
    }

    Ok(())
}

fn emit_text_output(
    replayed_events: u64,
    summary: Option<&AccountSummary>,
    positions: Option<&[Position]>,
    state_hash: Option<&str>,
) {
    if let Some(summary) = summary {
        println!("Events replayed:        {replayed_events}");
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
    if let Some(positions) = positions {
        println!("Positions:");
        for position in positions {
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
    if let Some(state_hash) = state_hash {
        println!("State hash:             {state_hash}");
    }
}

fn replay_event_files(
    engine: &mut Engine,
    event_files: Vec<input::EventIter<std::io::BufReader<File>>>,
) -> Result<u64> {
    let mut replayed = 0_u64;
    for events in event_files {
        replayed += replay_events(engine, events)?;
    }
    Ok(replayed)
}

fn replay_events(
    engine: &mut Engine,
    events: impl IntoIterator<Item = Result<Event>>,
) -> Result<u64> {
    let mut replayed = 0_u64;
    for event in events {
        engine.apply(event?)?;
        replayed += 1;
    }
    Ok(replayed)
}
