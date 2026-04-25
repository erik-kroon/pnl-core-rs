use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use pnl_core::*;
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
    let input::ReplayInput { mut engine, events } =
        input::open_replay_input(&args.config, &args.instruments, &args.events)?;
    let replayed_events = replay_events(&mut engine, events)?;

    let account_ids: Vec<_> = engine
        .positions()
        .map(|p| p.key.account_id)
        .chain([AccountId(1)])
        .collect();
    let primary_account = account_ids.into_iter().min().unwrap_or(AccountId(1));

    if args.show_summary {
        let summary = engine.account_summary(primary_account)?;
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
