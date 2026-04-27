use pnl_core::*;

struct PnlService {
    engine: Engine,
}

impl PnlService {
    fn new() -> Result<Self> {
        let mut engine = Engine::new(EngineConfig::default());
        engine.register_currency(CurrencyMeta {
            currency_id: CurrencyId::usd(),
            code: "USD".to_string(),
            scale: ACCOUNT_MONEY_SCALE,
        })?;
        engine.register_account(AccountMeta {
            account_id: AccountId(1),
            base_currency: CurrencyId::usd(),
        })?;
        engine.register_book(BookMeta {
            account_id: AccountId(1),
            book_id: BookId(1),
        })?;
        engine.register_instrument(InstrumentMeta {
            instrument_id: InstrumentId(1),
            symbol: "AAPL".to_string(),
            currency_id: CurrencyId::usd(),
            price_scale: 4,
            qty_scale: 0,
            multiplier: FixedI128::one(),
        })?;
        Ok(Self { engine })
    }

    fn ingest(&mut self, event: Event) -> Result<ApplyReceipt> {
        self.engine.apply(event)
    }

    fn account_summary(&self, account_id: AccountId) -> Result<AccountSummary> {
        self.engine.account_summary(account_id)
    }

    fn snapshot_bytes(&self) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        self.engine.write_snapshot_with_metadata(
            &mut bytes,
            SnapshotMetadataOptions {
                producer: "example-service".to_string(),
                build_version: env!("CARGO_PKG_VERSION").to_string(),
                fixture_identifier: None,
                user_notes: Some("periodic checkpoint".to_string()),
            },
        )?;
        Ok(bytes)
    }

    fn restore(snapshot: &[u8]) -> Result<Self> {
        Ok(Self {
            engine: Engine::read_snapshot(snapshot)?,
        })
    }
}

fn main() -> Result<()> {
    let mut service = PnlService::new()?;
    service.ingest(initial_cash(1, "100000.00")?)?;
    service.ingest(fill(2, Side::Buy, "100", "185.00", "1.00")?)?;
    service.ingest(mark(3, "187.50")?)?;

    let snapshot = service.snapshot_bytes()?;
    let restored = PnlService::restore(&snapshot)?;
    let summary = restored.account_summary(AccountId(1))?;

    println!(
        "seq={} equity={} state_hash={}",
        restored.engine.snapshot()?.metadata.last_applied_event_seq,
        summary.equity,
        summary.state_hash.to_hex()
    );

    Ok(())
}

fn initial_cash(seq: u64, amount: &str) -> Result<Event> {
    Ok(Event {
        seq,
        event_id: EventId(seq),
        ts_unix_ns: seq as i64,
        kind: EventKind::InitialCash(InitialCash {
            account_id: AccountId(1),
            currency_id: CurrencyId::usd(),
            amount: Money::parse_decimal(amount, CurrencyId::usd(), ACCOUNT_MONEY_SCALE)?,
        }),
    })
}

fn fill(seq: u64, side: Side, qty: &str, price: &str, fee: &str) -> Result<Event> {
    Ok(Event {
        seq,
        event_id: EventId(seq),
        ts_unix_ns: seq as i64,
        kind: EventKind::Fill(Fill {
            account_id: AccountId(1),
            book_id: BookId(1),
            instrument_id: InstrumentId(1),
            side,
            qty: Qty::parse_decimal(qty)?,
            price: Price::parse_decimal(price)?,
            fee: Money::parse_decimal(fee, CurrencyId::usd(), ACCOUNT_MONEY_SCALE)?,
        }),
    })
}

fn mark(seq: u64, price: &str) -> Result<Event> {
    Ok(Event {
        seq,
        event_id: EventId(seq),
        ts_unix_ns: seq as i64,
        kind: EventKind::Mark(MarkPriceUpdate {
            instrument_id: InstrumentId(1),
            price: Price::parse_decimal(price)?,
        }),
    })
}
