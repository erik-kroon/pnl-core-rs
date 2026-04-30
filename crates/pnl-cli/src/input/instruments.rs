use super::config::parse_currency_field;
use anyhow::{Context, Result};
use pnl_core::*;
use serde::Deserialize;
use std::io::Read;
use std::path::Path;

#[derive(Debug, Deserialize)]
struct InstrumentRow {
    instrument_id: u64,
    symbol: String,
    currency: String,
    price_scale: u8,
    qty_scale: u8,
    multiplier: String,
}

pub(super) fn load_instruments(engine: &mut Engine, path: &Path) -> Result<()> {
    let rdr =
        csv::Reader::from_path(path).with_context(|| format!("reading {}", path.display()))?;
    load_instrument_rows(engine, rdr)
        .with_context(|| format!("loading instruments from {}", path.display()))
}

pub(super) fn load_instrument_rows<R: Read>(
    engine: &mut Engine,
    mut rdr: csv::Reader<R>,
) -> Result<()> {
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
