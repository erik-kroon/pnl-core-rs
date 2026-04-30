use crate::error::{Error, Result};
use crate::event::InstrumentSplit;
use crate::position::{FxRate, Lot, LotId, Mark, Position, PositionKey};
use crate::registry::Registry;
use crate::types::{div_round, CurrencyId, InstrumentId, Price, Qty, RoundingMode};
use crate::valuation::{self, ValuationConfig};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CorporateActionStores {
    pub(crate) positions: BTreeMap<PositionKey, Position>,
    pub(crate) lots: BTreeMap<(PositionKey, LotId), Lot>,
    pub(crate) marks: BTreeMap<InstrumentId, Mark>,
}

pub(crate) struct CorporateActionState<'a> {
    pub(crate) registry: &'a Registry,
    pub(crate) positions: &'a BTreeMap<PositionKey, Position>,
    pub(crate) lots: &'a BTreeMap<(PositionKey, LotId), Lot>,
    pub(crate) marks: &'a BTreeMap<InstrumentId, Mark>,
    pub(crate) fx_rates: &'a BTreeMap<(CurrencyId, CurrencyId), FxRate>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CorporateActionUpdate {
    pub(crate) changed_positions: Vec<PositionKey>,
    pub(crate) stores: CorporateActionStores,
}

pub(crate) fn apply_split(
    split: &InstrumentSplit,
    state: CorporateActionState<'_>,
    valuation_config: ValuationConfig,
    rounding: RoundingMode,
    ts_unix_ns: i64,
) -> Result<CorporateActionUpdate> {
    if split.numerator == 0 || split.denominator == 0 {
        return Err(Error::InvalidSplitRatio);
    }

    let instrument = state.registry.instrument(split.instrument_id)?.clone();
    let numerator = i128::from(split.numerator);
    let denominator = i128::from(split.denominator);

    let mut stores = CorporateActionStores {
        positions: state.positions.clone(),
        lots: state.lots.clone(),
        marks: state.marks.clone(),
    };
    let mut changed_positions = Vec::new();

    if let Some(mark) = stores.marks.get_mut(&split.instrument_id) {
        mark.price = adjust_split_price(
            mark.price,
            numerator,
            denominator,
            instrument.price_scale,
            rounding,
        )?;
        mark.ts_unix_ns = ts_unix_ns;
    }

    for lot in stores
        .lots
        .values_mut()
        .filter(|lot| lot.instrument_id == split.instrument_id)
    {
        lot.original_qty = adjust_split_qty(lot.original_qty, numerator, denominator)?;
        lot.remaining_qty = adjust_split_qty(lot.remaining_qty, numerator, denominator)?;
        lot.entry_price = adjust_split_price(
            lot.entry_price,
            numerator,
            denominator,
            instrument.price_scale,
            rounding,
        )?;
    }

    for (key, position) in stores
        .positions
        .iter_mut()
        .filter(|(key, _)| key.instrument_id == split.instrument_id)
    {
        let key = *key;
        let account_currency = state.registry.account_currency(key.account_id)?;
        position.signed_qty = adjust_split_qty(position.signed_qty, numerator, denominator)?;
        position.avg_price = position
            .avg_price
            .map(|price| {
                adjust_split_price(
                    price,
                    numerator,
                    denominator,
                    instrument.price_scale,
                    rounding,
                )
            })
            .transpose()?;
        position.updated_at_unix_ns = ts_unix_ns;
        valuation::revalue_position(
            position,
            &instrument,
            account_currency,
            stores.marks.get(&key.instrument_id),
            state.fx_rates,
            valuation_config.clone(),
        )?;
        changed_positions.push(key);
    }

    Ok(CorporateActionUpdate {
        changed_positions,
        stores,
    })
}

fn adjust_split_qty(qty: Qty, numerator: i128, denominator: i128) -> Result<Qty> {
    let multiplied = qty
        .value
        .checked_mul(numerator)
        .ok_or(Error::ArithmeticOverflow)?;
    if multiplied % denominator != 0 {
        return Err(Error::InvalidScale);
    }
    Ok(Qty::new(multiplied / denominator, qty.scale))
}

fn adjust_split_price(
    price: Price,
    numerator: i128,
    denominator: i128,
    target_scale: u8,
    rounding: RoundingMode,
) -> Result<Price> {
    let multiplied = price
        .value
        .checked_mul(denominator)
        .ok_or(Error::ArithmeticOverflow)?;
    let value = div_round(multiplied, numerator, rounding)?;
    Price::new(value, price.scale).to_scale(target_scale, rounding)
}
