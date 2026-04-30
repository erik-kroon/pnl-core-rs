use crate::accounting::average_cost::weighted_avg_price;
use crate::accounting::fill::{fill_position_key, FillContext};
use crate::error::{Error, Result};
use crate::position::{Lot, LotId, Position, PositionKey, PositionSide};
use crate::types::{
    div_round, value_qty_price_multiplier, AccountingMethod, CurrencyId, EventId, Money, Price,
    Qty, RoundingMode,
};
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub(crate) struct LotFillInput<'a> {
    pub(crate) position: Option<Position>,
    pub(crate) lots: Vec<Lot>,
    pub(crate) context: FillContext<'a>,
}

#[derive(Clone, Debug)]
pub(crate) struct LotOpeningFillInput<'a> {
    pub(crate) position: Option<Position>,
    pub(crate) context: FillContext<'a>,
}
#[derive(Clone, Copy, Debug)]
struct OpeningLotInput {
    key: PositionKey,
    source_event_id: EventId,
    leg_index: u8,
    opened_seq: u64,
    opened_at_unix_ns: i64,
    qty: Qty,
    entry_price: Price,
    remaining_cost_basis: Money,
    side_sign: i128,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LotFillOutcome {
    pub(crate) key: PositionKey,
    pub(crate) position: Position,
    pub(crate) lots: Vec<Lot>,
    pub(crate) cash_delta: Money,
    pub(crate) realized_delta: Money,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LotOpeningFillOutcome {
    pub(crate) position: Position,
    pub(crate) lot: Lot,
    pub(crate) cash_delta: Money,
    pub(crate) realized_delta: Money,
}
pub(crate) fn apply_lot_opening_fill(
    input: LotOpeningFillInput<'_>,
    qty: Qty,
    signed_delta: i128,
    mut convert_money: impl FnMut(Money, CurrencyId) -> Result<Money>,
) -> Result<LotOpeningFillOutcome> {
    let key = fill_position_key(input.context.fill);
    let fee = convert_money(input.context.fill.fee, input.context.account_currency)?;
    let price = input.context.fill.price.to_scale(
        input.context.instrument.price_scale,
        input.context.config.rounding,
    )?;
    let zero = Money::zero(
        input.context.account_currency,
        input.context.config.account_money_scale,
    );
    let mut position = input.position.unwrap_or_else(|| Position {
        key,
        signed_qty: Qty::zero(qty.scale),
        avg_price: None,
        cost_basis: zero,
        realized_pnl: zero,
        unrealized_pnl: zero,
        gross_exposure: zero,
        net_exposure: zero,
        opened_at_unix_ns: None,
        updated_at_unix_ns: input.context.ts_unix_ns,
    });
    let old_qty = position.signed_qty.value;
    let new_qty = old_qty
        .checked_add(signed_delta)
        .ok_or(Error::ArithmeticOverflow)?;
    if !input.context.config.allow_short && new_qty < 0 {
        return Err(Error::ShortPositionNotAllowed);
    }
    if !input.context.config.allow_position_flip
        && old_qty != 0
        && old_qty.signum() != new_qty.signum()
        && new_qty != 0
    {
        return Err(Error::PositionFlipNotAllowed);
    }
    if old_qty != 0 && old_qty.signum() != signed_delta.signum() {
        return Err(Error::InvalidQuantity);
    }

    let trade_value = value_qty_price_multiplier(
        signed_delta,
        qty.scale,
        price,
        input.context.instrument.multiplier,
        input.context.instrument.currency_id,
        input.context.config.account_money_scale,
        input.context.config.rounding,
    )?;
    let signed_trade_value = convert_money(trade_value, input.context.account_currency)?;
    let cash_delta = signed_trade_value.checked_neg()?.checked_sub(fee)?;
    let realized_delta = fee.checked_neg()?;

    position.avg_price = Some(if old_qty == 0 {
        price
    } else {
        weighted_avg_price(
            old_qty.abs(),
            position.avg_price.unwrap(),
            qty.value,
            price,
            input.context.instrument.price_scale,
            input.context.config.rounding,
        )?
    });
    if old_qty == 0 {
        position.opened_at_unix_ns = Some(input.context.ts_unix_ns);
    }
    position.signed_qty = Qty::new(new_qty, qty.scale);
    position.cost_basis = position.cost_basis.checked_add(signed_trade_value)?;
    position.realized_pnl = position.realized_pnl.checked_add(realized_delta)?;
    position.updated_at_unix_ns = input.context.ts_unix_ns;

    let lot = opening_lot(OpeningLotInput {
        key,
        source_event_id: input.context.event_id,
        leg_index: 0,
        opened_seq: input.context.seq,
        opened_at_unix_ns: input.context.ts_unix_ns,
        qty,
        entry_price: price,
        remaining_cost_basis: signed_trade_value,
        side_sign: signed_delta.signum(),
    })?;

    Ok(LotOpeningFillOutcome {
        position,
        lot,
        cash_delta,
        realized_delta,
    })
}

pub(crate) fn lots_for_position(
    lots: &BTreeMap<(PositionKey, LotId), Lot>,
    key: PositionKey,
) -> Vec<Lot> {
    lots.range(lot_key_range(key))
        .map(|(_, lot)| lot.clone())
        .collect()
}

pub(crate) fn remove_lots_for_position(
    lots: &mut BTreeMap<(PositionKey, LotId), Lot>,
    key: PositionKey,
) {
    let keys: Vec<_> = lots
        .range(lot_key_range(key))
        .map(|(lot_key, _)| *lot_key)
        .collect();
    for lot_key in keys {
        lots.remove(&lot_key);
    }
}

fn lot_key_range(key: PositionKey) -> std::ops::RangeInclusive<(PositionKey, LotId)> {
    (
        key,
        LotId {
            source_event_id: EventId(0),
            leg_index: 0,
        },
    )
        ..=(
            key,
            LotId {
                source_event_id: EventId(u64::MAX),
                leg_index: u8::MAX,
            },
        )
}

pub(crate) fn apply_lot_fill(
    input: LotFillInput<'_>,
    mut convert_money: impl FnMut(Money, CurrencyId) -> Result<Money>,
) -> Result<LotFillOutcome> {
    if !matches!(
        input.context.config.method,
        AccountingMethod::Fifo | AccountingMethod::Lifo
    ) {
        return Err(Error::UnsupportedEventType("lot accounting method"));
    }

    let fee = convert_money(input.context.fill.fee, input.context.account_currency)?;
    let qty = input
        .context
        .fill
        .qty
        .to_scale_exact(input.context.instrument.qty_scale)?;
    if qty.value <= 0 {
        return Err(Error::InvalidQuantity);
    }
    let price = input.context.fill.price.to_scale(
        input.context.instrument.price_scale,
        input.context.config.rounding,
    )?;
    let signed_delta = qty
        .value
        .checked_mul(input.context.fill.side.sign())
        .ok_or(Error::ArithmeticOverflow)?;
    let key = fill_position_key(input.context.fill);
    let zero = Money::zero(
        input.context.account_currency,
        input.context.config.account_money_scale,
    );
    let mut position = input.position.unwrap_or_else(|| Position {
        key,
        signed_qty: Qty::zero(qty.scale),
        avg_price: None,
        cost_basis: zero,
        realized_pnl: zero,
        unrealized_pnl: zero,
        gross_exposure: zero,
        net_exposure: zero,
        opened_at_unix_ns: None,
        updated_at_unix_ns: input.context.ts_unix_ns,
    });
    let old_qty = position.signed_qty.value;
    let new_qty = old_qty
        .checked_add(signed_delta)
        .ok_or(Error::ArithmeticOverflow)?;
    if !input.context.config.allow_short && new_qty < 0 {
        return Err(Error::ShortPositionNotAllowed);
    }
    if !input.context.config.allow_position_flip
        && old_qty != 0
        && old_qty.signum() != new_qty.signum()
        && new_qty != 0
    {
        return Err(Error::PositionFlipNotAllowed);
    }

    let mut value_signed_qty_at_fill_price = |signed_qty| {
        let trade_value = value_qty_price_multiplier(
            signed_qty,
            qty.scale,
            price,
            input.context.instrument.multiplier,
            input.context.instrument.currency_id,
            input.context.config.account_money_scale,
            input.context.config.rounding,
        )?;
        convert_money(trade_value, input.context.account_currency)
    };

    let signed_trade_value = value_signed_qty_at_fill_price(signed_delta)?;
    let cash_delta = signed_trade_value.checked_neg()?.checked_sub(fee)?;
    let mut realized_delta = fee.checked_neg()?;
    let mut lots = input.lots;

    if old_qty == 0 || old_qty.signum() == signed_delta.signum() {
        let opened = opening_lot(OpeningLotInput {
            key,
            source_event_id: input.context.event_id,
            leg_index: 0,
            opened_seq: input.context.seq,
            opened_at_unix_ns: input.context.ts_unix_ns,
            qty,
            entry_price: price,
            remaining_cost_basis: signed_trade_value,
            side_sign: signed_delta.signum(),
        })?;
        lots.push(opened);
    } else {
        let mut remaining_to_close = qty.value.min(old_qty.abs());
        sort_lots_for_close(&mut lots, input.context.config.method);
        for lot in &mut lots {
            if remaining_to_close == 0 {
                break;
            }
            if lot.side.sign() != old_qty.signum() || lot.remaining_qty.value == 0 {
                continue;
            }
            let close_qty = remaining_to_close.min(lot.remaining_qty.value);
            let closed_basis = close_lot_basis(lot, close_qty, input.context.config.rounding)?;
            let closing_signed_qty = close_qty
                .checked_mul(signed_delta.signum())
                .ok_or(Error::ArithmeticOverflow)?;
            let closing_trade_value = value_signed_qty_at_fill_price(closing_signed_qty)?;
            let realized = closing_trade_value
                .checked_neg()?
                .checked_sub(closed_basis)?;
            realized_delta = realized_delta.checked_add(realized)?;
            lot.remaining_qty = Qty::new(
                lot.remaining_qty
                    .value
                    .checked_sub(close_qty)
                    .ok_or(Error::ArithmeticOverflow)?,
                lot.remaining_qty.scale,
            );
            lot.remaining_cost_basis = lot.remaining_cost_basis.checked_sub(closed_basis)?;
            remaining_to_close = remaining_to_close
                .checked_sub(close_qty)
                .ok_or(Error::ArithmeticOverflow)?;
        }
        if remaining_to_close != 0 {
            return Err(Error::InvalidQuantity);
        }
        lots.retain(|lot| lot.remaining_qty.value != 0);

        if new_qty != 0 && old_qty.signum() != new_qty.signum() {
            let opening_basis = value_signed_qty_at_fill_price(new_qty)?;
            lots.push(opening_lot(OpeningLotInput {
                key,
                source_event_id: input.context.event_id,
                leg_index: 1,
                opened_seq: input.context.seq,
                opened_at_unix_ns: input.context.ts_unix_ns,
                qty: Qty::new(new_qty.abs(), qty.scale),
                entry_price: price,
                remaining_cost_basis: opening_basis,
                side_sign: new_qty.signum(),
            })?);
        }
    }

    rebuild_position_from_lots(
        &mut position,
        key,
        lots.as_slice(),
        realized_delta,
        input.context.config.account_money_scale,
        input.context.config.rounding,
        input.context.ts_unix_ns,
    )?;

    Ok(LotFillOutcome {
        key,
        position,
        lots,
        cash_delta,
        realized_delta,
    })
}

fn opening_lot(input: OpeningLotInput) -> Result<Lot> {
    let OpeningLotInput {
        key,
        source_event_id,
        leg_index,
        opened_seq,
        opened_at_unix_ns,
        qty,
        entry_price,
        remaining_cost_basis,
        side_sign,
    } = input;

    if qty.value <= 0 {
        return Err(Error::InvalidQuantity);
    }
    let side = match side_sign {
        1 => PositionSide::Long,
        -1 => PositionSide::Short,
        _ => return Err(Error::InvalidQuantity),
    };
    Ok(Lot {
        lot_id: LotId {
            source_event_id,
            leg_index,
        },
        account_id: key.account_id,
        book_id: key.book_id,
        instrument_id: key.instrument_id,
        side,
        original_qty: qty,
        remaining_qty: qty,
        entry_price,
        remaining_cost_basis,
        opened_event_id: source_event_id,
        opened_seq,
        opened_at_unix_ns,
    })
}

fn sort_lots_for_close(lots: &mut [Lot], method: AccountingMethod) {
    lots.sort_by_key(|lot| (lot.opened_seq, lot.opened_event_id, lot.lot_id.leg_index));
    if method == AccountingMethod::Lifo {
        lots.reverse();
    }
}

fn close_lot_basis(lot: &Lot, close_qty: i128, rounding: RoundingMode) -> Result<Money> {
    if close_qty == lot.remaining_qty.value {
        return Ok(lot.remaining_cost_basis);
    }
    let amount = div_round(
        lot.remaining_cost_basis
            .amount
            .checked_mul(close_qty)
            .ok_or(Error::ArithmeticOverflow)?,
        lot.remaining_qty.value,
        rounding,
    )?;
    Ok(Money::new(
        amount,
        lot.remaining_cost_basis.scale,
        lot.remaining_cost_basis.currency_id,
    ))
}

fn rebuild_position_from_lots(
    position: &mut Position,
    key: PositionKey,
    lots: &[Lot],
    realized_delta: Money,
    account_money_scale: u8,
    rounding: RoundingMode,
    ts_unix_ns: i64,
) -> Result<()> {
    let account_currency = realized_delta.currency_id;
    let zero = Money::zero(account_currency, account_money_scale);
    let mut signed_qty_value = 0_i128;
    let mut cost_basis = zero;
    let mut opened_at_unix_ns: Option<i64> = None;
    let mut total_abs_qty = 0_i128;
    let mut weighted_price_value = 0_i128;
    let mut price_scale = None;
    let qty_scale = lots
        .first()
        .map(|lot| lot.remaining_qty.scale)
        .unwrap_or(position.signed_qty.scale);

    for lot in lots {
        signed_qty_value = signed_qty_value
            .checked_add(
                lot.remaining_qty
                    .value
                    .checked_mul(lot.side.sign())
                    .ok_or(Error::ArithmeticOverflow)?,
            )
            .ok_or(Error::ArithmeticOverflow)?;
        cost_basis = cost_basis.checked_add(lot.remaining_cost_basis)?;
        opened_at_unix_ns = Some(match opened_at_unix_ns {
            Some(current) => current.min(lot.opened_at_unix_ns),
            None => lot.opened_at_unix_ns,
        });
        let target_price_scale = price_scale.unwrap_or(lot.entry_price.scale);
        let entry_price = lot.entry_price.to_scale(target_price_scale, rounding)?;
        price_scale = Some(target_price_scale);
        total_abs_qty = total_abs_qty
            .checked_add(lot.remaining_qty.value)
            .ok_or(Error::ArithmeticOverflow)?;
        weighted_price_value = weighted_price_value
            .checked_add(
                lot.remaining_qty
                    .value
                    .checked_mul(entry_price.value)
                    .ok_or(Error::ArithmeticOverflow)?,
            )
            .ok_or(Error::ArithmeticOverflow)?;
    }

    position.key = key;
    position.signed_qty = Qty::new(signed_qty_value, qty_scale);
    position.avg_price = if total_abs_qty == 0 {
        None
    } else {
        Some(Price::new(
            div_round(weighted_price_value, total_abs_qty, rounding)?,
            price_scale.unwrap_or(0),
        ))
    };
    position.cost_basis = cost_basis;
    position.realized_pnl = position.realized_pnl.checked_add(realized_delta)?;
    position.opened_at_unix_ns = opened_at_unix_ns;
    position.updated_at_unix_ns = ts_unix_ns;
    Ok(())
}
