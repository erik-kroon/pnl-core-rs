use crate::accounting::fill::{fill_position_key, FillContext};
use crate::error::{Error, Result};
use crate::position::{Position, PositionKey};
use crate::types::{
    div_round, value_qty_price_multiplier, CurrencyId, Money, Price, Qty, RoundingMode,
};

#[derive(Clone, Debug)]
pub(crate) struct AverageCostFillInput<'a> {
    pub(crate) position: Option<Position>,
    pub(crate) context: FillContext<'a>,
}
#[derive(Clone, Copy, Debug)]
struct AverageCostPositionFill {
    pub(crate) key: PositionKey,
    pub(crate) qty: Qty,
    pub(crate) signed_delta: i128,
    pub(crate) price: Price,
    pub(crate) fee: Money,
    pub(crate) account_money_scale: u8,
    pub(crate) price_scale: u8,
    pub(crate) rounding: RoundingMode,
    pub(crate) allow_short: bool,
    pub(crate) allow_position_flip: bool,
    pub(crate) ts_unix_ns: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AverageCostFillOutcome {
    pub(crate) key: PositionKey,
    pub(crate) position: Position,
    pub(crate) cash_delta: Money,
    pub(crate) realized_delta: Money,
}
pub(crate) fn apply_average_cost_fill(
    input: AverageCostFillInput<'_>,
    mut convert_money: impl FnMut(Money, CurrencyId) -> Result<Money>,
) -> Result<AverageCostFillOutcome> {
    let fee = convert_money(input.context.fill.fee, input.context.account_currency)?;
    let qty = input
        .context
        .fill
        .qty
        .to_scale_exact(input.context.instrument.qty_scale)?;
    let price = input.context.fill.price.to_scale(
        input.context.instrument.price_scale,
        input.context.config.rounding,
    )?;
    let signed_delta = qty
        .value
        .checked_mul(input.context.fill.side.sign())
        .ok_or(Error::ArithmeticOverflow)?;
    let fill = AverageCostPositionFill {
        key: fill_position_key(input.context.fill),
        qty,
        signed_delta,
        price,
        fee,
        account_money_scale: input.context.config.account_money_scale,
        price_scale: input.context.instrument.price_scale,
        rounding: input.context.config.rounding,
        allow_short: input.context.config.allow_short,
        allow_position_flip: input.context.config.allow_position_flip,
        ts_unix_ns: input.context.ts_unix_ns,
    };
    let mut value_signed_qty_at_fill_price = |signed_qty| {
        let trade_value = value_qty_price_multiplier(
            signed_qty,
            fill.qty.scale,
            fill.price,
            input.context.instrument.multiplier,
            input.context.instrument.currency_id,
            input.context.config.account_money_scale,
            input.context.config.rounding,
        )?;
        convert_money(trade_value, input.context.account_currency)
    };

    apply_average_cost_position_fill(input.position, fill, &mut value_signed_qty_at_fill_price)
}
fn apply_average_cost_position_fill(
    position: Option<Position>,
    fill: AverageCostPositionFill,
    mut value_signed_qty_at_fill_price: impl FnMut(i128) -> Result<Money>,
) -> Result<AverageCostFillOutcome> {
    if fill.qty.value <= 0 {
        return Err(Error::InvalidQuantity);
    }

    let signed_trade_value = value_signed_qty_at_fill_price(fill.signed_delta)?;
    let cash_delta = signed_trade_value.checked_neg()?.checked_sub(fill.fee)?;
    let zero = Money::zero(fill.fee.currency_id, fill.account_money_scale);
    let mut position = position.unwrap_or_else(|| Position {
        key: fill.key,
        signed_qty: Qty::zero(fill.qty.scale),
        avg_price: None,
        cost_basis: zero,
        realized_pnl: zero,
        unrealized_pnl: zero,
        gross_exposure: zero,
        net_exposure: zero,
        opened_at_unix_ns: None,
        updated_at_unix_ns: fill.ts_unix_ns,
    });

    let old_qty = position.signed_qty.value;
    let new_qty = old_qty
        .checked_add(fill.signed_delta)
        .ok_or(Error::ArithmeticOverflow)?;
    if !fill.allow_short && new_qty < 0 {
        return Err(Error::ShortPositionNotAllowed);
    }
    if !fill.allow_position_flip
        && old_qty != 0
        && old_qty.signum() != new_qty.signum()
        && new_qty != 0
    {
        return Err(Error::PositionFlipNotAllowed);
    }

    let mut realized_delta = fill.fee.checked_neg()?;
    if old_qty == 0 || old_qty.signum() == fill.signed_delta.signum() {
        position.avg_price = Some(if old_qty == 0 {
            fill.price
        } else {
            weighted_avg_price(
                old_qty.abs(),
                position.avg_price.unwrap(),
                fill.qty.value,
                fill.price,
                fill.price_scale,
                fill.rounding,
            )?
        });
        if old_qty == 0 {
            position.opened_at_unix_ns = Some(fill.ts_unix_ns);
        }
        position.cost_basis = position.cost_basis.checked_add(signed_trade_value)?;
    } else {
        let closed_qty = old_qty.abs().min(fill.qty.value);
        let old_cost_basis = position.cost_basis;
        let closed_basis_amount = div_round(
            old_cost_basis
                .amount
                .checked_mul(closed_qty)
                .ok_or(Error::ArithmeticOverflow)?,
            old_qty.abs(),
            fill.rounding,
        )?;
        let closed_basis = Money::new(
            closed_basis_amount,
            fill.account_money_scale,
            fill.fee.currency_id,
        );
        let closing_signed_qty = closed_qty
            .checked_mul(fill.signed_delta.signum())
            .ok_or(Error::ArithmeticOverflow)?;
        let closing_trade_value = value_signed_qty_at_fill_price(closing_signed_qty)?;
        let realized = closing_trade_value
            .checked_neg()?
            .checked_sub(closed_basis)?;
        realized_delta = realized_delta.checked_add(realized)?;
        if new_qty == 0 {
            position.avg_price = None;
            position.cost_basis = zero;
            position.opened_at_unix_ns = None;
        } else if old_qty.signum() != new_qty.signum() {
            position.avg_price = Some(fill.price);
            position.cost_basis = value_signed_qty_at_fill_price(new_qty)?;
            position.opened_at_unix_ns = Some(fill.ts_unix_ns);
        } else {
            position.cost_basis = old_cost_basis.checked_sub(closed_basis)?;
        }
    }

    position.signed_qty = Qty::new(new_qty, fill.qty.scale);
    position.realized_pnl = position.realized_pnl.checked_add(realized_delta)?;
    position.updated_at_unix_ns = fill.ts_unix_ns;

    Ok(AverageCostFillOutcome {
        key: fill.key,
        position,
        cash_delta,
        realized_delta,
    })
}
pub(crate) fn weighted_avg_price(
    old_abs_qty: i128,
    old_avg: Price,
    add_abs_qty: i128,
    add_price: Price,
    price_scale: u8,
    rounding: RoundingMode,
) -> Result<Price> {
    let old_price = old_avg.to_scale(price_scale, rounding)?;
    let new_price = add_price.to_scale(price_scale, rounding)?;
    let old_cost = old_abs_qty
        .checked_mul(old_price.value)
        .ok_or(Error::ArithmeticOverflow)?;
    let add_cost = add_abs_qty
        .checked_mul(new_price.value)
        .ok_or(Error::ArithmeticOverflow)?;
    let total_cost = old_cost
        .checked_add(add_cost)
        .ok_or(Error::ArithmeticOverflow)?;
    let total_qty = old_abs_qty
        .checked_add(add_abs_qty)
        .ok_or(Error::ArithmeticOverflow)?;
    Ok(Price::new(
        div_round(total_cost, total_qty, rounding)?,
        price_scale,
    ))
}
