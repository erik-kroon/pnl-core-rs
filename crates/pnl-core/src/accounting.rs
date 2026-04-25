use crate::engine::{Position, PositionKey};
use crate::error::{Error, Result};
use crate::types::{div_round, Money, Price, Qty, RoundingMode};

#[derive(Clone, Copy, Debug)]
pub(crate) struct AverageCostFill {
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
    pub(crate) position: Position,
    pub(crate) cash_delta: Money,
    pub(crate) realized_delta: Money,
}

pub(crate) fn apply_average_cost_fill(
    position: Option<Position>,
    fill: AverageCostFill,
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
        position,
        cash_delta,
        realized_delta,
    })
}

fn weighted_avg_price(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        value_qty_price_multiplier, AccountId, BookId, CurrencyId, FixedI128, InstrumentId,
        ACCOUNT_MONEY_SCALE,
    };

    fn key() -> PositionKey {
        PositionKey {
            account_id: AccountId(1),
            book_id: BookId(1),
            instrument_id: InstrumentId(1),
        }
    }

    fn money(value: &str) -> Money {
        Money::parse_decimal(value, CurrencyId::usd(), ACCOUNT_MONEY_SCALE).unwrap()
    }

    fn price(value: &str) -> Price {
        Price::parse_decimal(value)
            .unwrap()
            .to_scale(4, RoundingMode::HalfEven)
            .unwrap()
    }

    fn fill(side_sign: i128, qty: i128, px: &str, fee: &str, ts_unix_ns: i64) -> AverageCostFill {
        AverageCostFill {
            key: key(),
            qty: Qty::from_units(qty),
            signed_delta: qty * side_sign,
            price: price(px),
            fee: money(fee),
            account_money_scale: ACCOUNT_MONEY_SCALE,
            price_scale: 4,
            rounding: RoundingMode::HalfEven,
            allow_short: true,
            allow_position_flip: true,
            ts_unix_ns,
        }
    }

    fn apply(position: Option<Position>, fill: AverageCostFill) -> Result<AverageCostFillOutcome> {
        apply_average_cost_fill(position, fill, |signed_qty| {
            value_qty_price_multiplier(
                signed_qty,
                0,
                fill.price,
                FixedI128::one(),
                CurrencyId::usd(),
                ACCOUNT_MONEY_SCALE,
                RoundingMode::HalfEven,
            )
        })
    }

    #[test]
    fn adds_to_average_cost_and_partially_closes_with_fee_realized() {
        let first = apply(None, fill(1, 100, "10.00", "0", 1)).unwrap();
        let second = apply(Some(first.position), fill(1, 50, "12.00", "0", 2)).unwrap();
        assert_eq!(second.position.signed_qty, Qty::from_units(150));
        assert_eq!(second.position.avg_price, Some(Price::new(106_667, 4)));
        assert_eq!(second.position.cost_basis, money("1600.00"));

        let close = apply(Some(second.position), fill(-1, 40, "12.00", "1.00", 3)).unwrap();
        assert_eq!(close.cash_delta, money("479.00"));
        assert_eq!(close.realized_delta, money("52.3333"));
        assert_eq!(close.position.signed_qty, Qty::from_units(110));
        assert_eq!(close.position.avg_price, Some(Price::new(106_667, 4)));
        assert_eq!(close.position.cost_basis, money("1173.3333"));
    }

    #[test]
    fn full_close_flattens_cost_basis_and_open_time() {
        let open = apply(None, fill(1, 10, "20.00", "0", 1)).unwrap();
        let close = apply(Some(open.position), fill(-1, 10, "21.00", "0", 2)).unwrap();
        assert_eq!(close.position.signed_qty, Qty::zero(0));
        assert_eq!(close.position.avg_price, None);
        assert_eq!(close.position.cost_basis, money("0.00"));
        assert_eq!(close.position.opened_at_unix_ns, None);
        assert_eq!(close.realized_delta, money("10.00"));
    }

    #[test]
    fn position_flip_realizes_closed_leg_and_starts_new_basis() {
        let open = apply(None, fill(1, 100, "10.00", "0", 1)).unwrap();
        let flip = apply(Some(open.position), fill(-1, 150, "12.00", "0", 2)).unwrap();
        assert_eq!(flip.position.signed_qty, Qty::from_units(-50));
        assert_eq!(flip.position.avg_price, Some(price("12.00")));
        assert_eq!(flip.position.cost_basis, money("-600.00"));
        assert_eq!(flip.position.opened_at_unix_ns, Some(2));
        assert_eq!(flip.realized_delta, money("200.00"));
    }
}
