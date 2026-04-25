use crate::engine::{Fill, InstrumentMeta, Position, PositionKey};
use crate::error::{Error, Result};
use crate::types::{
    div_round, value_qty_price_multiplier, CurrencyId, Money, Price, Qty, RoundingMode,
};

#[derive(Clone, Copy, Debug)]
pub(crate) struct AverageCostConfig {
    pub(crate) account_money_scale: u8,
    pub(crate) rounding: RoundingMode,
    pub(crate) allow_short: bool,
    pub(crate) allow_position_flip: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct AverageCostFillInput<'a> {
    pub(crate) position: Option<Position>,
    pub(crate) fill: &'a Fill,
    pub(crate) instrument: &'a InstrumentMeta,
    pub(crate) account_currency: CurrencyId,
    pub(crate) config: AverageCostConfig,
    pub(crate) ts_unix_ns: i64,
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
    let fee = convert_money(input.fill.fee, input.account_currency)?;
    let qty = input.fill.qty.to_scale_exact(input.instrument.qty_scale)?;
    let price = input
        .fill
        .price
        .to_scale(input.instrument.price_scale, input.config.rounding)?;
    let signed_delta = qty
        .value
        .checked_mul(input.fill.side.sign())
        .ok_or(Error::ArithmeticOverflow)?;
    let fill = AverageCostPositionFill {
        key: fill_position_key(input.fill),
        qty,
        signed_delta,
        price,
        fee,
        account_money_scale: input.config.account_money_scale,
        price_scale: input.instrument.price_scale,
        rounding: input.config.rounding,
        allow_short: input.config.allow_short,
        allow_position_flip: input.config.allow_position_flip,
        ts_unix_ns: input.ts_unix_ns,
    };
    let mut value_signed_qty_at_fill_price = |signed_qty| {
        let trade_value = value_qty_price_multiplier(
            signed_qty,
            fill.qty.scale,
            fill.price,
            input.instrument.multiplier,
            input.instrument.currency_id,
            input.config.account_money_scale,
            input.config.rounding,
        )?;
        convert_money(trade_value, input.account_currency)
    };

    apply_average_cost_position_fill(input.position, fill, &mut value_signed_qty_at_fill_price)
}

pub(crate) fn fill_position_key(fill: &Fill) -> PositionKey {
    PositionKey {
        account_id: fill.account_id,
        book_id: fill.book_id,
        instrument_id: fill.instrument_id,
    }
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

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct PositionRevaluation {
    pub(crate) account_currency: CurrencyId,
    pub(crate) account_money_scale: u8,
    pub(crate) valuation_price: Option<Price>,
    pub(crate) instrument_currency: CurrencyId,
    pub(crate) multiplier: crate::types::FixedI128,
    pub(crate) rounding: RoundingMode,
}

#[cfg(test)]
pub(crate) fn revalue_position(
    position: &mut Position,
    input: PositionRevaluation,
    mut convert_money: impl FnMut(Money, CurrencyId) -> Result<Money>,
) -> Result<()> {
    let zero = Money::zero(input.account_currency, input.account_money_scale);
    if position.signed_qty.value == 0 {
        position.unrealized_pnl = zero;
        position.gross_exposure = zero;
        position.net_exposure = zero;
        return Ok(());
    }
    position.net_exposure = if let Some(valuation_price) = input.valuation_price {
        let exposure = value_qty_price_multiplier(
            position.signed_qty.value,
            position.signed_qty.scale,
            valuation_price,
            input.multiplier,
            input.instrument_currency,
            input.account_money_scale,
            input.rounding,
        )?;
        convert_money(exposure, input.account_currency)?
    } else {
        position.cost_basis
    };
    position.gross_exposure = position.net_exposure.abs();
    position.unrealized_pnl = position.net_exposure.checked_sub(position.cost_basis)?;
    Ok(())
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
        AccountId, BookId, CurrencyId, FixedI128, InstrumentId, Side, ACCOUNT_MONEY_SCALE,
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

    fn instrument() -> InstrumentMeta {
        InstrumentMeta {
            instrument_id: InstrumentId(1),
            symbol: "TST".to_string(),
            currency_id: CurrencyId::usd(),
            price_scale: 4,
            qty_scale: 0,
            multiplier: FixedI128::one(),
        }
    }

    fn config() -> AverageCostConfig {
        AverageCostConfig {
            account_money_scale: ACCOUNT_MONEY_SCALE,
            rounding: RoundingMode::HalfEven,
            allow_short: true,
            allow_position_flip: true,
        }
    }

    fn fill(side: Side, qty: i128, px: &str, fee: &str) -> Fill {
        Fill {
            account_id: AccountId(1),
            book_id: BookId(1),
            instrument_id: InstrumentId(1),
            side,
            qty: Qty::from_units(qty),
            price: price(px),
            fee: money(fee),
        }
    }

    fn apply(
        position: Option<Position>,
        fill: Fill,
        ts_unix_ns: i64,
    ) -> Result<AverageCostFillOutcome> {
        let instrument = instrument();
        apply_average_cost_fill(
            AverageCostFillInput {
                position,
                fill: &fill,
                instrument: &instrument,
                account_currency: CurrencyId::usd(),
                config: config(),
                ts_unix_ns,
            },
            |money, to_currency| {
                assert_eq!(money.currency_id, to_currency);
                Ok(money)
            },
        )
    }

    #[test]
    fn fill_position_key_matches_fill_identity() {
        assert_eq!(fill_position_key(&fill(Side::Buy, 1, "10.00", "0")), key());
    }

    #[test]
    fn revalues_open_position_from_mark_price() {
        let mut open = apply(None, fill(Side::Buy, 10, "10.00", "0"), 1)
            .unwrap()
            .position;

        revalue_position(
            &mut open,
            PositionRevaluation {
                account_currency: CurrencyId::usd(),
                account_money_scale: ACCOUNT_MONEY_SCALE,
                valuation_price: Some(price("12.00")),
                instrument_currency: CurrencyId::usd(),
                multiplier: FixedI128::one(),
                rounding: RoundingMode::HalfEven,
            },
            |money, to_currency| {
                assert_eq!(money.currency_id, to_currency);
                Ok(money)
            },
        )
        .unwrap();

        assert_eq!(open.net_exposure, money("120.00"));
        assert_eq!(open.gross_exposure, money("120.00"));
        assert_eq!(open.unrealized_pnl, money("20.00"));
    }

    #[test]
    fn flat_position_revaluation_clears_exposure() {
        let open = apply(None, fill(Side::Buy, 10, "20.00", "0"), 1)
            .unwrap()
            .position;
        let mut closed = apply(Some(open), fill(Side::Sell, 10, "21.00", "0"), 2)
            .unwrap()
            .position;
        closed.net_exposure = money("210.00");
        closed.gross_exposure = money("210.00");
        closed.unrealized_pnl = money("10.00");

        revalue_position(
            &mut closed,
            PositionRevaluation {
                account_currency: CurrencyId::usd(),
                account_money_scale: ACCOUNT_MONEY_SCALE,
                valuation_price: Some(price("22.00")),
                instrument_currency: CurrencyId::usd(),
                multiplier: FixedI128::one(),
                rounding: RoundingMode::HalfEven,
            },
            |money, to_currency| {
                assert_eq!(money.currency_id, to_currency);
                Ok(money)
            },
        )
        .unwrap();

        assert_eq!(closed.net_exposure, money("0.00"));
        assert_eq!(closed.gross_exposure, money("0.00"));
        assert_eq!(closed.unrealized_pnl, money("0.00"));
    }

    #[test]
    fn converts_fee_and_trade_value_through_same_interface() {
        let eur = CurrencyId::from_code_const(*b"EUR");
        let instrument = InstrumentMeta {
            currency_id: eur,
            ..instrument()
        };
        let fill = Fill {
            fee: Money::parse_decimal("2.00", eur, ACCOUNT_MONEY_SCALE).unwrap(),
            ..fill(Side::Buy, 10, "100.00", "0")
        };

        let outcome = apply_average_cost_fill(
            AverageCostFillInput {
                position: None,
                fill: &fill,
                instrument: &instrument,
                account_currency: CurrencyId::usd(),
                config: config(),
                ts_unix_ns: 1,
            },
            |money, to_currency| {
                assert_eq!(to_currency, CurrencyId::usd());
                Ok(Money::new(
                    money.amount.checked_mul(2).unwrap(),
                    money.scale,
                    CurrencyId::usd(),
                ))
            },
        )
        .unwrap();

        assert_eq!(outcome.cash_delta, money("-2004.00"));
        assert_eq!(outcome.realized_delta, money("-4.00"));
        assert_eq!(outcome.position.cost_basis, money("2000.00"));
    }

    #[test]
    fn adds_to_average_cost_and_partially_closes_with_fee_realized() {
        let first = apply(None, fill(Side::Buy, 100, "10.00", "0"), 1).unwrap();
        let second = apply(Some(first.position), fill(Side::Buy, 50, "12.00", "0"), 2).unwrap();
        assert_eq!(second.key, key());
        assert_eq!(second.position.signed_qty, Qty::from_units(150));
        assert_eq!(second.position.avg_price, Some(Price::new(106_667, 4)));
        assert_eq!(second.position.cost_basis, money("1600.00"));

        let close = apply(
            Some(second.position),
            fill(Side::Sell, 40, "12.00", "1.00"),
            3,
        )
        .unwrap();
        assert_eq!(close.cash_delta, money("479.00"));
        assert_eq!(close.realized_delta, money("52.3333"));
        assert_eq!(close.position.signed_qty, Qty::from_units(110));
        assert_eq!(close.position.avg_price, Some(Price::new(106_667, 4)));
        assert_eq!(close.position.cost_basis, money("1173.3333"));
    }

    #[test]
    fn full_close_flattens_cost_basis_and_open_time() {
        let open = apply(None, fill(Side::Buy, 10, "20.00", "0"), 1).unwrap();
        let close = apply(Some(open.position), fill(Side::Sell, 10, "21.00", "0"), 2).unwrap();
        assert_eq!(close.position.signed_qty, Qty::zero(0));
        assert_eq!(close.position.avg_price, None);
        assert_eq!(close.position.cost_basis, money("0.00"));
        assert_eq!(close.position.opened_at_unix_ns, None);
        assert_eq!(close.realized_delta, money("10.00"));
    }

    #[test]
    fn position_flip_realizes_closed_leg_and_starts_new_basis() {
        let open = apply(None, fill(Side::Buy, 100, "10.00", "0"), 1).unwrap();
        let flip = apply(Some(open.position), fill(Side::Sell, 150, "12.00", "0"), 2).unwrap();
        assert_eq!(flip.position.signed_qty, Qty::from_units(-50));
        assert_eq!(flip.position.avg_price, Some(price("12.00")));
        assert_eq!(flip.position.cost_basis, money("-600.00"));
        assert_eq!(flip.position.opened_at_unix_ns, Some(2));
        assert_eq!(flip.realized_delta, money("200.00"));
    }
}
