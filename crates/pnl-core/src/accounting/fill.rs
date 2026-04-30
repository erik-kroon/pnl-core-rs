use crate::account::AccountState;
use crate::accounting::average_cost::{apply_average_cost_fill, AverageCostFillInput};
use crate::accounting::lots::{
    apply_lot_fill, apply_lot_opening_fill, lots_for_position, remove_lots_for_position,
    LotFillInput, LotOpeningFillInput,
};
use crate::error::{Error, Result};
use crate::event::Fill;
use crate::metadata::InstrumentMeta;
use crate::position::{FxRate, Lot, LotId, Mark, Position, PositionKey};
#[cfg(test)]
use crate::types::{value_qty_price_multiplier, Price, Qty};
use crate::types::{AccountingMethod, CurrencyId, EventId, Money, RoundingMode};
use crate::valuation::{self, ValuationConfig};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug)]
pub(crate) struct FillAccountingConfig {
    pub(crate) account_money_scale: u8,
    pub(crate) rounding: RoundingMode,
    pub(crate) allow_short: bool,
    pub(crate) allow_position_flip: bool,
    pub(crate) method: AccountingMethod,
}

#[derive(Debug)]
pub(crate) struct FillAccountingInput<'a> {
    pub(crate) fill: &'a Fill,
    pub(crate) instrument: &'a InstrumentMeta,
    pub(crate) account_currency: CurrencyId,
    pub(crate) config: FillAccountingConfig,
    pub(crate) valuation_config: ValuationConfig,
    pub(crate) seq: u64,
    pub(crate) event_id: EventId,
    pub(crate) ts_unix_ns: i64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct FillContext<'a> {
    pub(crate) fill: &'a Fill,
    pub(crate) instrument: &'a InstrumentMeta,
    pub(crate) account_currency: CurrencyId,
    pub(crate) config: FillAccountingConfig,
    pub(crate) seq: u64,
    pub(crate) event_id: EventId,
    pub(crate) ts_unix_ns: i64,
}

#[derive(Debug)]
pub(crate) struct FillAccountingState<'a> {
    pub(crate) account: &'a mut AccountState,
    pub(crate) positions: &'a mut BTreeMap<PositionKey, Position>,
    pub(crate) lots: &'a mut BTreeMap<(PositionKey, LotId), Lot>,
    pub(crate) marks: &'a BTreeMap<crate::types::InstrumentId, Mark>,
    pub(crate) fx_rates: &'a BTreeMap<(CurrencyId, CurrencyId), FxRate>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FillAccountingOutcome {
    pub(crate) key: PositionKey,
    pub(crate) cash_delta: Money,
    pub(crate) realized_delta: Money,
}

pub(crate) fn apply_fill(
    input: FillAccountingInput<'_>,
    state: FillAccountingState<'_>,
) -> Result<FillAccountingOutcome> {
    let key = fill_position_key(input.fill);
    let context = FillContext {
        fill: input.fill,
        instrument: input.instrument,
        account_currency: input.account_currency,
        config: input.config,
        seq: input.seq,
        event_id: input.event_id,
        ts_unix_ns: input.ts_unix_ns,
    };
    let mut convert_money = |money, to_currency_id| {
        valuation::convert_money(
            money,
            to_currency_id,
            state.fx_rates,
            input.valuation_config.clone(),
        )
    };

    let (mut position, cash_delta, realized_delta) = match input.config.method {
        AccountingMethod::AverageCost => {
            let position = state.positions.remove(&key);
            let outcome = apply_average_cost_fill(
                AverageCostFillInput { position, context },
                &mut convert_money,
            )?;
            (outcome.position, outcome.cash_delta, outcome.realized_delta)
        }
        AccountingMethod::Fifo | AccountingMethod::Lifo => {
            let position = state.positions.remove(&key);
            let qty = input.fill.qty.to_scale_exact(input.instrument.qty_scale)?;
            if qty.value <= 0 {
                return Err(Error::InvalidQuantity);
            }
            let signed_delta = qty
                .value
                .checked_mul(input.fill.side.sign())
                .ok_or(Error::ArithmeticOverflow)?;
            let old_qty = position
                .as_ref()
                .map(|position| position.signed_qty.value)
                .unwrap_or(0);
            if old_qty == 0 || old_qty.signum() == signed_delta.signum() {
                let outcome = apply_lot_opening_fill(
                    LotOpeningFillInput { position, context },
                    qty,
                    signed_delta,
                    &mut convert_money,
                )?;
                state.lots.insert((key, outcome.lot.lot_id), outcome.lot);
                (outcome.position, outcome.cash_delta, outcome.realized_delta)
            } else {
                let lots = lots_for_position(state.lots, key);
                let outcome = apply_lot_fill(
                    LotFillInput {
                        position,
                        lots,
                        context,
                    },
                    &mut convert_money,
                )?;
                remove_lots_for_position(state.lots, key);
                for lot in outcome.lots {
                    state.lots.insert((key, lot.lot_id), lot);
                }
                (outcome.position, outcome.cash_delta, outcome.realized_delta)
            }
        }
    };

    valuation::revalue_position(
        &mut position,
        input.instrument,
        input.account_currency,
        state.marks.get(&key.instrument_id),
        state.fx_rates,
        input.valuation_config,
    )?;

    state.account.cash = state.account.cash.checked_add(cash_delta)?;
    state.account.trading_realized_pnl = state
        .account
        .trading_realized_pnl
        .checked_add(realized_delta)?;
    state.account.realized_pnl = state.account.realized_pnl.checked_add(realized_delta)?;
    state.positions.insert(key, position);

    Ok(FillAccountingOutcome {
        key,
        cash_delta,
        realized_delta,
    })
}
pub(crate) fn fill_position_key(fill: &Fill) -> PositionKey {
    PositionKey {
        account_id: fill.account_id,
        book_id: fill.book_id,
        instrument_id: fill.instrument_id,
    }
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
#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounting::average_cost::AverageCostFillOutcome;
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

    fn account_state(cash: &str) -> AccountState {
        let zero = money("0");
        AccountState {
            account_id: AccountId(1),
            base_currency: CurrencyId::usd(),
            initial_cash: money(cash),
            cash: money(cash),
            net_external_cash_flows: zero,
            trading_realized_pnl: zero,
            interest_pnl: zero,
            borrow_pnl: zero,
            funding_pnl: zero,
            financing_pnl: zero,
            total_financing_pnl: zero,
            realized_pnl: zero,
            peak_equity: zero,
            current_drawdown: zero,
            max_drawdown: zero,
            initial_cash_set: true,
        }
    }

    fn fill_context<'a>(
        fill: &'a Fill,
        instrument: &'a InstrumentMeta,
        ts_unix_ns: i64,
    ) -> FillContext<'a> {
        FillContext {
            fill,
            instrument,
            account_currency: CurrencyId::usd(),
            config: FillAccountingConfig {
                account_money_scale: ACCOUNT_MONEY_SCALE,
                rounding: RoundingMode::HalfEven,
                allow_short: true,
                allow_position_flip: true,
                method: AccountingMethod::AverageCost,
            },
            seq: 1,
            event_id: EventId(1),
            ts_unix_ns,
        }
    }

    fn fill_accounting_config(method: AccountingMethod) -> FillAccountingConfig {
        FillAccountingConfig {
            account_money_scale: ACCOUNT_MONEY_SCALE,
            rounding: RoundingMode::HalfEven,
            allow_short: true,
            allow_position_flip: true,
            method,
        }
    }

    fn valuation_config() -> ValuationConfig {
        ValuationConfig {
            account_money_scale: ACCOUNT_MONEY_SCALE,
            rounding_mode: RoundingMode::HalfEven,
            fx_routing: crate::config::FxRoutingConfig::default(),
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
                context: fill_context(&fill, &instrument, ts_unix_ns),
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
    fn fill_accounting_lifecycle_updates_account_position_lots_and_valuation() {
        let fill = fill(Side::Buy, 10, "10.00", "1.00");
        let instrument = instrument();
        let mut account = account_state("1000.00");
        let mut positions = BTreeMap::new();
        let mut lots = BTreeMap::new();
        let mut marks = BTreeMap::new();
        marks.insert(
            InstrumentId(1),
            Mark {
                instrument_id: InstrumentId(1),
                price: price("12.00"),
                ts_unix_ns: 1,
            },
        );
        let fx_rates = BTreeMap::new();

        let outcome = apply_fill(
            FillAccountingInput {
                fill: &fill,
                instrument: &instrument,
                account_currency: CurrencyId::usd(),
                config: fill_accounting_config(AccountingMethod::Fifo),
                valuation_config: valuation_config(),
                seq: 1,
                event_id: EventId(1),
                ts_unix_ns: 1,
            },
            FillAccountingState {
                account: &mut account,
                positions: &mut positions,
                lots: &mut lots,
                marks: &marks,
                fx_rates: &fx_rates,
            },
        )
        .unwrap();

        let position = positions.get(&key()).unwrap();
        assert_eq!(outcome.key, key());
        assert_eq!(outcome.cash_delta, money("-101.00"));
        assert_eq!(outcome.realized_delta, money("-1.00"));
        assert_eq!(account.cash, money("899.00"));
        assert_eq!(account.trading_realized_pnl, money("-1.00"));
        assert_eq!(account.realized_pnl, money("-1.00"));
        assert_eq!(position.cost_basis, money("100.00"));
        assert_eq!(position.net_exposure, money("120.00"));
        assert_eq!(position.unrealized_pnl, money("20.00"));
        assert_eq!(lots.len(), 1);
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
                context: fill_context(&fill, &instrument, 1),
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
