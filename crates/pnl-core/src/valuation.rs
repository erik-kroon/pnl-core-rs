use crate::engine::{
    AccountState, FxRate, FxRateUpdate, InstrumentMeta, Mark, MarkPriceUpdate, Position,
    PositionKey,
};
use crate::error::{Error, Result};
use crate::types::{
    convert_money_with_rate, money_from_components, value_qty_price_multiplier, CurrencyId, Money,
    Price, RoundingMode,
};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug)]
pub(crate) struct ValuationConfig {
    pub(crate) account_money_scale: u8,
    pub(crate) rounding_mode: RoundingMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PositionValuation {
    pub(crate) market_value: Money,
    pub(crate) gross_exposure: Money,
    pub(crate) net_exposure: Money,
    pub(crate) unrealized_pnl: Money,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AccountPositionTotals {
    pub(crate) position_market_value: Money,
    pub(crate) gross_exposure: Money,
    pub(crate) net_exposure: Money,
    pub(crate) unrealized_pnl: Money,
    pub(crate) open_positions: u32,
}

pub(crate) fn normalize_mark(
    mark: &MarkPriceUpdate,
    instrument: &InstrumentMeta,
    config: ValuationConfig,
    ts_unix_ns: i64,
) -> Result<Mark> {
    Ok(Mark {
        instrument_id: mark.instrument_id,
        price: mark
            .price
            .to_scale(instrument.price_scale, config.rounding_mode)?,
        ts_unix_ns,
    })
}

pub(crate) fn normalize_fx_rate(
    fx: &FxRateUpdate,
    config: ValuationConfig,
    ts_unix_ns: i64,
) -> Result<FxRate> {
    if fx.rate.value <= 0 {
        return Err(Error::InvalidPrice);
    }
    Ok(FxRate {
        from_currency_id: fx.from_currency_id,
        to_currency_id: fx.to_currency_id,
        rate: fx.rate.to_scale(fx.rate.scale, config.rounding_mode)?,
        ts_unix_ns,
    })
}

pub(crate) fn convert_money(
    money: Money,
    to_currency_id: CurrencyId,
    rates: &BTreeMap<(CurrencyId, CurrencyId), FxRate>,
    config: ValuationConfig,
) -> Result<Money> {
    if money.currency_id == to_currency_id {
        return money_from_components(
            money.amount,
            money.scale,
            to_currency_id,
            config.account_money_scale,
            config.rounding_mode,
        );
    }
    let rate = direct_rate(rates, money.currency_id, to_currency_id)?;
    convert_money_with_rate(
        money,
        to_currency_id,
        rate,
        config.account_money_scale,
        config.rounding_mode,
    )
}

pub(crate) fn ensure_direct_rate(
    rates: &BTreeMap<(CurrencyId, CurrencyId), FxRate>,
    from_currency_id: CurrencyId,
    to_currency_id: CurrencyId,
) -> Result<()> {
    if from_currency_id == to_currency_id || rates.contains_key(&(from_currency_id, to_currency_id))
    {
        return Ok(());
    }
    Err(Error::MissingFxRate {
        from_currency: from_currency_id,
        to_currency: to_currency_id,
    })
}

pub(crate) fn revalue_position(
    position: &mut Position,
    instrument: &InstrumentMeta,
    account_currency: CurrencyId,
    mark: Option<&Mark>,
    rates: &BTreeMap<(CurrencyId, CurrencyId), FxRate>,
    config: ValuationConfig,
) -> Result<()> {
    let valuation = value_position(position, instrument, account_currency, mark, rates, config)?;
    position.net_exposure = valuation.net_exposure;
    position.gross_exposure = valuation.gross_exposure;
    position.unrealized_pnl = valuation.unrealized_pnl;
    Ok(())
}

pub(crate) fn value_position(
    position: &Position,
    instrument: &InstrumentMeta,
    account_currency: CurrencyId,
    mark: Option<&Mark>,
    rates: &BTreeMap<(CurrencyId, CurrencyId), FxRate>,
    config: ValuationConfig,
) -> Result<PositionValuation> {
    let zero = Money::zero(account_currency, config.account_money_scale);
    if position.signed_qty.value == 0 {
        return Ok(PositionValuation {
            market_value: zero,
            gross_exposure: zero,
            net_exposure: zero,
            unrealized_pnl: zero,
        });
    }

    let market_value = if let Some(mark) = mark {
        marked_market_value(
            position,
            instrument,
            mark.price,
            account_currency,
            rates,
            config,
        )?
    } else {
        position.cost_basis
    };
    let unrealized_pnl = market_value.checked_sub(position.cost_basis)?;
    Ok(PositionValuation {
        market_value,
        gross_exposure: market_value.abs(),
        net_exposure: market_value,
        unrealized_pnl,
    })
}

pub(crate) fn marked_market_value(
    position: &Position,
    instrument: &InstrumentMeta,
    mark_price: Price,
    account_currency: CurrencyId,
    rates: &BTreeMap<(CurrencyId, CurrencyId), FxRate>,
    config: ValuationConfig,
) -> Result<Money> {
    let instrument_value = value_qty_price_multiplier(
        position.signed_qty.value,
        position.signed_qty.scale,
        mark_price,
        instrument.multiplier,
        instrument.currency_id,
        config.account_money_scale,
        config.rounding_mode,
    )?;
    convert_money(instrument_value, account_currency, rates, config)
}

pub(crate) fn account_position_totals<'a>(
    positions: impl Iterator<Item = &'a Position>,
    account_currency: CurrencyId,
    account_money_scale: u8,
) -> Result<AccountPositionTotals> {
    let zero = Money::zero(account_currency, account_money_scale);
    let mut totals = AccountPositionTotals {
        position_market_value: zero,
        gross_exposure: zero,
        net_exposure: zero,
        unrealized_pnl: zero,
        open_positions: 0,
    };
    for position in positions {
        if position.signed_qty.value != 0 {
            totals.open_positions = totals
                .open_positions
                .checked_add(1)
                .ok_or(Error::ArithmeticOverflow)?;
        }
        totals.gross_exposure = totals.gross_exposure.checked_add(position.gross_exposure)?;
        totals.net_exposure = totals.net_exposure.checked_add(position.net_exposure)?;
        totals.position_market_value = totals.net_exposure;
        totals.unrealized_pnl = totals.unrealized_pnl.checked_add(position.unrealized_pnl)?;
    }
    Ok(totals)
}

pub(crate) fn positions_affected_by_mark(
    positions: &BTreeMap<PositionKey, Position>,
    instrument_id: crate::types::InstrumentId,
) -> Vec<PositionKey> {
    positions
        .keys()
        .copied()
        .filter(|key| key.instrument_id == instrument_id)
        .collect()
}

pub(crate) fn positions_affected_by_fx_rate(
    positions: &BTreeMap<PositionKey, Position>,
    instruments: &BTreeMap<crate::types::InstrumentId, InstrumentMeta>,
    accounts: &BTreeMap<crate::types::AccountId, AccountState>,
    from_currency_id: CurrencyId,
    to_currency_id: CurrencyId,
) -> Vec<PositionKey> {
    positions
        .keys()
        .copied()
        .filter(|key| {
            let Some(instrument) = instruments.get(&key.instrument_id) else {
                return false;
            };
            let Some(account) = accounts.get(&key.account_id) else {
                return false;
            };
            instrument.currency_id == from_currency_id && account.base_currency == to_currency_id
        })
        .collect()
}

pub(crate) fn ensure_direct_rates_for_positions(
    keys: &[PositionKey],
    accounts: &BTreeMap<crate::types::AccountId, AccountState>,
    instrument_currency_id: CurrencyId,
    rates: &BTreeMap<(CurrencyId, CurrencyId), FxRate>,
) -> Result<()> {
    for key in keys {
        let account_currency = accounts
            .get(&key.account_id)
            .ok_or(Error::UnknownAccount(key.account_id))?
            .base_currency;
        ensure_direct_rate(rates, instrument_currency_id, account_currency)?;
    }
    Ok(())
}

fn direct_rate(
    rates: &BTreeMap<(CurrencyId, CurrencyId), FxRate>,
    from_currency_id: CurrencyId,
    to_currency_id: CurrencyId,
) -> Result<Price> {
    rates
        .get(&(from_currency_id, to_currency_id))
        .map(|rate| rate.rate)
        .ok_or(Error::MissingFxRate {
            from_currency: from_currency_id,
            to_currency: to_currency_id,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{AccountState, InstrumentMeta};
    use crate::types::{AccountId, BookId, FixedI128, InstrumentId, Qty};

    fn usd() -> CurrencyId {
        CurrencyId::usd()
    }

    fn eur() -> CurrencyId {
        CurrencyId::from_code_const(*b"EUR")
    }

    fn config() -> ValuationConfig {
        ValuationConfig {
            account_money_scale: 2,
            rounding_mode: RoundingMode::HalfEven,
        }
    }

    fn money(amount: &str, currency_id: CurrencyId) -> Money {
        Money::parse_decimal(amount, currency_id, 2).unwrap()
    }

    fn price(value: &str) -> Price {
        Price::parse_decimal(value).unwrap()
    }

    fn instrument(currency_id: CurrencyId) -> InstrumentMeta {
        InstrumentMeta {
            instrument_id: InstrumentId(1),
            symbol: "TEST".to_string(),
            currency_id,
            price_scale: 2,
            qty_scale: 0,
            multiplier: FixedI128::one(),
        }
    }

    fn position(cost_basis: Money) -> Position {
        Position {
            key: PositionKey {
                account_id: AccountId(1),
                book_id: BookId(1),
                instrument_id: InstrumentId(1),
            },
            signed_qty: Qty::from_units(10),
            avg_price: Some(price("100.00")),
            cost_basis,
            realized_pnl: money("0", usd()),
            unrealized_pnl: money("0", usd()),
            gross_exposure: money("0", usd()),
            net_exposure: money("0", usd()),
            opened_at_unix_ns: Some(1),
            updated_at_unix_ns: 1,
        }
    }

    #[test]
    fn value_position_falls_back_to_cost_basis_without_mark() {
        let instrument = instrument(usd());
        let position = position(money("1000.00", usd()));
        let valuation = value_position(
            &position,
            &instrument,
            usd(),
            None,
            &BTreeMap::new(),
            config(),
        )
        .unwrap();

        assert_eq!(valuation.market_value, money("1000.00", usd()));
        assert_eq!(valuation.gross_exposure, money("1000.00", usd()));
        assert_eq!(valuation.unrealized_pnl, money("0.00", usd()));
    }

    #[test]
    fn marked_market_value_uses_direct_fx_rate() {
        let instrument = instrument(eur());
        let position = position(money("1100.00", usd()));
        let mut rates = BTreeMap::new();
        rates.insert(
            (eur(), usd()),
            FxRate {
                from_currency_id: eur(),
                to_currency_id: usd(),
                rate: price("1.20"),
                ts_unix_ns: 1,
            },
        );
        let mark = Mark {
            instrument_id: InstrumentId(1),
            price: price("110.00"),
            ts_unix_ns: 2,
        };

        let valuation =
            value_position(&position, &instrument, usd(), Some(&mark), &rates, config()).unwrap();

        assert_eq!(valuation.market_value, money("1320.00", usd()));
        assert_eq!(valuation.net_exposure, money("1320.00", usd()));
        assert_eq!(valuation.unrealized_pnl, money("220.00", usd()));
    }

    #[test]
    fn direct_fx_conversion_does_not_infer_inverse_rate() {
        let mut rates = BTreeMap::new();
        rates.insert(
            (usd(), eur()),
            FxRate {
                from_currency_id: usd(),
                to_currency_id: eur(),
                rate: price("0.90"),
                ts_unix_ns: 1,
            },
        );

        let err = convert_money(money("10.00", eur()), usd(), &rates, config()).unwrap_err();

        assert_eq!(
            err,
            Error::MissingFxRate {
                from_currency: eur(),
                to_currency: usd(),
            }
        );
    }

    #[test]
    fn fx_rate_affects_positions_matching_instrument_and_account_currency() {
        let key = PositionKey {
            account_id: AccountId(1),
            book_id: BookId(1),
            instrument_id: InstrumentId(1),
        };
        let mut positions = BTreeMap::new();
        positions.insert(key, position(money("1000.00", usd())));
        let mut instruments = BTreeMap::new();
        instruments.insert(InstrumentId(1), instrument(eur()));
        let mut accounts = BTreeMap::new();
        accounts.insert(
            AccountId(1),
            AccountState {
                account_id: AccountId(1),
                base_currency: usd(),
                initial_cash: money("0", usd()),
                cash: money("0", usd()),
                net_external_cash_flows: money("0", usd()),
                realized_pnl: money("0", usd()),
                peak_equity: money("0", usd()),
                current_drawdown: money("0", usd()),
                max_drawdown: money("0", usd()),
                initial_cash_set: false,
            },
        );

        let affected =
            positions_affected_by_fx_rate(&positions, &instruments, &accounts, eur(), usd());

        assert_eq!(affected, vec![key]);
    }
}
