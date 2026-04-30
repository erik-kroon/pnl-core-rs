use crate::config::FxRoutingConfig;
use crate::error::{Error, Result};
use crate::event::{FxRateUpdate, MarkPriceUpdate};
use crate::metadata::InstrumentMeta;
use crate::position::{FxRate, Mark, Position, PositionKey};
use crate::registry::Registry;
use crate::types::{
    checked_pow10, convert_money_with_rate, div_round, money_from_components,
    value_qty_price_multiplier, CurrencyId, InstrumentId, Money, Price, RoundingMode,
};
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub(crate) struct ValuationConfig {
    pub(crate) account_money_scale: u8,
    pub(crate) rounding_mode: RoundingMode,
    pub(crate) fx_routing: FxRoutingConfig,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ValuationUpdateResult {
    pub(crate) changed_positions: Vec<PositionKey>,
}

pub(crate) struct ValuationStores<'a> {
    pub(crate) registry: &'a Registry,
    pub(crate) positions: &'a mut BTreeMap<PositionKey, Position>,
    pub(crate) marks: &'a mut BTreeMap<InstrumentId, Mark>,
    pub(crate) fx_rates: &'a mut BTreeMap<(CurrencyId, CurrencyId), FxRate>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FxRateResolution {
    rate: Price,
    used_legs: Vec<(CurrencyId, CurrencyId)>,
}

struct FxRateResolver<'a> {
    rates: &'a BTreeMap<(CurrencyId, CurrencyId), FxRate>,
    config: &'a ValuationConfig,
}

impl<'a> FxRateResolver<'a> {
    fn new(
        rates: &'a BTreeMap<(CurrencyId, CurrencyId), FxRate>,
        config: &'a ValuationConfig,
    ) -> Self {
        Self { rates, config }
    }

    fn resolve(
        &self,
        from_currency_id: CurrencyId,
        to_currency_id: CurrencyId,
    ) -> Result<FxRateResolution> {
        if let Some(resolution) = self.direct_resolution(from_currency_id, to_currency_id) {
            return Ok(resolution);
        }
        if let Some(resolution) = self.leg_resolution(from_currency_id, to_currency_id)? {
            return Ok(resolution);
        }
        for pivot in &self.config.fx_routing.cross_rate_pivots {
            if *pivot == from_currency_id || *pivot == to_currency_id {
                continue;
            }
            let Some(first) = self.leg_resolution(from_currency_id, *pivot)? else {
                continue;
            };
            let Some(second) = self.leg_resolution(*pivot, to_currency_id)? else {
                continue;
            };
            let mut used_legs = first.used_legs;
            used_legs.extend(second.used_legs);
            return Ok(FxRateResolution {
                rate: multiply_rates(first.rate, second.rate)?,
                used_legs,
            });
        }
        Err(Error::MissingFxRate {
            from_currency: from_currency_id,
            to_currency: to_currency_id,
        })
    }

    fn route_uses_pair(
        &self,
        from_currency_id: CurrencyId,
        to_currency_id: CurrencyId,
        updated_from_currency_id: CurrencyId,
        updated_to_currency_id: CurrencyId,
    ) -> bool {
        if from_currency_id == to_currency_id {
            return false;
        }
        let Ok(resolution) = self.resolve(from_currency_id, to_currency_id) else {
            return false;
        };
        resolution
            .used_legs
            .contains(&(updated_from_currency_id, updated_to_currency_id))
    }

    fn direct_resolution(
        &self,
        from_currency_id: CurrencyId,
        to_currency_id: CurrencyId,
    ) -> Option<FxRateResolution> {
        self.direct_rate(from_currency_id, to_currency_id)
            .map(|rate| FxRateResolution {
                rate,
                used_legs: vec![(from_currency_id, to_currency_id)],
            })
    }

    fn leg_resolution(
        &self,
        from_currency_id: CurrencyId,
        to_currency_id: CurrencyId,
    ) -> Result<Option<FxRateResolution>> {
        if let Some(resolution) = self.direct_resolution(from_currency_id, to_currency_id) {
            return Ok(Some(resolution));
        }
        if self.config.fx_routing.allow_inverse {
            if let Some(rate) = self.direct_rate(to_currency_id, from_currency_id) {
                return Ok(Some(FxRateResolution {
                    rate: invert_rate(rate, self.config)?,
                    used_legs: vec![(to_currency_id, from_currency_id)],
                }));
            }
        }
        Ok(None)
    }

    fn direct_rate(
        &self,
        from_currency_id: CurrencyId,
        to_currency_id: CurrencyId,
    ) -> Option<Price> {
        self.rates
            .get(&(from_currency_id, to_currency_id))
            .map(|rate| rate.rate)
    }
}

pub(crate) fn apply_mark_update(
    mark: &MarkPriceUpdate,
    stores: ValuationStores<'_>,
    config: ValuationConfig,
    ts_unix_ns: i64,
) -> Result<ValuationUpdateResult> {
    let instrument = stores.registry.instrument(mark.instrument_id)?.clone();
    let normalized_mark = normalize_mark(mark, &instrument, config.clone(), ts_unix_ns)?;
    let changed_positions = positions_affected_by_mark(stores.positions, mark.instrument_id);
    ensure_resolved_rates_for_positions(
        &changed_positions,
        stores.registry,
        instrument.currency_id,
        stores.fx_rates,
        &config,
    )?;

    stores.marks.insert(mark.instrument_id, normalized_mark);
    for key in &changed_positions {
        let account_currency = stores.registry.account_currency(key.account_id)?;
        let mut position = stores.positions.remove(key).unwrap();
        revalue_position(
            &mut position,
            &instrument,
            account_currency,
            stores.marks.get(&key.instrument_id),
            stores.fx_rates,
            config.clone(),
        )?;
        stores.positions.insert(*key, position);
    }

    Ok(ValuationUpdateResult { changed_positions })
}

pub(crate) fn apply_fx_rate_update(
    fx: &FxRateUpdate,
    stores: ValuationStores<'_>,
    config: ValuationConfig,
    ts_unix_ns: i64,
) -> Result<ValuationUpdateResult> {
    stores.registry.ensure_currency(fx.from_currency_id)?;
    stores.registry.ensure_currency(fx.to_currency_id)?;
    let rate = normalize_fx_rate(fx, config.clone(), ts_unix_ns)?;
    stores
        .fx_rates
        .insert((fx.from_currency_id, fx.to_currency_id), rate);

    let changed_positions = positions_affected_by_fx_rate(
        stores.positions,
        stores.registry,
        fx.from_currency_id,
        fx.to_currency_id,
        stores.fx_rates,
        &config,
    );
    for key in &changed_positions {
        let instrument = stores.registry.instrument(key.instrument_id)?.clone();
        let account_currency = stores.registry.account_currency(key.account_id)?;
        let mut position = stores.positions.remove(key).unwrap();
        revalue_position(
            &mut position,
            &instrument,
            account_currency,
            stores.marks.get(&key.instrument_id),
            stores.fx_rates,
            config.clone(),
        )?;
        stores.positions.insert(*key, position);
    }

    Ok(ValuationUpdateResult { changed_positions })
}

fn normalize_mark(
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

fn normalize_fx_rate(
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
    let rate = FxRateResolver::new(rates, &config)
        .resolve(money.currency_id, to_currency_id)?
        .rate;
    convert_money_with_rate(
        money,
        to_currency_id,
        rate,
        config.account_money_scale,
        config.rounding_mode,
    )
}

pub(crate) fn ensure_resolved_rate(
    rates: &BTreeMap<(CurrencyId, CurrencyId), FxRate>,
    from_currency_id: CurrencyId,
    to_currency_id: CurrencyId,
    config: &ValuationConfig,
) -> Result<()> {
    if from_currency_id == to_currency_id
        || FxRateResolver::new(rates, config)
            .resolve(from_currency_id, to_currency_id)
            .is_ok()
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

fn positions_affected_by_mark(
    positions: &BTreeMap<PositionKey, Position>,
    instrument_id: crate::types::InstrumentId,
) -> Vec<PositionKey> {
    positions
        .keys()
        .copied()
        .filter(|key| key.instrument_id == instrument_id)
        .collect()
}

fn positions_affected_by_fx_rate(
    positions: &BTreeMap<PositionKey, Position>,
    registry: &Registry,
    from_currency_id: CurrencyId,
    to_currency_id: CurrencyId,
    rates: &BTreeMap<(CurrencyId, CurrencyId), FxRate>,
    config: &ValuationConfig,
) -> Vec<PositionKey> {
    positions
        .keys()
        .copied()
        .filter(|key| {
            let Ok(instrument) = registry.instrument(key.instrument_id) else {
                return false;
            };
            let Ok(account_currency) = registry.account_currency(key.account_id) else {
                return false;
            };
            FxRateResolver::new(rates, config).route_uses_pair(
                instrument.currency_id,
                account_currency,
                from_currency_id,
                to_currency_id,
            )
        })
        .collect()
}

fn ensure_resolved_rates_for_positions(
    keys: &[PositionKey],
    registry: &Registry,
    instrument_currency_id: CurrencyId,
    rates: &BTreeMap<(CurrencyId, CurrencyId), FxRate>,
    config: &ValuationConfig,
) -> Result<()> {
    for key in keys {
        let account_currency = registry.account_currency(key.account_id)?;
        ensure_resolved_rate(rates, instrument_currency_id, account_currency, config)?;
    }
    Ok(())
}

fn invert_rate(rate: Price, config: &ValuationConfig) -> Result<Price> {
    if rate.value <= 0 {
        return Err(Error::InvalidPrice);
    }
    let target_scale = rate.scale.max(config.account_money_scale.saturating_add(4));
    let numer_scale = target_scale
        .checked_add(rate.scale)
        .ok_or(Error::ArithmeticOverflow)?;
    let numer = checked_pow10(numer_scale)?;
    Ok(Price::new(
        div_round(numer, rate.value, config.rounding_mode)?,
        target_scale,
    ))
}

fn multiply_rates(first: Price, second: Price) -> Result<Price> {
    Ok(Price::new(
        first
            .value
            .checked_mul(second.value)
            .ok_or(Error::ArithmeticOverflow)?,
        first
            .scale
            .checked_add(second.scale)
            .ok_or(Error::ArithmeticOverflow)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{AccountMeta, BookMeta, CurrencyMeta};
    use crate::types::{AccountId, BookId, FixedI128, InstrumentId, Qty};

    fn usd() -> CurrencyId {
        CurrencyId::usd()
    }

    fn eur() -> CurrencyId {
        CurrencyId::from_code_const(*b"EUR")
    }

    fn gbp() -> CurrencyId {
        CurrencyId::from_code_const(*b"GBP")
    }

    fn config() -> ValuationConfig {
        ValuationConfig {
            account_money_scale: 2,
            rounding_mode: RoundingMode::HalfEven,
            fx_routing: FxRoutingConfig::default(),
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

    fn registry() -> Registry {
        let mut registry = Registry::new();
        registry
            .register_currency(
                CurrencyMeta {
                    currency_id: usd(),
                    code: "USD".to_string(),
                    scale: 2,
                },
                2,
            )
            .unwrap();
        registry
            .register_currency(
                CurrencyMeta {
                    currency_id: eur(),
                    code: "EUR".to_string(),
                    scale: 2,
                },
                2,
            )
            .unwrap();
        registry
            .register_account(AccountMeta {
                account_id: AccountId(1),
                base_currency: usd(),
            })
            .unwrap();
        registry
            .register_book(BookMeta {
                account_id: AccountId(1),
                book_id: BookId(1),
            })
            .unwrap();
        registry.register_instrument(instrument(eur())).unwrap();
        registry
    }

    fn registry_with_gbp() -> Registry {
        let mut registry = registry();
        registry
            .register_currency(
                CurrencyMeta {
                    currency_id: gbp(),
                    code: "GBP".to_string(),
                    scale: 2,
                },
                2,
            )
            .unwrap();
        registry
    }

    fn key() -> PositionKey {
        PositionKey {
            account_id: AccountId(1),
            book_id: BookId(1),
            instrument_id: InstrumentId(1),
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
    fn direct_fx_conversion_wins_over_enabled_inverse() {
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
        rates.insert(
            (usd(), eur()),
            FxRate {
                from_currency_id: usd(),
                to_currency_id: eur(),
                rate: price("0.50"),
                ts_unix_ns: 2,
            },
        );
        let mut config = config();
        config.fx_routing.allow_inverse = true;

        let converted = convert_money(money("10.00", eur()), usd(), &rates, config).unwrap();

        assert_eq!(converted, money("12.00", usd()));
    }

    #[test]
    fn enabled_inverse_fx_conversion_uses_inverse_rate() {
        let mut rates = BTreeMap::new();
        rates.insert(
            (usd(), eur()),
            FxRate {
                from_currency_id: usd(),
                to_currency_id: eur(),
                rate: price("0.80"),
                ts_unix_ns: 1,
            },
        );
        let mut config = config();
        config.fx_routing.allow_inverse = true;

        let converted = convert_money(money("10.00", eur()), usd(), &rates, config).unwrap();

        assert_eq!(converted, money("12.50", usd()));
    }

    #[test]
    fn configured_cross_route_converts_through_first_resolvable_pivot() {
        let gbp = CurrencyId::from_code_const(*b"GBP");
        let mut rates = BTreeMap::new();
        rates.insert(
            (eur(), gbp),
            FxRate {
                from_currency_id: eur(),
                to_currency_id: gbp,
                rate: price("0.80"),
                ts_unix_ns: 1,
            },
        );
        rates.insert(
            (gbp, usd()),
            FxRate {
                from_currency_id: gbp,
                to_currency_id: usd(),
                rate: price("1.25"),
                ts_unix_ns: 2,
            },
        );
        let mut config = config();
        config.fx_routing.cross_rate_pivots = vec![gbp];

        let converted = convert_money(money("10.00", eur()), usd(), &rates, config).unwrap();

        assert_eq!(converted, money("10.00", usd()));
    }

    #[test]
    fn configured_cross_route_uses_pivot_order() {
        let gbp = CurrencyId::from_code_const(*b"GBP");
        let chf = CurrencyId::from_code_const(*b"CHF");
        let mut rates = BTreeMap::new();
        rates.insert(
            (eur(), gbp),
            FxRate {
                from_currency_id: eur(),
                to_currency_id: gbp,
                rate: price("0.80"),
                ts_unix_ns: 1,
            },
        );
        rates.insert(
            (gbp, usd()),
            FxRate {
                from_currency_id: gbp,
                to_currency_id: usd(),
                rate: price("1.25"),
                ts_unix_ns: 2,
            },
        );
        rates.insert(
            (eur(), chf),
            FxRate {
                from_currency_id: eur(),
                to_currency_id: chf,
                rate: price("0.90"),
                ts_unix_ns: 3,
            },
        );
        rates.insert(
            (chf, usd()),
            FxRate {
                from_currency_id: chf,
                to_currency_id: usd(),
                rate: price("2.00"),
                ts_unix_ns: 4,
            },
        );
        let mut config = config();
        config.fx_routing.cross_rate_pivots = vec![chf, gbp];

        let converted = convert_money(money("10.00", eur()), usd(), &rates, config).unwrap();

        assert_eq!(converted, money("18.00", usd()));
    }

    #[test]
    fn fx_rate_resolver_reports_used_rate_legs_for_inverse_pivot_route() {
        let mut rates = BTreeMap::new();
        rates.insert(
            (gbp(), eur()),
            FxRate {
                from_currency_id: gbp(),
                to_currency_id: eur(),
                rate: price("1.25"),
                ts_unix_ns: 1,
            },
        );
        rates.insert(
            (gbp(), usd()),
            FxRate {
                from_currency_id: gbp(),
                to_currency_id: usd(),
                rate: price("1.50"),
                ts_unix_ns: 2,
            },
        );
        let mut config = config();
        config.fx_routing.allow_inverse = true;
        config.fx_routing.cross_rate_pivots = vec![gbp()];

        let resolution = FxRateResolver::new(&rates, &config)
            .resolve(eur(), usd())
            .unwrap();

        assert_eq!(resolution.rate, price("1.20000000"));
        assert_eq!(resolution.used_legs, vec![(gbp(), eur()), (gbp(), usd())]);
    }

    #[test]
    fn fx_rate_affects_positions_matching_instrument_and_account_currency() {
        let key = key();
        let mut positions = BTreeMap::new();
        positions.insert(key, position(money("1000.00", usd())));
        let registry = registry();
        let mut rates = BTreeMap::new();
        rates.insert(
            (eur(), usd()),
            FxRate {
                from_currency_id: eur(),
                to_currency_id: usd(),
                rate: price("1.10"),
                ts_unix_ns: 1,
            },
        );
        let config = config();

        let affected =
            positions_affected_by_fx_rate(&positions, &registry, eur(), usd(), &rates, &config);

        assert_eq!(affected, vec![key]);
    }

    #[test]
    fn mark_update_normalizes_discovers_and_revalues_positions() {
        let key = key();
        let registry = registry();
        let mut positions = BTreeMap::new();
        positions.insert(key, position(money("1000.00", usd())));
        let mut marks = BTreeMap::new();
        let mut rates = BTreeMap::new();
        rates.insert(
            (eur(), usd()),
            FxRate {
                from_currency_id: eur(),
                to_currency_id: usd(),
                rate: price("1.10"),
                ts_unix_ns: 1,
            },
        );

        let result = apply_mark_update(
            &MarkPriceUpdate {
                instrument_id: InstrumentId(1),
                price: price("110.001"),
            },
            ValuationStores {
                registry: &registry,
                positions: &mut positions,
                marks: &mut marks,
                fx_rates: &mut rates,
            },
            config(),
            2,
        )
        .unwrap();

        assert_eq!(result.changed_positions, vec![key]);
        assert_eq!(marks.get(&InstrumentId(1)).unwrap().price, price("110.00"));
        let position = positions.get(&key).unwrap();
        assert_eq!(position.net_exposure, money("1210.00", usd()));
        assert_eq!(position.gross_exposure, money("1210.00", usd()));
        assert_eq!(position.unrealized_pnl, money("210.00", usd()));
    }

    #[test]
    fn fx_update_revalues_positions_using_updated_cross_route_leg() {
        let key = key();
        let registry = registry_with_gbp();
        let mut positions = BTreeMap::new();
        positions.insert(key, position(money("1000.00", usd())));
        let mut marks = BTreeMap::new();
        marks.insert(
            InstrumentId(1),
            Mark {
                instrument_id: InstrumentId(1),
                price: price("110.00"),
                ts_unix_ns: 3,
            },
        );
        let mut rates = BTreeMap::new();
        rates.insert(
            (eur(), gbp()),
            FxRate {
                from_currency_id: eur(),
                to_currency_id: gbp(),
                rate: price("0.80"),
                ts_unix_ns: 1,
            },
        );
        rates.insert(
            (gbp(), usd()),
            FxRate {
                from_currency_id: gbp(),
                to_currency_id: usd(),
                rate: price("1.25"),
                ts_unix_ns: 2,
            },
        );
        let mut config = config();
        config.fx_routing.cross_rate_pivots = vec![gbp()];

        let result = apply_fx_rate_update(
            &FxRateUpdate {
                from_currency_id: gbp(),
                to_currency_id: usd(),
                rate: price("1.50"),
            },
            ValuationStores {
                registry: &registry,
                positions: &mut positions,
                marks: &mut marks,
                fx_rates: &mut rates,
            },
            config,
            4,
        )
        .unwrap();

        assert_eq!(result.changed_positions, vec![key]);
        assert_eq!(rates.get(&(gbp(), usd())).unwrap().rate, price("1.50"));
        let position = positions.get(&key).unwrap();
        assert_eq!(position.net_exposure, money("1320.00", usd()));
        assert_eq!(position.unrealized_pnl, money("320.00", usd()));
    }
}
