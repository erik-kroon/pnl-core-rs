use crate::engine::Engine;
use crate::error::{Error, Result};

pub(super) fn validate_restored_state(engine: &Engine) -> Result<()> {
    if engine
        .registry
        .ensure_currency(engine.config.base_currency)
        .is_err()
    {
        return Err(Error::SnapshotValidation("missing base currency"));
    }
    for account in engine.accounts.values() {
        if engine.registry.ensure_account(account.account_id).is_err()
            || engine
                .registry
                .account_currency(account.account_id)
                .is_ok_and(|currency| currency != account.base_currency)
        {
            return Err(Error::SnapshotValidation("account metadata invalid"));
        }
        if engine
            .registry
            .ensure_currency(account.base_currency)
            .is_err()
        {
            return Err(Error::SnapshotValidation("account currency missing"));
        }
        if account.cash.currency_id != account.base_currency
            || account.cash.scale != engine.config.account_money_scale
        {
            return Err(Error::SnapshotValidation("invalid account cash"));
        }
    }
    for book in engine.registry.books() {
        let account_id = book.account_id;
        let book_id = book.book_id;
        if !engine.accounts.contains_key(&account_id) {
            return Err(Error::SnapshotValidation("book references missing account"));
        }
        if book_id.0 == 0 {
            return Err(Error::SnapshotValidation("invalid book id"));
        }
    }
    for instrument in engine.registry.instruments() {
        if engine
            .registry
            .ensure_currency(instrument.currency_id)
            .is_err()
        {
            return Err(Error::SnapshotValidation("instrument currency missing"));
        }
    }
    for rate in engine.fx_rates.values() {
        if engine
            .registry
            .ensure_currency(rate.from_currency_id)
            .is_err()
            || engine
                .registry
                .ensure_currency(rate.to_currency_id)
                .is_err()
            || rate.rate.value <= 0
        {
            return Err(Error::SnapshotValidation("invalid fx rate"));
        }
    }
    for position in engine.positions.values() {
        if !engine.accounts.contains_key(&position.key.account_id)
            || engine
                .registry
                .ensure_book(position.key.account_id, position.key.book_id)
                .is_err()
            || engine
                .registry
                .instrument(position.key.instrument_id)
                .is_err()
        {
            return Err(Error::SnapshotValidation("position reference invalid"));
        }
        if position.signed_qty.value == 0 && position.avg_price.is_some() {
            return Err(Error::SnapshotValidation("flat position has avg price"));
        }
        if position.signed_qty.value != 0 && position.avg_price.is_none() {
            return Err(Error::SnapshotValidation("open position missing avg price"));
        }
        let account = engine.accounts.get(&position.key.account_id).unwrap();
        for money in [
            position.cost_basis,
            position.realized_pnl,
            position.unrealized_pnl,
            position.gross_exposure,
            position.net_exposure,
        ] {
            if money.currency_id != account.base_currency
                || money.scale != engine.config.account_money_scale
            {
                return Err(Error::SnapshotValidation("position money invalid"));
            }
        }
    }
    engine
        .replay_journal
        .validate_restored(engine.config.expected_start_seq)?;
    Ok(())
}
