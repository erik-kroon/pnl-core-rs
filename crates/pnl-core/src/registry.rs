use crate::error::{Error, Result};
use crate::metadata::{AccountMeta, BookMeta, CurrencyMeta, InstrumentMeta};
use crate::types::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct Registry {
    currencies: BTreeMap<CurrencyId, CurrencyMeta>,
    accounts: BTreeMap<AccountId, AccountMeta>,
    books: BTreeSet<(AccountId, BookId)>,
    instruments: BTreeMap<InstrumentId, InstrumentMeta>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_parts(
        currencies: BTreeMap<CurrencyId, CurrencyMeta>,
        accounts: BTreeMap<AccountId, AccountMeta>,
        books: BTreeSet<(AccountId, BookId)>,
        instruments: BTreeMap<InstrumentId, InstrumentMeta>,
    ) -> Self {
        Self {
            currencies,
            accounts,
            books,
            instruments,
        }
    }

    pub fn currencies(&self) -> impl Iterator<Item = &CurrencyMeta> {
        self.currencies.values()
    }

    pub fn books(&self) -> impl Iterator<Item = BookMeta> + '_ {
        self.books.iter().map(|(account_id, book_id)| BookMeta {
            account_id: *account_id,
            book_id: *book_id,
        })
    }

    pub fn instruments(&self) -> impl Iterator<Item = &InstrumentMeta> {
        self.instruments.values()
    }

    pub fn register_currency(&mut self, meta: CurrencyMeta, account_money_scale: u8) -> Result<()> {
        if meta.scale != account_money_scale {
            return Err(Error::InvalidScale);
        }
        insert_idempotent(
            &mut self.currencies,
            meta.currency_id,
            meta,
            "currency registration conflicts with existing metadata",
        )?;
        Ok(())
    }

    pub fn register_account(&mut self, meta: AccountMeta) -> Result<bool> {
        self.ensure_currency(meta.base_currency)?;
        insert_idempotent(
            &mut self.accounts,
            meta.account_id,
            meta,
            "account registration conflicts with existing metadata",
        )
    }

    pub fn register_book(&mut self, meta: BookMeta) -> Result<()> {
        self.ensure_account(meta.account_id)?;
        self.books.insert((meta.account_id, meta.book_id));
        Ok(())
    }

    pub fn register_instrument(&mut self, meta: InstrumentMeta) -> Result<()> {
        self.ensure_currency(meta.currency_id)?;
        if meta.multiplier.value <= 0 {
            return Err(Error::InvalidScale);
        }
        insert_idempotent(
            &mut self.instruments,
            meta.instrument_id,
            meta,
            "instrument registration conflicts with existing metadata",
        )?;
        Ok(())
    }

    pub fn ensure_currency(&self, currency_id: CurrencyId) -> Result<()> {
        if !self.currencies.contains_key(&currency_id) {
            return Err(Error::UnknownCurrency(currency_id));
        }
        Ok(())
    }

    pub fn ensure_account(&self, account_id: AccountId) -> Result<()> {
        if !self.accounts.contains_key(&account_id) {
            return Err(Error::UnknownAccount(account_id));
        }
        Ok(())
    }

    pub fn ensure_book(&self, account_id: AccountId, book_id: BookId) -> Result<()> {
        if !self.books.contains(&(account_id, book_id)) {
            return Err(Error::UnknownBook {
                account_id,
                book_id,
            });
        }
        Ok(())
    }

    pub fn instrument(&self, instrument_id: InstrumentId) -> Result<&InstrumentMeta> {
        self.instruments
            .get(&instrument_id)
            .ok_or(Error::UnknownInstrument(instrument_id))
    }

    pub fn account_currency(&self, account_id: AccountId) -> Result<CurrencyId> {
        Ok(self
            .accounts
            .get(&account_id)
            .ok_or(Error::UnknownAccount(account_id))?
            .base_currency)
    }

    pub fn ensure_money(
        &self,
        money: Money,
        currency_id: CurrencyId,
        account_money_scale: u8,
    ) -> Result<()> {
        self.ensure_currency(currency_id)?;
        if money.currency_id != currency_id || money.scale != account_money_scale {
            return Err(Error::InvalidScale);
        }
        Ok(())
    }

    pub fn ensure_account_currency(
        &self,
        account_id: AccountId,
        currency_id: CurrencyId,
    ) -> Result<()> {
        let expected_currency = self.account_currency(account_id)?;
        if expected_currency != currency_id {
            return Err(Error::CurrencyMismatch {
                money_currency: currency_id,
                expected_currency,
            });
        }
        Ok(())
    }
}

fn insert_idempotent<K, V>(
    map: &mut BTreeMap<K, V>,
    key: K,
    value: V,
    conflict: &'static str,
) -> Result<bool>
where
    K: Ord,
    V: PartialEq,
{
    if let Some(existing) = map.get(&key) {
        if existing == &value {
            return Ok(false);
        }
        return Err(Error::RegistrationConflict(conflict));
    }
    map.insert(key, value);
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usd() -> CurrencyMeta {
        CurrencyMeta {
            currency_id: CurrencyId::usd(),
            code: "USD".to_owned(),
            scale: ACCOUNT_MONEY_SCALE,
        }
    }

    #[test]
    fn duplicate_currency_registration_is_idempotent() {
        let mut registry = Registry::new();
        registry
            .register_currency(usd(), ACCOUNT_MONEY_SCALE)
            .unwrap();

        registry
            .register_currency(usd(), ACCOUNT_MONEY_SCALE)
            .unwrap();
    }

    #[test]
    fn conflicting_currency_registration_is_rejected() {
        let mut registry = Registry::new();
        registry
            .register_currency(usd(), ACCOUNT_MONEY_SCALE)
            .unwrap();
        let err = registry
            .register_currency(
                CurrencyMeta {
                    currency_id: CurrencyId::usd(),
                    code: "USX".to_owned(),
                    scale: ACCOUNT_MONEY_SCALE,
                },
                ACCOUNT_MONEY_SCALE,
            )
            .unwrap_err();

        assert_eq!(
            err,
            Error::RegistrationConflict("currency registration conflicts with existing metadata")
        );
    }

    #[test]
    fn account_and_book_references_are_validated() {
        let mut registry = Registry::new();
        registry
            .register_currency(usd(), ACCOUNT_MONEY_SCALE)
            .unwrap();

        assert_eq!(
            registry
                .register_book(BookMeta {
                    account_id: AccountId(1),
                    book_id: BookId(1),
                })
                .unwrap_err(),
            Error::UnknownAccount(AccountId(1))
        );

        registry
            .register_account(AccountMeta {
                account_id: AccountId(1),
                base_currency: CurrencyId::usd(),
            })
            .unwrap();
        registry
            .register_book(BookMeta {
                account_id: AccountId(1),
                book_id: BookId(1),
            })
            .unwrap();
        registry.ensure_book(AccountId(1), BookId(1)).unwrap();
    }
}
