use crate::types::{AccountId, BookId, CurrencyId, FixedI128, InstrumentId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrencyMeta {
    pub currency_id: CurrencyId,
    pub code: String,
    pub scale: u8,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountMeta {
    pub account_id: AccountId,
    pub base_currency: CurrencyId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookMeta {
    pub account_id: AccountId,
    pub book_id: BookId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstrumentMeta {
    pub instrument_id: InstrumentId,
    pub symbol: String,
    pub currency_id: CurrencyId,
    pub price_scale: u8,
    pub qty_scale: u8,
    pub multiplier: FixedI128,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum InstrumentLifecycleState {
    Active,
    Halted,
    Delisted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstrumentLifecycleMeta {
    pub instrument_id: InstrumentId,
    pub state: InstrumentLifecycleState,
}
