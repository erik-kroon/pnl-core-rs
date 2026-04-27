use crate::types::{AccountId, BookId, CurrencyId, EventId, InstrumentId, Money, Price, Qty};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PositionKey {
    pub account_id: AccountId,
    pub book_id: BookId,
    pub instrument_id: InstrumentId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub key: PositionKey,
    pub signed_qty: Qty,
    pub avg_price: Option<Price>,
    pub cost_basis: Money,
    pub realized_pnl: Money,
    pub unrealized_pnl: Money,
    pub gross_exposure: Money,
    pub net_exposure: Money,
    pub opened_at_unix_ns: Option<i64>,
    pub updated_at_unix_ns: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LotId {
    pub source_event_id: EventId,
    pub leg_index: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PositionSide {
    Long,
    Short,
}

impl PositionSide {
    pub fn sign(self) -> i128 {
        match self {
            PositionSide::Long => 1,
            PositionSide::Short => -1,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lot {
    pub lot_id: LotId,
    pub account_id: AccountId,
    pub book_id: BookId,
    pub instrument_id: InstrumentId,
    pub side: PositionSide,
    pub original_qty: Qty,
    pub remaining_qty: Qty,
    pub entry_price: Price,
    pub remaining_cost_basis: Money,
    pub opened_event_id: EventId,
    pub opened_seq: u64,
    pub opened_at_unix_ns: i64,
}

impl Lot {
    pub fn position_key(&self) -> PositionKey {
        PositionKey {
            account_id: self.account_id,
            book_id: self.book_id,
            instrument_id: self.instrument_id,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mark {
    pub instrument_id: InstrumentId,
    pub price: Price,
    pub ts_unix_ns: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FxRate {
    pub from_currency_id: CurrencyId,
    pub to_currency_id: CurrencyId,
    pub rate: Price,
    pub ts_unix_ns: i64,
}
