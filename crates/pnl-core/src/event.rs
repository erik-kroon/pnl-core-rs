use crate::types::{AccountId, BookId, CurrencyId, EventId, InstrumentId, Money, Price, Qty, Side};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    pub seq: u64,
    pub event_id: EventId,
    pub ts_unix_ns: i64,
    pub kind: EventKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventKind {
    InitialCash(InitialCash),
    CashAdjustment(CashAdjustment),
    Fill(Fill),
    Mark(MarkPriceUpdate),
    FxRate(FxRateUpdate),
    TradeCorrection(TradeCorrection),
    TradeBust(TradeBust),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitialCash {
    pub account_id: AccountId,
    pub currency_id: CurrencyId,
    pub amount: Money,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CashAdjustment {
    pub account_id: AccountId,
    pub currency_id: CurrencyId,
    pub amount: Money,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fill {
    pub account_id: AccountId,
    pub book_id: BookId,
    pub instrument_id: InstrumentId,
    pub side: Side,
    pub qty: Qty,
    pub price: Price,
    pub fee: Money,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeCorrection {
    pub original_event_id: EventId,
    pub replacement: Fill,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeBust {
    pub original_event_id: EventId,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarkPriceUpdate {
    pub instrument_id: InstrumentId,
    pub price: Price,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FxRateUpdate {
    pub from_currency_id: CurrencyId,
    pub to_currency_id: CurrencyId,
    pub rate: Price,
}
