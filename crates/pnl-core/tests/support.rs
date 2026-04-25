#![allow(dead_code)]

use pnl_core::*;

pub const ACCOUNT: AccountId = AccountId(1);
pub const BOOK: BookId = BookId(1);
pub const INSTRUMENT: InstrumentId = InstrumentId(1);

#[derive(Clone, Copy, Debug)]
pub struct Scenario {
    pub account_id: AccountId,
    pub book_id: BookId,
    pub instrument_id: InstrumentId,
    pub account_currency_id: CurrencyId,
    pub instrument_currency_id: CurrencyId,
    pub symbol: &'static str,
}

impl Default for Scenario {
    fn default() -> Self {
        Self {
            account_id: ACCOUNT,
            book_id: BOOK,
            instrument_id: INSTRUMENT,
            account_currency_id: CurrencyId::usd(),
            instrument_currency_id: CurrencyId::usd(),
            symbol: "AAPL",
        }
    }
}

impl Scenario {
    pub fn eur_instrument_usd_account() -> Self {
        Self {
            instrument_currency_id: eur(),
            symbol: "SAP",
            ..Self::default()
        }
    }

    pub fn engine(self) -> Engine {
        let mut engine = Engine::new(EngineConfig::default());
        engine
            .register_currency(CurrencyMeta {
                currency_id: self.account_currency_id,
                code: self.account_currency_id.code(),
                scale: ACCOUNT_MONEY_SCALE,
            })
            .unwrap();

        if self.instrument_currency_id != self.account_currency_id {
            engine
                .register_currency(CurrencyMeta {
                    currency_id: self.instrument_currency_id,
                    code: self.instrument_currency_id.code(),
                    scale: ACCOUNT_MONEY_SCALE,
                })
                .unwrap();
        }

        engine
            .register_account(AccountMeta {
                account_id: self.account_id,
                base_currency: self.account_currency_id,
            })
            .unwrap();
        engine
            .register_book(BookMeta {
                account_id: self.account_id,
                book_id: self.book_id,
            })
            .unwrap();
        engine
            .register_instrument(InstrumentMeta {
                instrument_id: self.instrument_id,
                symbol: self.symbol.to_string(),
                currency_id: self.instrument_currency_id,
                price_scale: 4,
                qty_scale: 0,
                multiplier: FixedI128::one(),
            })
            .unwrap();
        engine
    }

    pub fn engine_with_initial_cash(self, seq: u64, amount: &str) -> Engine {
        let mut engine = self.engine();
        engine.apply(self.initial_cash(seq, amount)).unwrap();
        engine
    }

    pub fn initial_cash(self, seq: u64, amount: &str) -> Event {
        Event {
            seq,
            event_id: EventId(seq),
            ts_unix_ns: seq as i64,
            kind: EventKind::InitialCash(InitialCash {
                account_id: self.account_id,
                currency_id: self.account_currency_id,
                amount: money_in(amount, self.account_currency_id),
            }),
        }
    }

    pub fn cash_adjustment(self, seq: u64, amount: &str) -> Event {
        Event {
            seq,
            event_id: EventId(seq),
            ts_unix_ns: seq as i64,
            kind: EventKind::CashAdjustment(CashAdjustment {
                account_id: self.account_id,
                currency_id: self.account_currency_id,
                amount: money_in(amount, self.account_currency_id),
                reason: Some("test".to_string()),
            }),
        }
    }

    pub fn fill(self, seq: u64, side: Side, qty: i128, px: &str, fee: &str) -> Event {
        self.fill_with_fee_currency(seq, side, qty, px, fee, self.account_currency_id)
    }

    pub fn fill_with_fee_currency(
        self,
        seq: u64,
        side: Side,
        qty: i128,
        px: &str,
        fee: &str,
        fee_currency_id: CurrencyId,
    ) -> Event {
        Event {
            seq,
            event_id: EventId(seq),
            ts_unix_ns: seq as i64,
            kind: EventKind::Fill(self.replacement_fill(side, qty, px, fee, fee_currency_id)),
        }
    }

    pub fn replacement_fill(
        self,
        side: Side,
        qty: i128,
        px: &str,
        fee: &str,
        fee_currency_id: CurrencyId,
    ) -> Fill {
        Fill {
            account_id: self.account_id,
            book_id: self.book_id,
            instrument_id: self.instrument_id,
            side,
            qty: Qty::from_units(qty),
            price: price(px),
            fee: money_in(fee, fee_currency_id),
        }
    }

    pub fn mark(self, seq: u64, px: &str) -> Event {
        Event {
            seq,
            event_id: EventId(seq),
            ts_unix_ns: seq as i64,
            kind: EventKind::Mark(MarkPriceUpdate {
                instrument_id: self.instrument_id,
                price: price(px),
            }),
        }
    }

    pub fn fx(
        self,
        seq: u64,
        from_currency_id: CurrencyId,
        to_currency_id: CurrencyId,
        rate: &str,
    ) -> Event {
        Event {
            seq,
            event_id: EventId(seq),
            ts_unix_ns: seq as i64,
            kind: EventKind::FxRate(FxRateUpdate {
                from_currency_id,
                to_currency_id,
                rate: price(rate),
            }),
        }
    }

    pub fn correct_fill(self, seq: u64, original_event_id: EventId, replacement: Fill) -> Event {
        Event {
            seq,
            event_id: EventId(seq),
            ts_unix_ns: seq as i64,
            kind: EventKind::TradeCorrection(TradeCorrection {
                original_event_id,
                replacement,
                reason: Some("test correction".to_string()),
            }),
        }
    }

    pub fn bust_fill(self, seq: u64, original_event_id: EventId) -> Event {
        Event {
            seq,
            event_id: EventId(seq),
            ts_unix_ns: seq as i64,
            kind: EventKind::TradeBust(TradeBust {
                original_event_id,
                reason: Some("test bust".to_string()),
            }),
        }
    }

    pub fn position_key(self) -> PositionKey {
        PositionKey {
            account_id: self.account_id,
            book_id: self.book_id,
            instrument_id: self.instrument_id,
        }
    }
}

pub fn setup() -> Engine {
    Scenario::default().engine()
}

pub fn setup_eur_instrument_usd_account() -> Engine {
    Scenario::eur_instrument_usd_account().engine()
}

pub fn initial(seq: u64, amount: &str) -> Event {
    Scenario::default().initial_cash(seq, amount)
}

pub fn cash(seq: u64, amount: &str) -> Event {
    Scenario::default().cash_adjustment(seq, amount)
}

pub fn fill(seq: u64, side: Side, qty: i128, px: &str, fee: &str) -> Event {
    Scenario::default().fill(seq, side, qty, px, fee)
}

pub fn fill_fee_currency(
    seq: u64,
    side: Side,
    qty: i128,
    px: &str,
    fee: &str,
    fee_currency_id: CurrencyId,
) -> Event {
    Scenario::default().fill_with_fee_currency(seq, side, qty, px, fee, fee_currency_id)
}

pub fn replacement_fill(
    side: Side,
    qty: i128,
    px: &str,
    fee: &str,
    fee_currency_id: CurrencyId,
) -> Fill {
    Scenario::default().replacement_fill(side, qty, px, fee, fee_currency_id)
}

pub fn mark(seq: u64, px: &str) -> Event {
    Scenario::default().mark(seq, px)
}

pub fn fx(seq: u64, from_currency_id: CurrencyId, to_currency_id: CurrencyId, rate: &str) -> Event {
    Scenario::default().fx(seq, from_currency_id, to_currency_id, rate)
}

pub fn correct_fill(seq: u64, original_event_id: EventId, replacement: Fill) -> Event {
    Scenario::default().correct_fill(seq, original_event_id, replacement)
}

pub fn bust_fill(seq: u64, original_event_id: EventId) -> Event {
    Scenario::default().bust_fill(seq, original_event_id)
}

pub fn position_key() -> PositionKey {
    Scenario::default().position_key()
}

pub fn eur() -> CurrencyId {
    CurrencyId::from_code("EUR").unwrap()
}

pub fn money(value: &str) -> Money {
    money_in(value, CurrencyId::usd())
}

pub fn money_in(value: &str, currency_id: CurrencyId) -> Money {
    Money::parse_decimal(value, currency_id, ACCOUNT_MONEY_SCALE).unwrap()
}

pub fn price(value: &str) -> Price {
    Price::parse_decimal(value).unwrap()
}
