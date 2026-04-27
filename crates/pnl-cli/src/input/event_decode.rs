use anyhow::{Context, Result};
use pnl_core::*;
use serde_json::Value;

#[derive(Clone, Copy, Debug)]
pub(super) struct EventDecodeConfig {
    pub(super) base_currency: CurrencyId,
    pub(super) money_scale: u8,
}

#[derive(Clone, Copy)]
struct EventFields<'a> {
    value: &'a Value,
}

struct EventEnvelope<'a> {
    seq: u64,
    event_id: EventId,
    ts_unix_ns: i64,
    event_type: &'a str,
    fields: EventFields<'a>,
}

pub(super) fn decode_event_line(
    line: &str,
    config: EventDecodeConfig,
    line_context: &str,
) -> Result<Event> {
    let value: Value =
        serde_json::from_str(line).with_context(|| format!("parsing {line_context}"))?;
    let envelope = EventEnvelope::from_value(&value)?;
    let event_type = envelope.event_type.to_string();
    envelope
        .decode(config)
        .with_context(|| format!("converting {line_context} type {event_type:?}"))
}

impl<'a> EventEnvelope<'a> {
    fn from_value(value: &'a Value) -> Result<Self> {
        let fields = EventFields { value };
        let seq = fields.required_u64("seq")?;
        Ok(Self {
            seq,
            event_id: EventId(fields.optional_u64("event_id")?.unwrap_or(seq)),
            ts_unix_ns: fields.optional_i64("ts_unix_ns")?.unwrap_or(0),
            event_type: fields.required_str("type")?,
            fields,
        })
    }

    fn decode(self, config: EventDecodeConfig) -> Result<Event> {
        let kind = match self.event_type {
            "initial_cash" => self.fields.initial_cash(config)?,
            "cash_adjustment" => self.fields.cash_adjustment(config)?,
            "interest" => EventKind::Interest(self.fields.financing_event(config)?),
            "borrow" => EventKind::Borrow(self.fields.financing_event(config)?),
            "funding" => EventKind::Funding(self.fields.financing_event(config)?),
            "financing" => EventKind::Financing(self.fields.financing_event(config)?),
            "fill" => EventKind::Fill(self.fields.fill(config)?),
            "trade_correction" => self.fields.trade_correction(config)?,
            "trade_bust" => self.fields.trade_bust()?,
            "mark" => self.fields.mark()?,
            "fx_rate" => self.fields.fx_rate()?,
            "split" => self.fields.split()?,
            "symbol_change" => self.fields.symbol_change()?,
            "instrument_lifecycle" => self.fields.instrument_lifecycle()?,
            other => anyhow::bail!("unsupported field type value {other:?}"),
        };
        Ok(Event {
            seq: self.seq,
            event_id: self.event_id,
            ts_unix_ns: self.ts_unix_ns,
            kind,
        })
    }
}

impl<'a> EventFields<'a> {
    fn initial_cash(self, config: EventDecodeConfig) -> Result<EventKind> {
        let account_id = AccountId(self.required_u64("account_id")?);
        let currency_id = self.currency("currency", config.base_currency)?;
        Ok(EventKind::InitialCash(InitialCash {
            account_id,
            currency_id,
            amount: self.required_money("amount", currency_id, config.money_scale)?,
        }))
    }

    fn cash_adjustment(self, config: EventDecodeConfig) -> Result<EventKind> {
        let account_id = AccountId(self.required_u64("account_id")?);
        let currency_id = self.currency("currency", config.base_currency)?;
        Ok(EventKind::CashAdjustment(CashAdjustment {
            account_id,
            currency_id,
            amount: self.required_money("amount", currency_id, config.money_scale)?,
            reason: self.optional_string("reason")?,
        }))
    }

    fn financing_event(self, config: EventDecodeConfig) -> Result<FinancingEvent> {
        let currency_id = self.currency("currency", config.base_currency)?;
        Ok(FinancingEvent {
            account_id: AccountId(self.required_u64("account_id")?),
            currency_id,
            amount: self.required_money("amount", currency_id, config.money_scale)?,
            reason: self.optional_string("reason")?,
        })
    }

    fn fill(self, config: EventDecodeConfig) -> Result<Fill> {
        let side = match self.required_str("side")? {
            "buy" => Side::Buy,
            "sell" => Side::Sell,
            other => anyhow::bail!("unsupported field side value {other:?}"),
        };
        Ok(Fill {
            account_id: AccountId(self.required_u64("account_id")?),
            book_id: BookId(self.required_u64("book_id")?),
            instrument_id: InstrumentId(self.required_u64("instrument_id")?),
            side,
            qty: self.required_qty("qty")?,
            price: self.required_price("price")?,
            fee: self.money_with_default(
                "fee",
                "0",
                self.currency("fee_currency", config.base_currency)?,
                config.money_scale,
            )?,
        })
    }

    fn trade_correction(self, config: EventDecodeConfig) -> Result<EventKind> {
        Ok(EventKind::TradeCorrection(TradeCorrection {
            original_event_id: EventId(self.required_u64("original_event_id")?),
            replacement: self.fill(config)?,
            reason: self.optional_string("reason")?,
        }))
    }

    fn trade_bust(self) -> Result<EventKind> {
        Ok(EventKind::TradeBust(TradeBust {
            original_event_id: EventId(self.required_u64("original_event_id")?),
            reason: self.optional_string("reason")?,
        }))
    }

    fn mark(self) -> Result<EventKind> {
        Ok(EventKind::Mark(MarkPriceUpdate {
            instrument_id: InstrumentId(self.required_u64("instrument_id")?),
            price: self.required_price("price")?,
        }))
    }

    fn fx_rate(self) -> Result<EventKind> {
        Ok(EventKind::FxRate(FxRateUpdate {
            from_currency_id: self.required_currency("from_currency")?,
            to_currency_id: self.required_currency("to_currency")?,
            rate: self.required_price("rate")?,
        }))
    }

    fn split(self) -> Result<EventKind> {
        Ok(EventKind::Split(InstrumentSplit {
            instrument_id: InstrumentId(self.required_u64("instrument_id")?),
            numerator: self.required_u32("numerator")?,
            denominator: self.required_u32("denominator")?,
            reason: self.optional_string("reason")?,
        }))
    }

    fn symbol_change(self) -> Result<EventKind> {
        Ok(EventKind::SymbolChange(InstrumentSymbolChange {
            instrument_id: InstrumentId(self.required_u64("instrument_id")?),
            symbol: self.required_string("symbol")?,
            reason: self.optional_string("reason")?,
        }))
    }

    fn instrument_lifecycle(self) -> Result<EventKind> {
        Ok(EventKind::InstrumentLifecycle(InstrumentLifecycle {
            instrument_id: InstrumentId(self.required_u64("instrument_id")?),
            state: parse_lifecycle_state(self.required_str("lifecycle_state")?)?,
            reason: self.optional_string("reason")?,
        }))
    }

    fn required_money(&self, field: &str, currency_id: CurrencyId, scale: u8) -> Result<Money> {
        self.money_with_default(field, self.required_str(field)?, currency_id, scale)
    }

    fn money_with_default(
        &self,
        field: &str,
        value: &str,
        currency_id: CurrencyId,
        scale: u8,
    ) -> Result<Money> {
        Money::parse_decimal(value, currency_id, scale)
            .with_context(|| format!("invalid field {field} value {value:?}"))
    }

    fn required_qty(&self, field: &str) -> Result<Qty> {
        let value = self.required_str(field)?;
        Qty::parse_decimal(value).with_context(|| format!("invalid field {field} value {value:?}"))
    }

    fn required_price(&self, field: &str) -> Result<Price> {
        let value = self.required_str(field)?;
        Price::parse_decimal(value)
            .with_context(|| format!("invalid field {field} value {value:?}"))
    }

    fn currency(&self, field: &str, fallback: CurrencyId) -> Result<CurrencyId> {
        match self.optional_str(field)? {
            Some(code) => parse_currency_field(field, code),
            None => Ok(fallback),
        }
    }

    fn required_currency(&self, field: &str) -> Result<CurrencyId> {
        parse_currency_field(field, self.required_str(field)?)
    }

    fn optional_str(&self, field: &str) -> Result<Option<&'a str>> {
        match self.field(field) {
            Some(Value::String(value)) => Ok(Some(value)),
            Some(value) => anyhow::bail!("invalid field {field} value {}", field_value(value)),
            None => Ok(None),
        }
    }

    fn required_str(&self, field: &str) -> Result<&'a str> {
        self.optional_str(field)?
            .with_context(|| format!("missing required field {field}"))
    }

    fn optional_string(&self, field: &str) -> Result<Option<String>> {
        Ok(self.optional_str(field)?.map(str::to_string))
    }

    fn required_string(&self, field: &str) -> Result<String> {
        Ok(self.required_str(field)?.to_string())
    }

    fn optional_u64(&self, field: &str) -> Result<Option<u64>> {
        match self.field(field) {
            Some(value) => value
                .as_u64()
                .with_context(|| format!("invalid field {field} value {}", field_value(value)))
                .map(Some),
            None => Ok(None),
        }
    }

    fn required_u64(&self, field: &str) -> Result<u64> {
        self.optional_u64(field)?
            .with_context(|| format!("missing required field {field}"))
    }

    fn optional_i64(&self, field: &str) -> Result<Option<i64>> {
        match self.field(field) {
            Some(value) => value
                .as_i64()
                .with_context(|| format!("invalid field {field} value {}", field_value(value)))
                .map(Some),
            None => Ok(None),
        }
    }

    fn required_u32(&self, field: &str) -> Result<u32> {
        let value = self.required_u64(field)?;
        u32::try_from(value).with_context(|| {
            format!(
                "invalid field {field} value {}",
                field_value(self.field(field).unwrap())
            )
        })
    }

    fn field(&self, field: &str) -> Option<&'a Value> {
        self.value.get(field)
    }
}

fn parse_currency_field(field: &str, value: &str) -> Result<CurrencyId> {
    CurrencyId::from_code(value).with_context(|| format!("invalid field {field} value {value:?}"))
}

fn parse_lifecycle_state(value: &str) -> Result<InstrumentLifecycleState> {
    match value {
        "active" => Ok(InstrumentLifecycleState::Active),
        "halted" => Ok(InstrumentLifecycleState::Halted),
        "delisted" => Ok(InstrumentLifecycleState::Delisted),
        other => anyhow::bail!("unsupported field lifecycle_state value {other:?}"),
    }
}

fn field_value(value: &Value) -> String {
    match value {
        Value::String(value) => format!("{value:?}"),
        other => other.to_string(),
    }
}
