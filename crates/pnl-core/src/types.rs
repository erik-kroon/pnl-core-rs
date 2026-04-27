use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fmt;

pub const ACCOUNT_MONEY_SCALE: u8 = 4;
pub const ACCOUNT_RATIO_SCALE: u8 = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AccountId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BookId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct InstrumentId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EventId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CurrencyId(pub u32);

impl CurrencyId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn usd() -> Self {
        Self::from_code_const(*b"USD")
    }

    pub const fn from_code_const(code: [u8; 3]) -> Self {
        Self(((code[0] as u32) << 16) | ((code[1] as u32) << 8) | code[2] as u32)
    }

    pub fn from_code(code: &str) -> Result<Self> {
        let bytes = code.as_bytes();
        if bytes.len() != 3 || !bytes.iter().all(u8::is_ascii_uppercase) {
            return Err(Error::InvalidScale);
        }
        Ok(Self::from_code_const([bytes[0], bytes[1], bytes[2]]))
    }

    pub fn code(self) -> String {
        let a = ((self.0 >> 16) & 0xff) as u8;
        let b = ((self.0 >> 8) & 0xff) as u8;
        let c = (self.0 & 0xff) as u8;
        String::from_utf8_lossy(&[a, b, c]).to_string()
    }
}

impl fmt::Display for CurrencyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.code())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoundingMode {
    HalfEven,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccountingMethod {
    AverageCost,
    Fifo,
    Lifo,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn sign(self) -> i128 {
        match self {
            Side::Buy => 1,
            Side::Sell => -1,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FixedI128 {
    pub value: i128,
    pub scale: u8,
}

impl FixedI128 {
    pub const fn new(value: i128, scale: u8) -> Self {
        Self { value, scale }
    }

    pub const fn zero(scale: u8) -> Self {
        Self { value: 0, scale }
    }

    pub const fn one() -> Self {
        Self { value: 1, scale: 0 }
    }

    pub fn parse_decimal(input: &str) -> Result<Self> {
        parse_decimal(input)
    }

    pub fn to_scale(self, target_scale: u8, rounding: RoundingMode) -> Result<Self> {
        Ok(Self {
            value: rescale_i128(self.value, self.scale, target_scale, rounding)?,
            scale: target_scale,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Price {
    pub value: i128,
    pub scale: u8,
}

impl Price {
    pub const fn new(value: i128, scale: u8) -> Self {
        Self { value, scale }
    }

    pub fn parse_decimal(input: &str) -> Result<Self> {
        let fixed = parse_decimal(input)?;
        if fixed.value < 0 {
            return Err(Error::InvalidPrice);
        }
        Ok(Self {
            value: fixed.value,
            scale: fixed.scale,
        })
    }

    pub fn to_scale(self, target_scale: u8, rounding: RoundingMode) -> Result<Self> {
        Ok(Self {
            value: rescale_i128(self.value, self.scale, target_scale, rounding)?,
            scale: target_scale,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Qty {
    pub value: i128,
    pub scale: u8,
}

impl Qty {
    pub const fn new(value: i128, scale: u8) -> Self {
        Self { value, scale }
    }

    pub const fn zero(scale: u8) -> Self {
        Self { value: 0, scale }
    }

    pub fn from_units(value: i128) -> Self {
        Self { value, scale: 0 }
    }

    pub fn parse_decimal(input: &str) -> Result<Self> {
        let fixed = parse_decimal(input)?;
        if fixed.value <= 0 {
            return Err(Error::InvalidQuantity);
        }
        Ok(Self {
            value: fixed.value,
            scale: fixed.scale,
        })
    }

    pub fn to_scale_exact(self, target_scale: u8) -> Result<Self> {
        Ok(Self {
            value: rescale_exact(self.value, self.scale, target_scale)?,
            scale: target_scale,
        })
    }

    pub fn abs_value(self) -> i128 {
        self.value.abs()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Money {
    pub amount: i128,
    pub scale: u8,
    pub currency_id: CurrencyId,
}

impl Money {
    pub const fn new(amount: i128, scale: u8, currency_id: CurrencyId) -> Self {
        Self {
            amount,
            scale,
            currency_id,
        }
    }

    pub const fn zero(currency_id: CurrencyId, scale: u8) -> Self {
        Self {
            amount: 0,
            scale,
            currency_id,
        }
    }

    pub fn parse_decimal(input: &str, currency_id: CurrencyId, scale: u8) -> Result<Self> {
        let fixed = parse_decimal(input)?.to_scale(scale, RoundingMode::HalfEven)?;
        Ok(Self {
            amount: fixed.value,
            scale,
            currency_id,
        })
    }

    pub fn checked_add(self, other: Money) -> Result<Money> {
        self.ensure_compatible(other)?;
        Ok(Self::new(
            self.amount
                .checked_add(other.amount)
                .ok_or(Error::ArithmeticOverflow)?,
            self.scale,
            self.currency_id,
        ))
    }

    pub fn checked_sub(self, other: Money) -> Result<Money> {
        self.ensure_compatible(other)?;
        Ok(Self::new(
            self.amount
                .checked_sub(other.amount)
                .ok_or(Error::ArithmeticOverflow)?,
            self.scale,
            self.currency_id,
        ))
    }

    pub fn checked_neg(self) -> Result<Money> {
        Ok(Self::new(
            self.amount.checked_neg().ok_or(Error::ArithmeticOverflow)?,
            self.scale,
            self.currency_id,
        ))
    }

    pub fn abs(self) -> Money {
        Self::new(self.amount.abs(), self.scale, self.currency_id)
    }

    pub fn ensure_compatible(self, other: Money) -> Result<()> {
        if self.scale != other.scale || self.currency_id != other.currency_id {
            return Err(Error::InvalidScale);
        }
        Ok(())
    }
}

impl fmt::Display for Money {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_scaled(self.amount, self.scale, f)?;
        write!(f, " {}", self.currency_id)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ratio {
    pub value: i128,
    pub scale: u8,
}

impl Ratio {
    pub const fn zero(scale: u8) -> Self {
        Self { value: 0, scale }
    }

    pub fn from_fraction(
        numer: i128,
        denom: i128,
        scale: u8,
        rounding: RoundingMode,
    ) -> Result<Self> {
        if denom == 0 {
            return Err(Error::DivisionByZero);
        }
        let factor = checked_pow10(scale)?;
        let scaled = numer.checked_mul(factor).ok_or(Error::ArithmeticOverflow)?;
        Ok(Self {
            value: div_round(scaled, denom, rounding)?,
            scale,
        })
    }
}

impl fmt::Display for Ratio {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_scaled(self.value, self.scale, f)
    }
}

pub fn checked_pow10(exp: u8) -> Result<i128> {
    let mut value = 1_i128;
    for _ in 0..exp {
        value = value.checked_mul(10).ok_or(Error::ArithmeticOverflow)?;
    }
    Ok(value)
}

pub fn rescale_exact(value: i128, from_scale: u8, to_scale: u8) -> Result<i128> {
    if from_scale == to_scale {
        return Ok(value);
    }
    if from_scale < to_scale {
        let factor = checked_pow10(to_scale - from_scale)?;
        value.checked_mul(factor).ok_or(Error::ArithmeticOverflow)
    } else {
        let factor = checked_pow10(from_scale - to_scale)?;
        if value % factor != 0 {
            return Err(Error::InvalidScale);
        }
        Ok(value / factor)
    }
}

pub fn rescale_i128(
    value: i128,
    from_scale: u8,
    to_scale: u8,
    rounding: RoundingMode,
) -> Result<i128> {
    if from_scale == to_scale {
        return Ok(value);
    }
    if from_scale < to_scale {
        let factor = checked_pow10(to_scale - from_scale)?;
        return value.checked_mul(factor).ok_or(Error::ArithmeticOverflow);
    }
    let factor = checked_pow10(from_scale - to_scale)?;
    div_round(value, factor, rounding)
}

pub fn div_round(numer: i128, denom: i128, rounding: RoundingMode) -> Result<i128> {
    if denom == 0 {
        return Err(Error::DivisionByZero);
    }
    match rounding {
        RoundingMode::HalfEven => div_round_half_even(numer, denom),
    }
}

fn div_round_half_even(numer: i128, denom: i128) -> Result<i128> {
    let sign = if (numer < 0) ^ (denom < 0) { -1 } else { 1 };
    let n = numer.checked_abs().ok_or(Error::ArithmeticOverflow)?;
    let d = denom.checked_abs().ok_or(Error::ArithmeticOverflow)?;
    let q = n / d;
    let r = n % d;
    let twice = r.checked_mul(2).ok_or(Error::ArithmeticOverflow)?;
    let rounded_abs = if twice > d || (twice == d && q % 2 != 0) {
        q.checked_add(1).ok_or(Error::ArithmeticOverflow)?
    } else {
        q
    };
    rounded_abs
        .checked_mul(sign)
        .ok_or(Error::ArithmeticOverflow)
}

pub fn money_from_components(
    value: i128,
    scale: u8,
    currency_id: CurrencyId,
    target_scale: u8,
    rounding: RoundingMode,
) -> Result<Money> {
    Ok(Money::new(
        rescale_i128(value, scale, target_scale, rounding)?,
        target_scale,
        currency_id,
    ))
}

pub fn convert_money_with_rate(
    money: Money,
    to_currency_id: CurrencyId,
    rate: Price,
    target_scale: u8,
    rounding: RoundingMode,
) -> Result<Money> {
    if money.currency_id == to_currency_id {
        return money_from_components(
            money.amount,
            money.scale,
            to_currency_id,
            target_scale,
            rounding,
        );
    }
    let converted = money
        .amount
        .checked_mul(rate.value)
        .ok_or(Error::ArithmeticOverflow)?;
    let scale = money
        .scale
        .checked_add(rate.scale)
        .ok_or(Error::ArithmeticOverflow)?;
    money_from_components(converted, scale, to_currency_id, target_scale, rounding)
}

pub fn value_qty_price_multiplier(
    qty_value: i128,
    qty_scale: u8,
    price: Price,
    multiplier: FixedI128,
    currency_id: CurrencyId,
    target_scale: u8,
    rounding: RoundingMode,
) -> Result<Money> {
    let qp = qty_value
        .checked_mul(price.value)
        .ok_or(Error::ArithmeticOverflow)?;
    let qpm = qp
        .checked_mul(multiplier.value)
        .ok_or(Error::ArithmeticOverflow)?;
    let scale = qty_scale
        .checked_add(price.scale)
        .and_then(|s| s.checked_add(multiplier.scale))
        .ok_or(Error::ArithmeticOverflow)?;
    money_from_components(qpm, scale, currency_id, target_scale, rounding)
}

fn parse_decimal(input: &str) -> Result<FixedI128> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(Error::InvalidScale);
    }
    let negative = trimmed.starts_with('-');
    let unsigned = trimmed.strip_prefix('-').unwrap_or(trimmed);
    let mut split = unsigned.split('.');
    let whole = split.next().ok_or(Error::InvalidScale)?;
    let frac = split.next().unwrap_or("");
    if split.next().is_some()
        || whole.is_empty()
        || !whole.chars().all(|c| c.is_ascii_digit())
        || !frac.chars().all(|c| c.is_ascii_digit())
    {
        return Err(Error::InvalidScale);
    }
    let scale = u8::try_from(frac.len()).map_err(|_| Error::InvalidScale)?;
    let digits = format!("{whole}{frac}");
    let mut value = digits.parse::<i128>().map_err(|_| Error::InvalidScale)?;
    if negative {
        value = value.checked_neg().ok_or(Error::ArithmeticOverflow)?;
    }
    Ok(FixedI128 { value, scale })
}

fn write_scaled(value: i128, scale: u8, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    if scale == 0 {
        return write!(f, "{value}");
    }
    let negative = value < 0;
    let abs = value.abs();
    let factor = 10_i128.pow(scale as u32);
    let whole = abs / factor;
    let frac = abs % factor;
    if negative {
        write!(f, "-")?;
    }
    write!(f, "{whole}.{:0width$}", frac, width = scale as usize)
}
