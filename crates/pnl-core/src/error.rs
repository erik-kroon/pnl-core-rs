use crate::types::{AccountId, BookId, CurrencyId, EventId, InstrumentId};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    #[error("unknown account {0:?}")]
    UnknownAccount(AccountId),
    #[error("unknown book {book_id:?} for account {account_id:?}")]
    UnknownBook {
        account_id: AccountId,
        book_id: BookId,
    },
    #[error("unknown instrument {0:?}")]
    UnknownInstrument(InstrumentId),
    #[error("instrument {0:?} is not active")]
    InactiveInstrument(InstrumentId),
    #[error("unknown currency {0:?}")]
    UnknownCurrency(CurrencyId),
    #[error("{0}")]
    RegistrationConflict(&'static str),
    #[error("duplicate event {0:?}")]
    DuplicateEvent(EventId),
    #[error("unknown original event {0:?}")]
    UnknownOriginalEvent(EventId),
    #[error("correction target is not a fill event {0:?}")]
    CorrectionTargetNotFill(EventId),
    #[error("correction replacement must keep the original account, book, and instrument")]
    CorrectionKeyMismatch,
    #[error("out-of-order event: expected sequence {expected}, received {received}")]
    OutOfOrderEvent { expected: u64, received: u64 },
    #[error("invalid quantity")]
    InvalidQuantity,
    #[error("invalid price")]
    InvalidPrice,
    #[error("invalid scale")]
    InvalidScale,
    #[error("invalid split ratio")]
    InvalidSplitRatio,
    #[error("invalid symbol")]
    InvalidSymbol,
    #[error("arithmetic overflow")]
    ArithmeticOverflow,
    #[error("division by zero")]
    DivisionByZero,
    #[error("short positions are not allowed")]
    ShortPositionNotAllowed,
    #[error("position flips are not allowed")]
    PositionFlipNotAllowed,
    #[error("missing fx rate from {from_currency:?} to {to_currency:?}")]
    MissingFxRate {
        from_currency: CurrencyId,
        to_currency: CurrencyId,
    },
    #[error("currency mismatch: money={money_currency:?}, expected={expected_currency:?}")]
    CurrencyMismatch {
        money_currency: CurrencyId,
        expected_currency: CurrencyId,
    },
    #[error("unsupported event type {0}")]
    UnsupportedEventType(&'static str),
    #[error("snapshot validation failed: {0}")]
    SnapshotValidation(&'static str),
    #[error("snapshot version unsupported: {0}")]
    SnapshotVersionUnsupported(u16),
    #[error("snapshot hash mismatch")]
    SnapshotHashMismatch,
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("io error: {0}")]
    Io(String),
}

impl From<postcard::Error> for Error {
    fn from(err: postcard::Error) -> Self {
        Error::Serialization(err.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Serialization(err.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err.to_string())
    }
}
