use anyhow::{Context, Result};
use pnl_core::*;
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub(super) struct CliConfig {
    pub(super) base_currency: String,
    account_money_scale: Option<u8>,
    pub(super) accounting_method: Option<String>,
    pub(super) fx_allow_inverse: Option<bool>,
    pub(super) fx_cross_rate_pivots: Option<Vec<String>>,
    allow_short: Option<bool>,
    allow_position_flip: Option<bool>,
    expected_start_seq: Option<u64>,
    currencies: Option<Vec<CliCurrency>>,
    accounts: Option<Vec<CliAccount>>,
    books: Option<Vec<CliBook>>,
}

#[derive(Debug, Deserialize)]
struct CliCurrency {
    code: String,
    scale: Option<u8>,
}

#[derive(Debug, Deserialize)]
struct CliAccount {
    account_id: u64,
    base_currency: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CliBook {
    account_id: u64,
    book_id: u64,
}

pub(super) fn load_config(path: &Path) -> Result<CliConfig> {
    let config_text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    toml::from_str(&config_text).with_context(|| format!("parsing {}", path.display()))
}

pub(super) fn build_engine(config: CliConfig, base_currency: CurrencyId) -> Result<Engine> {
    let mut engine = Engine::new(EngineConfig {
        base_currency,
        account_money_scale: config.account_money_scale.unwrap_or(ACCOUNT_MONEY_SCALE),
        accounting_method: parse_accounting_method(config.accounting_method.as_deref())?,
        fx_routing: parse_fx_routing(&config)?,
        allow_short: config.allow_short.unwrap_or(true),
        allow_position_flip: config.allow_position_flip.unwrap_or(true),
        expected_start_seq: config.expected_start_seq.unwrap_or(1),
        ..EngineConfig::default()
    });

    engine.register_currency(CurrencyMeta {
        currency_id: base_currency,
        code: config.base_currency.clone(),
        scale: engine.config().account_money_scale,
    })?;

    for currency in config.currencies.unwrap_or_default() {
        engine.register_currency(CurrencyMeta {
            currency_id: CurrencyId::from_code(&currency.code)?,
            code: currency.code,
            scale: currency
                .scale
                .unwrap_or(engine.config().account_money_scale),
        })?;
    }

    let accounts = config.accounts.unwrap_or_else(|| {
        vec![CliAccount {
            account_id: 1,
            base_currency: None,
        }]
    });
    for account in accounts {
        let account_currency = match account.base_currency {
            Some(code) => {
                let currency_id = CurrencyId::from_code(&code)?;
                engine.register_currency(CurrencyMeta {
                    currency_id,
                    code,
                    scale: engine.config().account_money_scale,
                })?;
                currency_id
            }
            None => base_currency,
        };
        engine.register_account(AccountMeta {
            account_id: AccountId(account.account_id),
            base_currency: account_currency,
        })?;
    }

    let books = config.books.unwrap_or_else(|| {
        vec![CliBook {
            account_id: 1,
            book_id: 1,
        }]
    });
    for book in books {
        engine.register_book(BookMeta {
            account_id: AccountId(book.account_id),
            book_id: BookId(book.book_id),
        })?;
    }

    Ok(engine)
}

pub(super) fn parse_currency_field(field: &str, value: &str) -> Result<CurrencyId> {
    CurrencyId::from_code(value).with_context(|| format!("invalid field {field} value {value:?}"))
}

pub(super) fn parse_accounting_method(value: Option<&str>) -> Result<AccountingMethod> {
    match value.unwrap_or("average_cost") {
        "average_cost" => Ok(AccountingMethod::AverageCost),
        "fifo" => Ok(AccountingMethod::Fifo),
        "lifo" => Ok(AccountingMethod::Lifo),
        other => anyhow::bail!("unsupported field accounting_method value {other:?}"),
    }
}

fn parse_fx_routing(config: &CliConfig) -> Result<FxRoutingConfig> {
    let mut cross_rate_pivots = Vec::new();
    for value in config.fx_cross_rate_pivots.as_deref().unwrap_or(&[]) {
        cross_rate_pivots.push(parse_currency_field("fx_cross_rate_pivots", value)?);
    }
    Ok(FxRoutingConfig {
        allow_inverse: config.fx_allow_inverse.unwrap_or(false),
        cross_rate_pivots,
    })
}

#[cfg(test)]
pub(super) fn default_config() -> CliConfig {
    CliConfig {
        base_currency: "USD".to_string(),
        account_money_scale: None,
        accounting_method: None,
        fx_allow_inverse: None,
        fx_cross_rate_pivots: None,
        allow_short: None,
        allow_position_flip: None,
        expected_start_seq: None,
        currencies: None,
        accounts: None,
        books: None,
    }
}
