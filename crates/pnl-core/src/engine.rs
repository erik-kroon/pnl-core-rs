use crate::account::AccountState;
use crate::account_metrics::AccountMetrics;
use crate::config::EngineConfig;
use crate::error::Result;
use crate::event::Event;
use crate::metadata::{
    AccountMeta, BookMeta, CurrencyMeta, InstrumentLifecycleState, InstrumentMeta,
};
use crate::position::{FxRate, Lot, LotId, Mark, Position, PositionKey};
use crate::registry::Registry;
use crate::replay_journal::ReplayJournal;
use crate::state_hash::{hash_engine_state, StateHash};
use crate::summary::{
    AccountReconciliation, AccountSummary, ApplyReceipt, ExplainedApplyReceipt, ReplayReport,
};
use crate::types::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) mod event_application;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Engine {
    pub(crate) config: EngineConfig,
    pub(crate) registry: Registry,
    pub(crate) accounts: BTreeMap<AccountId, AccountState>,
    pub(crate) positions: BTreeMap<PositionKey, Position>,
    pub(crate) lots: BTreeMap<(PositionKey, LotId), Lot>,
    pub(crate) marks: BTreeMap<InstrumentId, Mark>,
    pub(crate) fx_rates: BTreeMap<(CurrencyId, CurrencyId), FxRate>,
    pub(crate) replay_journal: ReplayJournal,
}

impl Engine {
    pub fn new(config: EngineConfig) -> Self {
        Self {
            config,
            registry: Registry::new(),
            accounts: BTreeMap::new(),
            positions: BTreeMap::new(),
            lots: BTreeMap::new(),
            marks: BTreeMap::new(),
            fx_rates: BTreeMap::new(),
            replay_journal: ReplayJournal::new(),
        }
    }

    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    pub fn positions(&self) -> impl Iterator<Item = &Position> {
        self.positions.values()
    }

    pub fn lots(&self) -> impl Iterator<Item = &Lot> {
        self.lots.values()
    }

    pub fn lots_for_position(&self, key: PositionKey) -> impl Iterator<Item = &Lot> {
        self.lots
            .range(
                (
                    key,
                    LotId {
                        source_event_id: EventId(0),
                        leg_index: 0,
                    },
                )
                    ..=(
                        key,
                        LotId {
                            source_event_id: EventId(u64::MAX),
                            leg_index: u8::MAX,
                        },
                    ),
            )
            .map(|(_, lot)| lot)
    }

    pub fn register_currency(&mut self, meta: CurrencyMeta) -> Result<()> {
        self.registry
            .register_currency(meta, self.config.account_money_scale)
    }

    pub fn register_account(&mut self, meta: AccountMeta) -> Result<()> {
        let inserted = self.registry.register_account(meta.clone())?;
        if !inserted {
            return Ok(());
        }
        let zero = Money::zero(meta.base_currency, self.config.account_money_scale);
        self.accounts.insert(
            meta.account_id,
            AccountState {
                account_id: meta.account_id,
                base_currency: meta.base_currency,
                initial_cash: zero,
                cash: zero,
                net_external_cash_flows: zero,
                trading_realized_pnl: zero,
                interest_pnl: zero,
                borrow_pnl: zero,
                funding_pnl: zero,
                financing_pnl: zero,
                total_financing_pnl: zero,
                realized_pnl: zero,
                peak_equity: zero,
                current_drawdown: zero,
                max_drawdown: zero,
                initial_cash_set: false,
            },
        );
        Ok(())
    }

    pub fn register_book(&mut self, meta: BookMeta) -> Result<()> {
        self.registry.register_book(meta)
    }

    pub fn register_instrument(&mut self, meta: InstrumentMeta) -> Result<()> {
        self.registry.register_instrument(meta)
    }

    pub fn instrument(&self, instrument_id: InstrumentId) -> Result<&InstrumentMeta> {
        self.registry.instrument(instrument_id)
    }

    pub fn instrument_lifecycle(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<InstrumentLifecycleState> {
        self.registry.instrument_lifecycle(instrument_id)
    }

    pub fn apply_many(&mut self, events: impl IntoIterator<Item = Event>) -> Result<ReplayReport> {
        let mut applied = 0_u64;
        let mut last_sequence = self.replay_journal.last_seq();
        for event in events {
            let receipt = self.apply(event)?;
            applied = applied
                .checked_add(1)
                .ok_or(crate::error::Error::ArithmeticOverflow)?;
            last_sequence = receipt.sequence;
        }
        Ok(ReplayReport {
            applied,
            last_sequence,
            state_hash: self.state_hash(),
        })
    }

    pub fn apply(&mut self, event: Event) -> Result<ApplyReceipt> {
        crate::replay_journal::apply_event(self, event)
    }

    pub fn apply_explained(&mut self, event: Event) -> Result<ExplainedApplyReceipt> {
        let before = self.clone();
        let receipt = self.apply(event)?;
        let mut account_ids = BTreeSet::new();
        account_ids.extend(receipt.changed_accounts.iter().copied());
        account_ids.extend(
            receipt
                .changed_positions
                .iter()
                .map(|position| position.account_id),
        );

        let mut account_changes = Vec::new();
        for account_id in account_ids {
            let before_metrics = AccountMetrics::compute(&before, account_id)?;
            let after_metrics = AccountMetrics::compute(self, account_id)?;
            account_changes.push(AccountMetrics::explain_change(
                before_metrics,
                after_metrics,
            )?);
        }

        Ok(ExplainedApplyReceipt {
            receipt,
            account_changes,
        })
    }

    pub fn position(&self, key: PositionKey) -> Option<&Position> {
        self.positions.get(&key)
    }

    pub fn account_summary(&self, account_id: AccountId) -> Result<AccountSummary> {
        Ok(AccountMetrics::compute(self, account_id)?.into_summary(self.state_hash()))
    }

    pub fn account_reconciliation(&self, account_id: AccountId) -> Result<AccountReconciliation> {
        self.registry.ensure_account(account_id)?;
        let initial_cash = self.accounts.get(&account_id).unwrap().initial_cash;
        Ok(AccountMetrics::compute(self, account_id)?
            .into_reconciliation(initial_cash, self.state_hash()))
    }

    pub fn state_hash(&self) -> StateHash {
        hash_engine_state(self)
    }
}
