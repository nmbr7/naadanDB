use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc},
};

use crate::storage::NaadanError;

const BASE_TRANSACTION_ID: u64 = 0;
const BASE_TRANSACTION_TIMESTAMP: u64 = 2 ^ 64;

#[derive(Debug)]
/// DB transaction manager
pub struct TransactionManager {
    /// Collection of active transactions in the system
    // TODO: use an ordered data structure
    active_transactions: HashMap<u64, Arc<Transaction>>,

    /// Collection of recently commited transactions in the system
    // TODO: use an ordered data structure
    commited_transactions: HashMap<u64, Arc<Transaction>>,

    /// That current transaction timestamp in-progress in the system
    /// will be initially set to [`BASE_TRANSACTION_TIMESTAMP`]
    current_timestamp: AtomicU64,

    /// That current transaction id in-progress in the system
    /// will be initially set to [`BASE_TRANSACTION_ID`]
    current_transaction_id: AtomicU64,
}

impl TransactionManager {
    /// Init a new [`TransactionManager`].
    pub fn init() -> Self {
        let transaction_manager = Self {
            active_transactions: HashMap::new(),
            commited_transactions: HashMap::new(),
            current_timestamp: AtomicU64::new(BASE_TRANSACTION_TIMESTAMP),
            current_transaction_id: AtomicU64::new(BASE_TRANSACTION_ID),
        };

        transaction_manager.start_background_maintanance_job();

        transaction_manager
    }

    /// Start a new DB [`Transaction`].
    pub fn start_new_transaction(&mut self) -> Result<Arc<Transaction>, NaadanError> {
        let transaction_id = self
            .current_transaction_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;

        let timestamp = self
            .current_timestamp
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;

        let transaction = Arc::new(Transaction::new(
            AtomicU64::new(transaction_id),
            AtomicU64::new(timestamp),
        ));

        self.active_transactions
            .insert(transaction_id, transaction.clone());

        Ok(transaction)
    }

    /// Rollback a transaction provided an transaction ID
    pub fn rollback_transaction(&mut self, transaction_id: u64) -> Result<(), NaadanError> {
        Ok(())
    }

    /// Commit a transaction provided an transaction ID
    pub fn commit_transaction(&mut self, transaction_id: u64) -> Result<(), NaadanError> {
        Ok(())
    }

    fn start_background_maintanance_job(&self) {
        // TODO: start background task to clean up old transaction data
    }

    fn stop_background_maintanance_job(&self) {
        // TODO: stop background task to clean up old transaction data
    }
}

#[derive(Debug)]
/// A single database transaction
pub struct Transaction {
    /// Transaction id (2^63 < TID < 2^64)
    id: AtomicU64,
    /// Transaction start timestamp (0 < ST <= 2^63)
    start_timestamp: AtomicU64,
    /// Transaction commit timestamp (0 < CT <= 2^63)
    commit_timstamp: Option<AtomicU64>,
}

impl Transaction {
    /// Creates a new [`Transaction`].
    fn new(id: AtomicU64, start_timestamp: AtomicU64) -> Self {
        Self {
            id,
            start_timestamp,
            commit_timstamp: None,
        }
    }

    pub fn id(&self) -> u64 {
        self.id.as_ptr() as u64
    }
}
