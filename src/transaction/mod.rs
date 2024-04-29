use core::slice;
use std::{
    cell::{OnceCell, RefCell},
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc, OnceLock},
    time::SystemTime,
};

use once_cell::sync::Lazy;
use sqlparser::ast::Expr;
use tokio::{
    sync::{Mutex, RwLock},
    task,
};

use crate::{
    query::{NaadanRecord, RecordSet},
    storage::{
        storage_engine::{self, NaadanStorageEngine, ScanIterator},
        CatalogEngine, NaadanError, RowIdType, StorageEngine,
    },
};

const BASE_TRANSACTION_ID: u64 = 0;
const BASE_TRANSACTION_TIMESTAMP: u64 = 2 ^ 64;

type RowChangeData = HashMap<String, Expr>;

#[derive(Debug)]
struct RowVersionNode {
    /// Transaction id (2^63 < TID < 2^64) or Transaction timestamp (0 < CT <= 2^63)
    id: AtomicU64,
    change_data: RowChangeData,
    table_name: String,
    prev_version: Option<Arc<RwLock<RowVersionNode>>>,
}

impl RowVersionNode {
    pub fn new(
        id: AtomicU64,
        change_data: RowChangeData,
        table_name: String,
        prev_version: Option<Arc<RwLock<RowVersionNode>>>,
    ) -> Self {
        Self {
            id,
            change_data,
            prev_version,
            table_name,
        }
    }
}

#[derive(Debug)]
/// DB transaction manager
pub struct TransactionManager<E: StorageEngine> {
    /// Collection of active transactions in the system
    // TODO: use an ordered data structure
    active_transactions: Mutex<HashMap<u64, Arc<Box<MvccTransaction<E>>>>>,

    /// Collection of recently commited transactions in the system
    // TODO: use an ordered data structure
    commited_transactions: HashMap<u64, Arc<Box<MvccTransaction<E>>>>,

    /// That current transaction timestamp in-progress in the system
    /// will be initially set to [`BASE_TRANSACTION_TIMESTAMP`]
    current_timestamp: AtomicU64,

    /// That current transaction id in-progress in the system
    /// will be initially set to [`BASE_TRANSACTION_ID`]
    current_transaction_id: AtomicU64,

    /// Mapping for all the row version change chain
    row_version_map: HashMap<u64, Arc<RwLock<Box<RowVersionNode>>>>,

    storage_engine: Arc<Box<E>>,
}

impl<E: StorageEngine> TransactionManager<E> {
    /// Init a new [`TransactionManager`].
    pub fn init(storage_engine: Arc<Box<E>>) -> Self {
        let transaction_manager = Self {
            active_transactions: Mutex::new(HashMap::new()),
            commited_transactions: HashMap::new(),
            current_timestamp: AtomicU64::new(BASE_TRANSACTION_TIMESTAMP),
            current_transaction_id: AtomicU64::new(BASE_TRANSACTION_ID),
            row_version_map: HashMap::new(),
            storage_engine,
        };

        transaction_manager.start_background_maintanance_job();

        transaction_manager
    }

    /// Start a new DB [`Transaction`].
    pub fn start_new_transaction(
        &self,
        transaction_manager: Arc<Box<TransactionManager<E>>>,
    ) -> Result<Arc<Box<MvccTransaction<E>>>, NaadanError> {
        let transaction_id = self
            .current_transaction_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;

        let timestamp = self
            .current_timestamp
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;

        let transaction = Arc::new(Box::new(MvccTransaction::new(
            AtomicU64::new(transaction_id),
            AtomicU64::new(timestamp),
            transaction_manager,
        )));

        {
            let mut active_transactions = task::block_in_place(|| {
                println!("Locking storage_instance {:?}", SystemTime::now());
                self.active_transactions.blocking_lock()
                // do some compute-heavy work or call synchronous code
            });

            active_transactions.insert(transaction_id, transaction.clone());

            println!("Active Trans: {:?}", active_transactions);
        }

        Ok(transaction)
    }

    /// Rollback a transaction provided an transaction ID
    pub fn rollback_transaction(&self, transaction_id: u64) -> Result<(), NaadanError> {
        Ok(())
    }

    /// Commit a transaction provided an transaction ID
    pub fn commit_transaction(&self, transaction_id: u64) -> Result<(), NaadanError> {
        for (row_id, row_version) in &self.get_active_transaction(transaction_id).change_map {
            let row_version_data = &row_version.blocking_read();
            let table_name = &row_version_data.table_name;

            let updates_columns = &row_version_data.change_data;
            let schema = self
                .storage_engine()
                .get_table_details(&table_name)
                .unwrap();
            self.storage_engine()
                .update_table_rows(Some(vec![row_id.clone() as u64]), updates_columns, &schema)
                .unwrap();
        }

        // TODO: validate transaction queries for conflict

        // TODO: change the id to commit timestamp for all row version change for the current transaction

        // TOOD: move active transaction to committed transaction list

        Ok(())
    }

    fn start_background_maintanance_job(&self) {
        // TODO: start background task to clean up old transaction data
    }

    fn stop_background_maintanance_job(&self) {
        // TODO: stop background task to clean up old transaction data
    }

    pub fn get_active_transaction(&self, t_id: u64) -> Arc<Box<MvccTransaction<E>>> {
        let mut active_transactions = task::block_in_place(|| {
            println!("Locking storage_instance {:?}", SystemTime::now());
            self.active_transactions.blocking_lock()
            // do some compute-heavy work or call synchronous code
        });

        active_transactions.get(&t_id).unwrap().clone()
    }

    pub fn storage_engine(&self) -> &Box<E> {
        &self.storage_engine
    }
}

#[derive(Debug)]
/// A single database transaction
pub struct MvccTransaction<E: StorageEngine> {
    /// Transaction id (2^63 < TID < 2^64)
    id: AtomicU64,
    /// Transaction start timestamp (0 < ST <= 2^63)
    start_timestamp: AtomicU64,
    /// Transaction commit timestamp (0 < CT <= 2^63)
    commit_timstamp: Option<AtomicU64>,

    change_map: HashMap<u64, Arc<RwLock<RowVersionNode>>>,

    transaction_manager: Arc<Box<TransactionManager<E>>>,
}

unsafe impl<E: StorageEngine> Send for MvccTransaction<E> {}

impl<E: StorageEngine> MvccTransaction<E> {
    /// Creates a new [`Transaction`].
    fn new(
        id: AtomicU64,
        start_timestamp: AtomicU64,
        transaction_manager: Arc<Box<TransactionManager<E>>>,
    ) -> Self {
        Self {
            id,
            start_timestamp,
            commit_timstamp: None,
            change_map: HashMap::new(),
            transaction_manager,
        }
    }

    pub fn id(&self) -> u64 {
        self.id.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn storage_engine(&self) -> &Box<E> {
        self.transaction_manager().storage_engine()
    }

    pub fn transaction_manager(&self) -> &Box<TransactionManager<E>> {
        &self.transaction_manager
    }
}

pub struct MvccScanIterator<'a> {
    row_iter: Box<dyn Iterator<Item = Result<NaadanRecord, NaadanError>> + 'a>,
}

impl<'a> MvccScanIterator<'a> {
    pub fn new(row_iter: Box<dyn Iterator<Item = Result<NaadanRecord, NaadanError>> + 'a>) -> Self {
        Self { row_iter }
    }
}

impl<'a> Iterator for MvccScanIterator<'a> {
    type Item = Result<NaadanRecord, NaadanError>;

    fn next(&mut self) -> Option<Self::Item> {
        let a = self.row_iter.next();
        a
    }
}
impl<E: StorageEngine> StorageEngine for MvccTransaction<E> {
    type ScanIterator<'a> = MvccScanIterator<'a> where E: 'a;
    fn write_table_rows(
        &self,
        row_values: crate::query::RecordSet,
        schema: &crate::storage::catalog::Table,
    ) -> Result<RowIdType, NaadanError> {
        self.storage_engine().write_table_rows(row_values, schema)
    }

    fn read_table_rows<'a>(
        &'a self,
        row_ids: &'a [u64],
        schema: &'a crate::storage::catalog::Table,
    ) -> MvccScanIterator<'_> {
        let row_iter = self.storage_engine().read_table_rows(row_ids, schema);
        MvccScanIterator::new(Box::new(row_iter))
    }

    fn scan_table<'a>(
        &'a self,
        predicate: Option<crate::query::plan::ScalarExprType>,
        schema: &'a crate::storage::catalog::Table,
    ) -> MvccScanIterator<'_> {
        let row_iter = self.storage_engine().scan_table(predicate, schema);
        MvccScanIterator::new(Box::new(row_iter))
    }

    fn delete_table_rows(
        &self,
        row_ids: &[u64],
        schema: &crate::storage::catalog::Table,
    ) -> Result<crate::query::RecordSet, NaadanError> {
        self.storage_engine().delete_table_rows(row_ids, schema)
    }

    fn update_table_rows(
        &self,
        row_ids: Option<Vec<u64>>,
        updates_columns: &HashMap<String, Expr>,
        schema: &crate::storage::catalog::Table,
    ) -> Result<&[RowIdType], NaadanError> {
        self.storage_engine()
            .update_table_rows(row_ids, updates_columns, schema)
    }
}

impl<E: StorageEngine> CatalogEngine for MvccTransaction<E> {
    fn add_table_details(
        &self,
        table: &mut crate::storage::catalog::Table,
    ) -> Result<crate::storage::TableIdType, NaadanError> {
        self.storage_engine().add_table_details(table)
    }

    fn get_table_details(
        &self,
        name: &String,
    ) -> Result<crate::storage::catalog::Table, NaadanError> {
        self.storage_engine().get_table_details(name)
    }

    fn delete_table_details(
        &self,
        name: &String,
    ) -> Result<crate::storage::catalog::Table, NaadanError> {
        self.storage_engine().delete_table_details(name)
    }
}
