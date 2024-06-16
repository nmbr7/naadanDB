use core::slice;
use std::{
    cell::{OnceCell, RefCell},
    collections::{btree_map, BTreeMap, HashMap},
    iter,
    sync::{atomic::AtomicU64, Arc, OnceLock},
    time::SystemTime,
};

use env_logger::Target;
use sqlparser::{ast::Expr, keywords::RLIKE};
use tokio::{
    sync::{Mutex, RwLock, RwLockReadGuard},
    task,
};

use crate::{
    query::{NaadanRecord, RecordSet},
    storage::{
        storage_engine::{self, NaadanStorageEngine, ScanIterator},
        CatalogEngine, NaadanError, RowIdType, ScanType, StorageEngine,
    },
    utils,
};

const BASE_TRANSACTION_ID: u64 = u64::pow(2, 63);
const BASE_TRANSACTION_TIMESTAMP: u64 = 0;

type RowChangeData = BTreeMap<String, Expr>;

#[derive(Debug)]
struct RowVersionNode {
    /// Transaction id (2^63 < TID < 2^64) or Commit Transaction timestamp (0 < CT <= 2^63)
    id: AtomicU64,
    change_data: RowChangeData,
    table_name: String,
    prev_version: Option<Arc<RwLock<Box<RowVersionNode>>>>,
}

impl RowVersionNode {
    pub fn new(
        id: AtomicU64,
        change_data: RowChangeData,
        table_name: String,
        prev_version: Option<Arc<RwLock<Box<RowVersionNode>>>>,
    ) -> Self {
        Self {
            id,
            change_data,
            prev_version,
            table_name,
        }
    }

    fn set_prev_version(&mut self, prev_version: Option<Arc<RwLock<Box<RowVersionNode>>>>) {
        self.prev_version = prev_version;
    }
}

#[derive(Debug)]
/// DB transaction manager
pub struct TransactionManager<E: StorageEngine> {
    /// Collection of active transactions in the system
    active_transactions: RwLock<BTreeMap<u64, Arc<Box<MvccTransaction<E>>>>>,

    /// Collection of recently commited transactions in the system ordered by commit timestamp
    committed_transactions: RwLock<BTreeMap<u64, Arc<Box<MvccTransaction<E>>>>>,

    /// That current transaction timestamp in-progress in the system
    /// will be initially set to [`BASE_TRANSACTION_TIMESTAMP`]
    current_timestamp: AtomicU64,

    /// That current transaction id in-progress in the system
    /// will be initially set to [`BASE_TRANSACTION_ID`]
    current_transaction_id: AtomicU64,

    /// Mapping for all the row version change chain
    row_version_map: RwLock<BTreeMap<u64, Arc<RwLock<Box<RowVersionNode>>>>>,

    /// Shared instance of the storage engine
    storage_engine: Arc<Box<E>>,
}

impl<E: StorageEngine> TransactionManager<E> {
    /// Init a new [`TransactionManager`].
    pub fn init(storage_engine: Arc<Box<E>>) -> Self {
        let transaction_manager = Self {
            active_transactions: RwLock::new(BTreeMap::new()),
            committed_transactions: RwLock::new(BTreeMap::new()),
            current_timestamp: AtomicU64::new(BASE_TRANSACTION_TIMESTAMP),
            current_transaction_id: AtomicU64::new(BASE_TRANSACTION_ID),
            row_version_map: RwLock::new(BTreeMap::new()),
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
        let transaction_id = self.get_new_transaction_id();
        let timestamp = self.get_new_timestamp();

        let transaction = Arc::new(Box::new(MvccTransaction::new(
            AtomicU64::new(transaction_id),
            AtomicU64::new(timestamp),
            transaction_manager,
        )));

        {
            let mut active_transactions = task::block_in_place(|| {
                utils::log(
                    format!("Transaction - TID: {:?}", transaction_id),
                    format!("Locking active_transactions {:?}", SystemTime::now()),
                );
                self.active_transactions.blocking_write()
            });

            active_transactions.insert(transaction_id, transaction.clone());

            utils::log(
                format!("Transaction - TID: {:?}", transaction_id),
                format!("Active Transaction count: {:?}", active_transactions.len()),
            );
            utils::log(
                format!("Transaction - TID: {:?}", transaction_id),
                format!(
                    "Starting new transaction with ID: [{:?}] and Start timestamp: [{:?}]",
                    transaction.id, transaction.start_timestamp
                ),
            );
        }

        Ok(transaction)
    }

    fn get_new_transaction_id(&self) -> u64 {
        let transaction_id = self
            .current_transaction_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        transaction_id
    }

    pub fn get_new_timestamp(&self) -> u64 {
        let timestamp = self
            .current_timestamp
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        timestamp
    }

    /// Rollback a transaction provided an transaction ID
    pub fn rollback_transaction(&self, transaction_id: u64) -> Result<(), NaadanError> {
        Ok(())
    }

    /// Commit a transaction provided an transaction ID
    pub fn commit_transaction(&self, transaction_id: u64) -> Result<(), NaadanError> {
        task::block_in_place(|| {
            let current_transaction = self.get_active_transaction(transaction_id);

            // TODO: validate transaction queries for conflict

            let current_start_time = current_transaction
                .start_timestamp
                .load(std::sync::atomic::Ordering::Relaxed);

            utils::log(
                format!("Transaction - TID: {:?}", transaction_id),
                format!(
                    "Current start time: {:?}, >>> Query: {:?} <<<",
                    current_start_time, current_transaction.queries,
                ),
            );

            let committed_transaction_map = self.committed_transactions.blocking_read();

            let mut found_conflict: bool = false;
            for (commit_timestamp, committed_transaction) in
                committed_transaction_map.range(current_start_time..)
            {
                utils::log(
                    format!("Transaction - TID: {:?}", transaction_id),
                    format!(
                        "Committed Transaction: {:?}, Commit time: {:?}, >>> Query: {:?} <<<",
                        committed_transaction.id, commit_timestamp, committed_transaction.queries,
                    ),
                );

                current_transaction
                    .change_map
                    .blocking_read()
                    .iter()
                    .for_each(|row_data| {
                        // utils::log(
                        //     format!("Transaction - TID: {:?}", transaction_id),
                        //     format!(
                        //         "RowID: [{}] Commited Change_map [{:?}]",
                        //         row_data.0,
                        //         row_data.1.blocking_read().change_data
                        //     ),
                        // );

                        if committed_transaction
                            .change_map
                            .blocking_read()
                            .contains_key(row_data.0)
                        {
                            // TODO: Compare the changed columns to find exact confict
                            //       Also use predicate matching conflict check
                            found_conflict = true;
                            return;
                        }
                    });

                if found_conflict {
                    break;
                }
            }
            drop(committed_transaction_map);

            if found_conflict {
                utils::log(
                    format!("Transaction - TID: {:?}", transaction_id),
                    format!("Transaction conflicts with a commited transaction"),
                );

                return Err(NaadanError::TransactionAborted);
            }

            let commit_timestamp = self.get_new_timestamp();

            // Move active transaction to committed transaction list
            let transaction = self
                .active_transactions
                .blocking_write()
                .remove(&transaction_id)
                .unwrap();

            // Change the id to commit timestamp for all row version change for the current transaction
            transaction.set_commit_timestamp(commit_timestamp);

            utils::log(format!("Transaction - TID: {:?}", transaction_id),
            format!("Commiting  transaction with ID: [{:?}] Start timestamp: [{:?}] Commit timestamp: [{:?}]",
            transaction.id, transaction.start_timestamp,transaction.commit_timstamp));

            // TODO: Validation and write to committed_transaction map should happen atomically
            self.committed_transactions.blocking_write().insert(
                transaction
                    .commit_timstamp
                    .load(std::sync::atomic::Ordering::Relaxed),
                transaction,
            );

            // Persist the change to the storage
            for (row_id, row_version) in current_transaction.change_map.blocking_read().iter() {
                let mut updated_columns: BTreeMap<String, Expr> = BTreeMap::new();

                let row_version_node = row_version.blocking_read();
                let table_name = row_version_node.table_name.clone();
                Self::get_final_column_updates(
                    transaction_id,
                    commit_timestamp,
                    &mut updated_columns,
                    row_version_node,
                );
                let schema = self
                    .storage_engine()
                    .get_table_details(&table_name)
                    .unwrap();

                utils::log(
                    format!("Transaction - TID: {:?}", transaction_id),
                    format!("Final Updated row {:?}", updated_columns),
                );
                self.storage_engine()
                    .update_table_rows(
                        &ScanType::RowIds(vec![row_id.clone() as u64]),
                        &updated_columns,
                        &schema,
                    )
                    .unwrap();
            }

            Ok(())
        })
    }

    fn get_final_column_updates(
        transaction_id: u64,
        commit_timestamp: u64,
        updated_columns: &mut BTreeMap<String, Expr>,
        row_version_node: RwLockReadGuard<Box<RowVersionNode>>,
    ) {
        let row_change_data_id = row_version_node
            .id
            .load(std::sync::atomic::Ordering::Acquire);

        if row_change_data_id == transaction_id || row_change_data_id == commit_timestamp {
            for column in &row_version_node.change_data {
                updated_columns.insert(column.0.clone(), column.1.clone());
                utils::log(
                    format!("Transaction - TID: {:?}", transaction_id),
                    format!("Row change data {:?}", row_version_node.change_data),
                );
            }
        }

        match &row_version_node.prev_version {
            Some(node) => {
                let node_lock = node.blocking_read();
                Self::get_final_column_updates(
                    transaction_id,
                    commit_timestamp,
                    updated_columns,
                    node_lock,
                );
            }
            None => {}
        }
    }

    fn start_background_maintanance_job(&self) {
        // TODO: start background task to clean up old transaction data
    }

    fn stop_background_maintanance_job(&self) {
        // TODO: stop background task to clean up old transaction data
    }

    pub fn get_active_transaction(&self, t_id: u64) -> Arc<Box<MvccTransaction<E>>> {
        let active_transactions = task::block_in_place(|| {
            utils::log(
                format!("Transaction - TID: {:?}", t_id),
                format!("Locking active_transactions {:?}", SystemTime::now()),
            );
            self.active_transactions.blocking_read()
        });

        active_transactions.get(&t_id).unwrap().clone()
    }

    pub fn storage_engine(&self) -> &Box<E> {
        &self.storage_engine
    }

    fn set_row_version_map(
        &self,
        row_id: u64,
        new_base_row_version_node: Arc<RwLock<Box<RowVersionNode>>>,
    ) {
        let mut row_version_map = self.row_version_map.blocking_write();

        let prev_row_node = match row_version_map.get(&row_id) {
            Some(prev_row_node) => Some(prev_row_node.clone()),
            None => None,
        };

        new_base_row_version_node
            .blocking_write()
            .set_prev_version(prev_row_node);

        row_version_map.insert(row_id, new_base_row_version_node);
    }

    fn row_version_node(&self, row_id: u64) -> Option<Arc<RwLock<Box<RowVersionNode>>>> {
        match self.row_version_map.blocking_read().get(&row_id) {
            Some(row_version_node) => Some(row_version_node.clone()),
            None => None,
        }
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
    commit_timstamp: AtomicU64,

    change_map: RwLock<HashMap<u64, Arc<RwLock<Box<RowVersionNode>>>>>,

    transaction_manager: Arc<Box<TransactionManager<E>>>,

    queries: RwLock<Vec<String>>,
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
            commit_timstamp: AtomicU64::new(0),
            change_map: RwLock::new(HashMap::new()),
            transaction_manager,
            queries: RwLock::new(vec![]),
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

    fn set_row_version_node(
        &self,
        row_id: u64,
        row_version_node: Arc<RwLock<Box<RowVersionNode>>>,
    ) {
        let mut row_version_map = self.change_map.blocking_write();

        row_version_map.insert(row_id, row_version_node);
    }

    /// Update the commit timestamp of the transaction and all the row version changes
    pub fn set_commit_timestamp(&self, commit_timstamp: u64) {
        self.commit_timstamp
            .store(commit_timstamp, std::sync::atomic::Ordering::Release);

        for (_, row_version_node) in self.change_map.blocking_read().iter() {
            row_version_node
                .blocking_write()
                .id
                .store(commit_timstamp, std::sync::atomic::Ordering::Release);
        }
    }

    pub fn add_query(&self, query: String) {
        task::block_in_place(|| {
            self.queries.blocking_write().push(query);
        });
    }

    pub fn set_start_timestamp(&self, start_timestamp: u64) {
        self.start_timestamp
            .store(start_timestamp, std::sync::atomic::Ordering::Release);
    }
}

pub struct MvccScanIterator<'a, E: StorageEngine> {
    transaction: &'a MvccTransaction<E>,
    row_iter: Box<dyn Iterator<Item = Result<NaadanRecord, NaadanError>> + 'a>,
}

impl<'a, E: StorageEngine> MvccScanIterator<'a, E> {
    pub fn new(
        transaction: &'a MvccTransaction<E>,
        row_iter: Box<dyn Iterator<Item = Result<NaadanRecord, NaadanError>> + 'a>,
    ) -> Self {
        Self {
            transaction,
            row_iter,
        }
    }

    fn reduce_record_value(
        &mut self,
        mut row_version_map_val: Arc<RwLock<Box<RowVersionNode>>>,
        transaction_id: u64,
        transaction_start_timestamp: u64,
        row_value: &mut Result<NaadanRecord, NaadanError>,
    ) {
        loop {
            let row_change_node = row_version_map_val.blocking_read();
            let row_change_data_id = row_change_node
                .id
                .load(std::sync::atomic::Ordering::Acquire);

            if row_change_data_id == transaction_id
                || row_change_data_id < transaction_start_timestamp
            {
                utils::log(
                    format!("Transaction - TID: {:?}", self.transaction.id),
                    format!("Change committed at: {:?}", row_change_data_id),
                );

                let record = row_value.as_mut().unwrap();
                for column in &row_change_node.change_data {
                    record.update_column_value(column)
                }
                break;
            } else {
                let next_node = match &row_change_node.prev_version {
                    Some(node) => node.clone(),
                    None => break,
                };

                drop(row_change_node);
                row_version_map_val = next_node;
            }
        }
    }
}

impl<'a, E: StorageEngine> Iterator for MvccScanIterator<'a, E> {
    type Item = Result<NaadanRecord, NaadanError>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.row_iter.next();
        match row {
            Some(mut row_value) => {
                // println!("{:?}", row_value);
                let row_id: u64 = row_value.as_ref().unwrap().row_id();
                let transaction_start_timestamp = self
                    .transaction
                    .start_timestamp
                    .load(std::sync::atomic::Ordering::Relaxed);

                let transaction_id = self
                    .transaction
                    .id
                    .load(std::sync::atomic::Ordering::Relaxed);

                let row_version_map_value = self
                    .transaction
                    .transaction_manager()
                    .row_version_node(row_id);

                if let Some(row_version_map_val) = row_version_map_value {
                    utils::log(
                        format!("Transaction - TID: {:?}", self.transaction.id),
                        format!("Before: {:?}", row_value),
                    );

                    self.reduce_record_value(
                        row_version_map_val,
                        transaction_id,
                        transaction_start_timestamp,
                        &mut row_value,
                    );

                    utils::log(
                        format!("Transaction - TID: {:?}", self.transaction.id),
                        format!("After: {:?}", row_value),
                    );
                }

                Some(row_value)
            }
            None => return None,
        }
    }
}

impl<E: StorageEngine> StorageEngine for MvccTransaction<E> {
    type ScanIterator<'a> = MvccScanIterator<'a, E> where E: 'a;
    fn write_table_rows(
        &self,
        row_values: crate::query::RecordSet,
        schema: &crate::storage::catalog::Table,
    ) -> Result<RowIdType, NaadanError> {
        self.storage_engine().write_table_rows(row_values, schema)
    }

    fn scan_table<'a>(
        &'a self,
        scan_type: &'a ScanType,
        schema: &'a crate::storage::catalog::Table,
    ) -> MvccScanIterator<'_, E> {
        let row_iter = self.storage_engine().scan_table(scan_type, schema);
        MvccScanIterator::new(self, Box::new(row_iter))
    }

    fn delete_table_rows(
        &self,
        row_ids: &[u64],
        schema: &crate::storage::catalog::Table,
    ) -> Result<crate::query::RecordSet, NaadanError> {
        self.storage_engine().delete_table_rows(row_ids, schema)
    }

    fn update_table_rows<'a>(
        &self,
        scan_type: &'a ScanType,
        updates_columns: &BTreeMap<String, Expr>,
        schema: &crate::storage::catalog::Table,
    ) -> Result<Vec<RowIdType>, NaadanError> {
        task::block_in_place(|| {
            let mut row_ids: Vec<RowIdType> = vec![];
            for row in self.scan_table(scan_type, schema) {
                match row {
                    Ok(record) => {
                        let row_id = record.row_id();
                        row_ids.push(row_id);

                        let transaction_id = self.id.load(std::sync::atomic::Ordering::Relaxed);
                        let new_base_row_node =
                            Arc::new(RwLock::new(Box::new(RowVersionNode::new(
                                AtomicU64::new(transaction_id),
                                updates_columns.clone(),
                                schema.name.clone(),
                                None,
                            ))));

                        // TODO: Check whether the row already exist in the transaction change map
                        //       if exists, then it means that the row is getting updated the second
                        //       time in the same transaction context.

                        // Add row change in transaction manager global row version change map
                        self.transaction_manager()
                            .set_row_version_map(row_id, new_base_row_node.clone());

                        // Add row change in current transaction row version change map
                        self.set_row_version_node(row_id, new_base_row_node);
                    }

                    Err(_) => continue,
                }
            }

            Ok(row_ids)
        })
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
