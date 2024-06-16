pub mod catalog;
mod fs;
pub mod page;
pub mod storage_engine;
mod utils;

use std::collections::{BTreeMap, HashMap};

use sqlparser::ast::Expr;
use thiserror::Error;

use crate::query::{plan::ScalarExprType, NaadanRecord, RecordSet};

use self::catalog::Table;

#[derive(Error, Debug)]
pub enum NaadanError {
    #[error("Failed to parse the query: \n - {0}")]
    QueryParseFailed(String),

    #[error("Table not found")]
    TableNotFound,
    #[error("Adding new table failed")]
    TableAddFailed,
    #[error("Table already exists")]
    TableAlreadyExists,

    #[error("Adding new rows to table failed")]
    RowAddFailed,
    #[error("Page capacity is full")]
    PageCapacityFull(u64),
    #[error("Getting rows from table failed, Row not found")]
    RowNotFound,
    #[error("Flusing page to disk failed")]
    PageFlushFailed,

    #[error("Logical plan creation failed")]
    LogicalPlanFailed,

    #[error("Query not in a valid transaction session")]
    TransactionSessionInvalid,
    #[error("Transaction aborted")]
    TransactionAborted,

    #[error("Physical plan creation failed")]
    PhysicalPlanFailed,
    #[error("Query execution failed")]
    QueryExecutionFailed,

    #[error("Provided columns does not match the table schema")]
    SchemaValidationFailed,

    #[error("File system operation failed")]
    FileSystemError(#[from] std::io::Error),

    #[error("Unknown server error")]
    Unknown,
}

pub type RowIdType = u64;
pub type TableIdType = usize;

/// Trait interface for DB schema operations.
pub trait CatalogEngine {
    /// Add table into the DB catalog
    fn add_table_details(&self, table: &mut Table) -> Result<TableIdType, NaadanError>;

    /// Get table details from the DB catalog
    fn get_table_details(&self, name: &String) -> Result<Table, NaadanError>;

    /// Delete table details from the DB catalog
    fn delete_table_details(&self, name: &String) -> Result<Table, NaadanError>;
}

pub enum ScanType {
    Filter(crate::query::plan::ScalarExprType),
    RowIds(Vec<u64>),
    Full,
}
/// Trait interface for all storage operations.
pub trait StorageEngine: CatalogEngine + std::fmt::Debug + Send {
    type ScanIterator<'a>: Iterator<Item = Result<NaadanRecord, NaadanError>>
    where
        Self: 'a;

    /// Insert new rows into a table
    fn write_table_rows(
        &self,
        row_values: RecordSet,
        schema: &Table,
    ) -> Result<RowIdType, NaadanError>;

    /// Retrieve rows from a table
    fn scan_table<'a>(
        &'a self,
        scan_types: &'a ScanType,
        schema: &'a Table,
    ) -> Self::ScanIterator<'_>;

    /// Delete rows from a table
    fn delete_table_rows(&self, row_ids: &[u64], schema: &Table) -> Result<RecordSet, NaadanError>;

    /// Update rows from a table
    fn update_table_rows<'a>(
        &self,
        scan_types: &'a ScanType,
        updates_columns: &BTreeMap<String, Expr>,
        schema: &Table,
    ) -> Result<Vec<RowIdType>, NaadanError>;
}
