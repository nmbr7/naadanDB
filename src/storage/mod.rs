pub mod catalog;
mod fs;
pub mod page;
pub mod storage_engine;
mod utils;

use std::collections::HashMap;

use sqlparser::ast::Expr;
use thiserror::Error;

use crate::query::{plan::ScalarExprType, RecordSet};

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
    #[error("Flusing page to disk failed")]
    PageFlushFailed,

    #[error("Logical plan creation failed")]
    LogicalPlanFailed,

    #[error("Query not in a valid transaction session")]
    TransactionSessionInvalid,

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

pub type RowIdType = usize;
pub type TableIdType = usize;

/// Trait interface for DB schema operations.
pub trait CatalogEngine {
    /// Add table into the DB catalog
    fn add_table_details(&mut self, table: &mut Table) -> Result<TableIdType, NaadanError>;

    /// Get table details from the DB catalog
    fn get_table_details(&self, name: &String) -> Result<Table, NaadanError>;

    /// Delete table details from the DB catalog
    fn delete_table_details(&self, name: &String) -> Result<Table, NaadanError>;
}

/// Trait interface for all storage operations.
pub trait StorageEngine: CatalogEngine + std::fmt::Debug + Send {
    /// Insert new rows into a table
    fn write_table_rows(
        &mut self,
        row_values: RecordSet,
        schema: &Table,
    ) -> Result<RowIdType, NaadanError>;

    /// Retrieve rows from a table
    fn read_table_rows(&self, row_ids: &[usize], schema: &Table) -> Result<RecordSet, NaadanError>;

    /// Retrieve rows from a table
    fn scan_table(
        &self,
        predicate: Option<ScalarExprType>,
        schema: &Table,
    ) -> Result<RecordSet, NaadanError>;

    /// Delete rows from a table
    fn delete_table_rows(
        &self,
        row_ids: &[usize],
        schema: &Table,
    ) -> Result<RecordSet, NaadanError>;

    /// Update rows from a table
    fn update_table_rows(
        &mut self,
        row_ids: Option<Vec<usize>>,
        updates_columns: HashMap<String, Expr>,
        schema: &Table,
    ) -> Result<&[RowIdType], NaadanError>;
}
