pub mod catalog;
pub mod page;
pub mod storage_engine;

mod utils;

use sqlparser::ast::Values;
use thiserror::Error;

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
    #[error("Physical plan creation failed")]
    PhysicalPlanFailed,
    #[error("Query execution failed")]
    QueryExecutionFailed,

    #[error("Unknown server error")]
    Unknown,
}

pub type RowIdType = usize;
pub type TableIdType = usize;

pub trait StorageEngine: std::fmt::Debug {
    /// Add table into the DB catalog
    fn add_table_details(&mut self, table: &mut Table) -> Result<TableIdType, NaadanError>;

    /// Get table details from the DB catalog
    fn get_table_details(&self, name: &String) -> Result<Table, NaadanError>;

    /// Insert new rows into a table
    fn write_table_rows(
        &mut self,
        row_values: Values,
        table: &Table,
    ) -> Result<RowIdType, NaadanError>;

    /// Retrieve rows from a table
    fn read_table_rows(&self, row_id: &[usize], schema: &Table) -> Result<Values, NaadanError>;
}
