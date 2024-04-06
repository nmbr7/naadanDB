pub mod catalog;
pub mod page;
pub mod storage_engine;
mod utils;

use sqlparser::ast::Values;
use thiserror::Error;

use self::{catalog::Table, page::RowData};

#[derive(Error, Debug)]
pub enum StorageEngineError {
    #[error("Table not found")]
    TableNotFound,
    #[error("Adding new table failed")]
    TableAddFailed,
    #[error("Table already exists")]
    TableAlreadyExists,
    #[error("Adding new row to table failed")]
    RowAddFailed,
    #[error("Flusing page to disk failed")]
    PageFlushFailed,
    #[error("Unknown storage engine error")]
    Unknown,
}

pub type RowIdType = usize;
pub type TableIdType = usize;

pub trait StorageEngine: std::fmt::Debug {
    /// Get table details from the DB catalog
    /// Will include table id and columns details
    fn get_table_details(&self, name: &String) -> Result<Table, StorageEngineError>;

    /// Add table into the DB catalog
    fn add_table(&mut self, table: &mut Table) -> Result<TableIdType, StorageEngineError>;

    /// Insert new row into a table
    fn add_row_into_table(
        &mut self,
        row_values: Values,
        table: &Table,
    ) -> Result<RowIdType, StorageEngineError>;

    fn read_row(&mut self, row_id: usize) -> Result<&RowData, bool>;

    fn read_rows(&mut self, row_id: &[usize]) -> Result<Vec<&RowData>, bool>;

    fn write_row(&mut self, row_id: &usize, row_data: RowData) -> Result<usize, bool>;

    fn write_rows(&mut self, row_data: Vec<Vec<u8>>) -> Result<Vec<RowData>, bool>;

    fn reset_memory(&mut self);
}
