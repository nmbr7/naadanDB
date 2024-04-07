pub mod catalog;
pub mod page;
pub mod storage_engine;

mod utils;

use sqlparser::ast::Values;
use thiserror::Error;

use self::catalog::Table;

#[derive(Error, Debug)]
pub enum NaadanError {
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
    fn get_table_details(&self, name: &String) -> Result<Table, NaadanError>;

    /// Add table into the DB catalog
    fn add_table(&mut self, table: &mut Table) -> Result<TableIdType, NaadanError>;

    /// Insert new row into a table
    fn add_row_into_table(
        &mut self,
        row_values: Values,
        table: &Table,
    ) -> Result<RowIdType, NaadanError>;

    //fn read_row(&mut self, row_id: usize) -> Result<&Values, NaadanError>;

    fn read_rows(&mut self, row_id: &[usize], schema: &Table) -> Result<Values, NaadanError>;
    // fn write_row(&mut self, row_id: &usize, row_data: RowData) -> Result<usize, bool>;

    // fn write_rows(&mut self, row_data: Vec<Vec<u8>>) -> Result<Vec<RowData>, bool>;

    // fn reset_memory(&mut self);
}
