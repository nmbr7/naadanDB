use core::slice;
use std::{collections::BTreeMap, fmt};

use self::parser::NaadanParser;
use crate::storage::{catalog::Column, NaadanError};
use sqlparser::ast::{Expr, Statement};

mod kernel;
pub mod parser;
pub mod plan;
pub mod query_engine;

/// Represents a row in a DB table (a collection of [`Expr`] type)
#[derive(Debug, Clone)]
pub struct NaadanRecord {
    row_id: u64,
    columns: Vec<Expr>,
    column_schema: Option<BTreeMap<String, Column>>,
}

impl NaadanRecord {
    pub fn new(row_id: u64, columns: Vec<Expr>) -> Self {
        Self {
            row_id,
            columns,
            column_schema: None,
        }
    }

    pub fn new_with_column_schema(
        row_id: u64,
        columns: Vec<Expr>,
        column_schema: BTreeMap<String, Column>,
    ) -> Self {
        Self {
            row_id,
            columns,
            column_schema: Some(column_schema),
        }
    }

    pub fn columns(&self) -> &[Expr] {
        &self.columns
    }

    pub fn row_id(&self) -> u64 {
        self.row_id
    }

    pub fn update_column_value(&mut self, column: (&String, &Expr)) {
        let column_index = self
            .column_schema
            .as_ref()
            .unwrap()
            .iter()
            .position(|val| val.0 == column.0)
            .unwrap();

        self.columns[column_index] = column.1.clone();
    }

    pub fn set_column_schema(&mut self, column_schema: Option<BTreeMap<String, Column>>) {
        self.column_schema = column_schema;
    }

    pub fn column_schema(&self) -> Option<&BTreeMap<String, Column>> {
        self.column_schema.as_ref()
    }
}

/// Represents a collection of rows.
#[derive(Debug, Clone)]
pub struct RecordSet {
    records: Vec<NaadanRecord>,
    count: usize,
}

impl RecordSet {
    pub fn new(records: Vec<NaadanRecord>) -> Self {
        let count = records.len();
        Self { records, count }
    }

    pub fn add_record(&mut self, record: NaadanRecord) {
        self.records.push(record);
        self.count += 1;
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn records(&self) -> &[NaadanRecord] {
        &self.records
    }
}

impl<'a> IntoIterator for &'a RecordSet {
    type Item = &'a NaadanRecord;

    type IntoIter = slice::Iter<'a, NaadanRecord>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.iter()
    }
}

impl IntoIterator for RecordSet {
    type Item = NaadanRecord;

    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.into_iter()
    }
}

impl fmt::Display for RecordSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut query_result = String::new();
        self.records.iter().for_each(|row| {
            let columns: Vec<String> = row.columns.iter().map(|val| val.to_string()).collect();
            query_result += columns.join(", ").as_str();
            query_result += "\n";
        });

        if self.count > 0 {
            query_result += "\n\n";
        }

        write!(f, "Total {} rows.\n{}", self.count, query_result)
    }
}

#[derive(Debug)]
pub struct NaadanQuery {
    pub query_string: String,
    _params: Vec<String>,
    pub ast: Vec<Statement>,
}

impl NaadanQuery {
    pub fn init(query: String) -> Result<Self, NaadanError> {
        match NaadanParser::parse(&query) {
            Ok(ast) => Ok(Self {
                query_string: query.clone(),
                _params: vec![],
                ast: ast,
            }),
            Err(err) => return Err(NaadanError::QueryParseFailed(err.to_string())),
        }
    }
}
