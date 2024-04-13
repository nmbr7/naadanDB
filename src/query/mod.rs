use core::slice;
use std::fmt;

use sqlparser::ast::{Expr, Statement};

use crate::storage::NaadanError;

use self::parser::NaadanParser;

mod kernel;
pub mod parser;
pub mod plan;
pub mod query_engine;

pub type NaadanRecord = Vec<Expr>;

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
}

impl<'a> IntoIterator for &'a RecordSet {
    type Item = &'a Vec<Expr>;

    type IntoIter = slice::Iter<'a, Vec<Expr>>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.iter()
    }
}

impl IntoIterator for RecordSet {
    type Item = Vec<Expr>;

    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.into_iter()
    }
}

impl fmt::Display for RecordSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut query_result = String::new();
        self.records.iter().for_each(|r| {
            let rr: Vec<String> = r.iter().map(|val| val.to_string()).collect();
            query_result += rr.join(", ").as_str();
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
