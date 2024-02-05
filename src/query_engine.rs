use std::sync::Arc;

use crate::storage_engine::{self, StorageEngine};
use crate::utils::log;

use log::debug;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use tokio::sync::Mutex;

pub struct NaadanParser;
impl NaadanParser {
    pub fn parse(query: &str) -> Result<Vec<Statement>, ParserError> {
        debug!("=======\nParsing query {:?}\n =======", query);

        let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
        let ast = Parser::parse_sql(&dialect, query).unwrap();

        debug!("Parsed AST is {:?}", ast);

        Ok(ast)
    }
}

pub struct NaadanQueryEngine {
    pub storage_engine: Arc<Mutex<Box<dyn StorageEngine + Send>>>,
}

impl NaadanQueryEngine {
    pub fn init(storage_engine: Arc<Mutex<Box<dyn StorageEngine + Send>>>) -> Self {
        Self {
            storage_engine: storage_engine,
        }
    }

    pub fn plan(&self, query: & NaadanQuery) -> Result<bool, bool> {
        // Plan

        Ok(true)
    }

    pub fn execute(&self) -> Result<bool, bool> {
        Ok(true)
    }
}

#[derive(Debug)]
pub struct NaadanQuery {
    query_string: String,
    params: Vec<String>,
    ast: Vec<Statement>,
}

impl NaadanQuery {
    pub fn init(query: String) -> Self {
        Self {
            query_string: query.clone(),
            params: vec![],
            ast: NaadanParser::parse(&query).unwrap(),
        }
    }

    pub fn add_param(&mut self, param1: String) -> &mut Self {
        self.params.push(param1);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn Test_Query() {
        let mut query = NaadanQuery::init("select * from table1".to_string());

        query.add_param("name".to_string());
        query.add_param("query2".to_string());
    }

    #[test]
    fn Test_Parser() {
        let sql = "SELECT a, b, 123, myfunc(b) \
                   FROM table_1 \
                   WHERE a > b AND b < 100 \
                   ORDER BY a DESC, b";

        let ast = NaadanParser::parse(sql);

        let sql = "SELECT distinct 2 as w, a, b \
                   FROM table_2 \
                   WHERE a > 100 \
                   ORDER BY b";

        let ast = NaadanParser::parse(sql);
    }
}
