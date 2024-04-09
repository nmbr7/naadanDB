use sqlparser::ast::Statement;

use crate::storage::NaadanError;

use self::parser::NaadanParser;

mod kernel;
pub mod parser;
pub mod plan;
pub mod query_engine;

mod utils;

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
