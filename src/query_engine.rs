use crate::utils::log;

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::ast::Statement;

pub struct NaadanParser;
impl NaadanParser {
    pub fn parse(query: &str) -> Result<Vec<Statement>, ParserError>
    {
        log(format!("\n =======\nParsing query {:?}\n =======", query));
        
        let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
        let ast = Parser::parse_sql(&dialect, query).unwrap();

        log(format!("Parsed AST is {:?}", ast));

        Ok(ast)
    }
}

#[derive(Debug)]
pub struct NaadanQuery {
    query_string: String,
    params: Vec<String>,
    ast: Vec<Statement>,
}

impl NaadanQuery {
    pub fn new(query: String) -> Self {
        Self {
            query_string: query,
            params: vec![],
            ast: vec![],
        }
    }

    pub fn add_param(&mut self, param1: String) -> &mut Self {
        self.params.push(param1);
        self
    }

    pub fn execute(&mut self) -> Result<bool, bool> {
        // Parse
        self.ast = NaadanParser::parse(&self.query_string).unwrap();

        // Plan

        // Optimize

        // Execute

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn Test_Query() {
        let mut query = NaadanQuery::new("select * from table1".to_string());

        query.add_param("name".to_string());
        query.add_param("query2".to_string());
        let query_status = query.execute();

        let log_string = format!("Query status {:?}", query_status);
        log(log_string);
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

