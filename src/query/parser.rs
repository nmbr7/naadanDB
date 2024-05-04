use log::debug;
use sqlparser::{
    ast::Statement,
    dialect::GenericDialect,
    parser::{Parser, ParserError},
};

use crate::utils;

pub struct NaadanParser;
impl NaadanParser {
    pub fn parse(query: &str) -> Result<Vec<Statement>, ParserError> {
        utils::log(
            "Parser".to_string(),
            format!(
                "============== Parsing query {:?} Started ==============",
                query,
            ),
        );

        let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
        let ast = Parser::parse_sql(&dialect, query)?;

        utils::log("Parser".to_string(), format!("Parsed AST is {:?}\n", ast));
        utils::log(
            "Parser".to_string(),
            format!(
                "============== Parsing query {:?} Finished ==============",
                query
            ),
        );
        Ok(ast)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser() {
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
