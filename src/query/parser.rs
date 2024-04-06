use log::debug;
use sqlparser::{
    ast::Statement,
    dialect::GenericDialect,
    parser::{Parser, ParserError},
};

pub struct NaadanParser;
impl NaadanParser {
    pub fn parse(query: &str) -> Result<Vec<Statement>, ParserError> {
        debug!(
            "\n ==============\nParsing query {:?} Started\n ==============",
            query
        );

        let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
        let ast = Parser::parse_sql(&dialect, query)?;

        debug!("Parsed AST is {:?}\n", ast);
        debug!(
            "\n ==============\nParsing query {:?} Finished\n ==============",
            query
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
