use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use crate::plan::{
    Edge, Plan, PlanExpr, PlanGroup, Relational, RelationalExprType, Scalar, ScalarExprType,
};
use crate::storage_engine::StorageEngine;

use log::debug;
use sqlparser::ast::{SetExpr, Statement, TableFactor};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use tokio::sync::Mutex;

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

pub struct NaadanQueryEngine {
    pub storage_engine: Arc<Mutex<Box<dyn StorageEngine + Send>>>,
}

impl NaadanQueryEngine {
    pub fn init(storage_engine: Arc<Mutex<Box<dyn StorageEngine + Send>>>) -> Self {
        Self {
            storage_engine: storage_engine,
        }
    }

    pub fn plan<'a>(&self, query: &'a NaadanQuery) -> Result<Vec<Plan<'a>>, bool> {
        let mut final_plan_list: Vec<Plan> = vec![];
        // Iterate through the AST and create best logical plan which potentially has the least cost of execution

        // TODO: Read from the DB catalog to get info about the the table (table name id, row count, columns etc..)
        // based on the info from the catalog info, validate the query.
        for statement in query.ast.iter() {
            let mut plan: Plan = Plan {
                estimated_row_count: 0,
                estimated_time: Duration::from_millis(0),
                plan_expr_root: None,
            };

            match statement {
                Statement::Commit { .. }
                | Statement::Insert { .. }
                | Statement::CreateDatabase { .. }
                | Statement::AlterIndex { .. }
                | Statement::CreateIndex { .. } => {
                    debug!("DB and table config statements, not doing much planning for now.")
                    // TODO: for tables which has indexes will need to add index updates in the plan
                }
                Statement::Query(query_data) => {
                    match &*query_data.body {
                        SetExpr::Select(select_query) => {
                            //  Get the query target Tables as in the 'FROM' expression
                            let expr_group = Self::process_table(select_query).unwrap();
                            debug!("{:#?}", expr_group);
                            let first_vec = expr_group.first().unwrap();
                            let first_expr = Rc::clone(first_vec.borrow().exprs.first().unwrap());
                            plan.plan_expr_root = Some(first_expr);
                            //debug!("{:?}", expr_group);
                        }
                        // TODO
                        // sqlparser::ast::SetExpr::Query(_) => todo!(),
                        // sqlparser::ast::SetExpr::SetOperation {
                        //     op,
                        //     set_quantifier,
                        //     left,
                        //     right,
                        // } => todo!(),
                        // sqlparser::ast::SetExpr::Values(_) => todo!(),
                        // sqlparser::ast::SetExpr::Insert(_) => todo!(),
                        // sqlparser::ast::SetExpr::Update(_) => todo!(),
                        // sqlparser::ast::SetExpr::Table(_) => todo!(),
                        _ => debug!("Provided Query is {:?} ", &*query_data.body),
                    }
                }
                _ => {
                    debug!(
                        "Provided statement {:?} doesn't have plans for now",
                        statement
                    )
                }
            }

            final_plan_list.push(plan);
        }

        // TODO iterate the AST and do nomalization and pre-processing and create the Plan structure for all expressions
        // explore different combination of plan structure and emit a final plan, which will be sent for physical plan preparation.

        Ok(final_plan_list)
    }

    fn init_plan_expr(value: PlanExpr<'_>) -> Result<Edge<PlanGroup>, bool> {
        let expr_grp = Rc::new(RefCell::new(PlanGroup {
            exprs: vec![],
            best_expr: None,
        }));

        let rel_expr = Rc::new(RefCell::new(value));

        // Link the expression with group
        expr_grp.borrow_mut().exprs.push(Rc::clone(&rel_expr));

        match &mut *rel_expr.borrow_mut() {
            PlanExpr::Relational(relation) => {
                let r = Rc::clone(&expr_grp);
                relation.group = Some(Rc::downgrade(&r));
            }
            //PlanExpr::Scalar { rel_type, group } => todo!(),
            _ => {}
        };

        Ok(expr_grp)
    }

    fn process_table(
        select_query: &Box<sqlparser::ast::Select>,
    ) -> Result<Vec<Edge<PlanGroup>>, bool> {
        let mut plan_group_list: Vec<Edge<PlanGroup>> = vec![];
        for table in select_query.from.iter() {
            match &table.relation {
                TableFactor::Table { name, .. } => {
                    debug!(" Select query on Table {}", name.0.first().unwrap().value);
                    let value = PlanExpr::Relational(Relational {
                        rel_type: RelationalExprType::ScanExpr { table_id: 1 },
                        group: None,
                    });

                    let expr_group = Self::init_plan_expr(value).unwrap();
                    plan_group_list.push(expr_group);
                    // TODO: Get the table id from the catalog table
                    // self.storage_engine.lock()
                }
                // TODO
                // sqlparser::ast::TableFactor::Derived { lateral, subquery, alias } => todo!(),
                // sqlparser::ast::TableFactor::TableFunction { expr, alias } => todo!(),
                // sqlparser::ast::TableFactor::Function { lateral, name, args, alias } => todo!(),
                // sqlparser::ast::TableFactor::UNNEST { alias, array_exprs, with_offset, with_offset_alias } => todo!(),
                // sqlparser::ast::TableFactor::JsonTable { json_expr, json_path, columns, alias } => todo!(),
                // sqlparser::ast::TableFactor::NestedJoin { table_with_joins, alias } => todo!(),
                // sqlparser::ast::TableFactor::Pivot { table, aggregate_function, value_column, pivot_values, alias } => todo!(),
                // sqlparser::ast::TableFactor::Unpivot { table, value, name, columns, alias } => todo!(),
                _ => debug!(
                    "Provided 'Select FROM' refers {:?} relation",
                    &table.relation
                ),
            }
        }
        for projection in select_query.projection.iter() {
            match projection {
                sqlparser::ast::SelectItem::UnnamedExpr(expr) => match expr {
                    sqlparser::ast::Expr::Identifier(identifier) => {
                        debug!("{:?}", identifier.value);

                        let value = PlanExpr::Scalar(Scalar {
                            rel_type: ScalarExprType::Identifier {
                                value: identifier.value.clone(),
                            },
                            group: None,
                        });

                        let expr_group = Self::init_plan_expr(value).unwrap();
                        plan_group_list.push(expr_group);
                    }
                    _ => {}
                },
                // sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => todo!(),
                // sqlparser::ast::SelectItem::QualifiedWildcard(_, _) => todo!(),
                // sqlparser::ast::SelectItem::Wildcard(_) => todo!(),
                _ => {}
            }
        }

        debug!("{:?}", plan_group_list);
        Ok(plan_group_list)
    }

    pub fn execute(&self) -> Result<bool, bool> {
        Ok(true)
    }
}

#[derive(Debug)]
pub struct NaadanQuery {
    pub query_string: String,
    _params: Vec<String>,
    pub ast: Vec<Statement>,
}

impl NaadanQuery {
    pub fn init(query: String) -> Result<Self, ParserError> {
        match NaadanParser::parse(&query) {
            Ok(ast) => Ok(Self {
                query_string: query.clone(),
                _params: vec![],
                ast: ast,
            }),
            Err(err) => return Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
