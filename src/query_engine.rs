use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::catalog::{Column, ColumnType};
use crate::plan::{
    CreateTableExpr, Edge, Plan, PlanExpr, PlanGroup, Relational, RelationalExprType, Scalar,
    ScalarExprType, ScanExpr, Stats,
};
use crate::storage_engine::{Page, StorageEngine};

use log::debug;
use sqlparser::ast::{DataType, SetExpr, Statement, TableFactor};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use tokio::sync::Mutex;
use tokio::task;

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

pub struct ExecContext {
    last_result: Option<Vec<u8>>,
    last_op_name: Option<String>,
    last_op_time: Option<Duration>,
}

impl ExecContext {
    pub fn init() -> Self {
        Self {
            last_result: None,
            last_op_name: None,
            last_op_time: None,
        }
    }
}

fn create_table(
    context: &mut ExecContext,
    table_expr: Option<RelationalExprType>,
    scalar_val: Option<ScalarExprType>,
) {
    debug!("creating table");
}

fn scan_table(
    context: &mut ExecContext,
    table_expr: Option<RelationalExprType>,
    scalar_val: Option<ScalarExprType>,
) {
    debug!("scaning table");
}

type PlanExecFn = fn(context: &mut ExecContext, Option<RelationalExprType>, Option<ScalarExprType>);

pub struct NaadanQueryEngine {
    pub storage_engine: Arc<Mutex<Box<dyn StorageEngine + Send>>>,
}

impl NaadanQueryEngine {
    pub async fn init(storage_engine: Arc<Mutex<Box<dyn StorageEngine + Send>>>) -> Self {
        Self {
            storage_engine: storage_engine,
        }
    }

    pub async fn prepare_exec_plan<'a>(
        &'a self,
        logical_plan: &Vec<Plan<'a>>,
    ) -> Result<Vec<PlanExecFn>, bool> {
        let mut exec_vec: Vec<PlanExecFn> = Vec::new();

        match logical_plan.last() {
            Some(plan) => {
                let mut root_exp = (plan.plan_expr_root.as_ref().unwrap().borrow());
                //   let ty = exp.borrow();
                match &*root_exp {
                    PlanExpr::Relational(val) => match &val.rel_type {
                        RelationalExprType::ScanExpr(val) => {
                            exec_vec.push(scan_table);
                        }
                        RelationalExprType::CreateTableExpr(val) => {
                            exec_vec.push(create_table);
                        }
                        RelationalExprType::InsertExpr(_) => todo!(),
                        RelationalExprType::InnerJoinExpr(_, _, _) => todo!(),
                        RelationalExprType::FilterExpr(_) => todo!(),
                        RelationalExprType::IndexScanExpr { index_id } => todo!(),
                    },

                    _ => {}
                }
            }
            None => todo!(),
        }

        debug!("Physical_plan prepared.");
        Ok(exec_vec)
    }

    pub async fn prepare_logical_plan<'a>(
        &'a self,
        query: &'a NaadanQuery,
    ) -> Result<Vec<Plan<'a>>, bool> {
        let mut final_plan_list: Vec<Plan<'a>> = vec![];
        // Iterate through the AST and create best logical plan which potentially has the least cost of execution

        // TODO: Read from the DB catalog to get info about the the table (table name id, row count, columns etc..)
        // based on the info from the catalog info, validate the query.
        for statement in query.ast.iter() {
            let mut plan: Plan = Plan {
                plan_stats: Some(Stats {
                    estimated_row_count: 0,
                    estimated_time: Duration::from_millis(0),
                }),
                plan_expr_root: None,
            };

            match statement {
                Statement::CreateTable {
                    or_replace,
                    temporary,
                    external,
                    global,
                    if_not_exists,
                    transient,
                    name,
                    columns,
                    constraints,
                    hive_distribution,
                    hive_formats,
                    table_properties,
                    with_options,
                    file_format,
                    location,
                    query,
                    without_rowid,
                    like,
                    clone,
                    engine,
                    comment,
                    auto_increment_offset,
                    default_charset,
                    collation,
                    on_commit,
                    on_cluster,
                    order_by,
                    partition_by,
                    cluster_by,
                    options,
                    strict,
                } => {
                    let mut column_map: HashMap<String, Column> = HashMap::new();
                    for col in columns {
                        let mut is_nullable = false;
                        for opts in &col.options {
                            match &opts.option {
                                sqlparser::ast::ColumnOption::Null => {
                                    is_nullable = true;
                                }
                                sqlparser::ast::ColumnOption::NotNull => todo!(),
                                sqlparser::ast::ColumnOption::Default(_) => todo!(),
                                sqlparser::ast::ColumnOption::Unique {
                                    is_primary,
                                    characteristics,
                                } => todo!(),
                                sqlparser::ast::ColumnOption::ForeignKey {
                                    foreign_table,
                                    referred_columns,
                                    on_delete,
                                    on_update,
                                    characteristics,
                                } => todo!(),
                                sqlparser::ast::ColumnOption::Check(_) => todo!(),
                                sqlparser::ast::ColumnOption::DialectSpecific(_) => todo!(),
                                sqlparser::ast::ColumnOption::CharacterSet(_) => todo!(),
                                sqlparser::ast::ColumnOption::Comment(_) => todo!(),
                                sqlparser::ast::ColumnOption::OnUpdate(_) => todo!(),
                                sqlparser::ast::ColumnOption::Generated {
                                    generated_as,
                                    sequence_options,
                                    generation_expr,
                                    generation_expr_mode,
                                    generated_keyword,
                                } => todo!(),
                                sqlparser::ast::ColumnOption::Options(_) => todo!(),
                            }
                        }

                        column_map.insert(
                            col.name.value.clone(),
                            Column {
                                column_type: ColumnType::from(&col.data_type),
                                is_nullable,
                            },
                        );
                    }

                    // Check if a table exist with the same name.

                    // Create plan
                    let local_plan = PlanExpr::Relational(Relational {
                        rel_type: RelationalExprType::CreateTableExpr(
                            crate::plan::CreateTableExpr {
                                table_name: name.0.last().unwrap().value.clone(),
                                columns: column_map,
                            },
                        ),
                        group: None,
                        stats: None,
                    });

                    plan.plan_expr_root = Some(Rc::new(RefCell::new(local_plan)));
                }
                Statement::AlterTable { .. } => {}

                Statement::Insert {
                    or,
                    ignore,
                    into,
                    table_name,
                    table_alias,
                    columns,
                    overwrite,
                    source,
                    partitioned,
                    after_columns,
                    table,
                    on,
                    returning,
                    replace_into,
                    priority,
                } => {
                    debug!(
                        "Insert statement, table name is {} with columns {:?} and values {:?}",
                        table_name, columns, source
                    )
                }
                Statement::Update { .. } => {}
                Statement::Delete { .. } => {}

                Statement::StartTransaction { .. } => {}
                Statement::Rollback { .. } => {}
                Statement::Commit { .. } => {}

                Statement::CreateDatabase { .. } => {}
                Statement::Drop { .. } => {}

                Statement::CreateIndex { .. } => {}
                Statement::AlterIndex { .. } => {}

                Statement::Query(query_data) => {
                    match &*query_data.body {
                        SetExpr::Select(select_query) => {
                            //  Get the query target Tables as in the 'FROM' expression
                            let result = self.process_select_query(select_query);
                            let expr_group = match result {
                                Ok(val) => val,
                                Err(err) => {
                                    debug!("Query failed with error: {}", err);
                                    return Err(false);
                                }
                            };
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

    fn process_select_query(
        self: &Self,
        select_query: &Box<sqlparser::ast::Select>,
    ) -> Result<Vec<Edge<PlanGroup>>, bool> {
        let mut plan_group_list: Vec<Edge<PlanGroup>> = vec![];
        for table in select_query.from.iter() {
            match &table.relation {
                TableFactor::Table { name, .. } => {
                    let table_name = &name.0.first().unwrap().value;
                    let table_id: u16;
                    let table_schema: HashMap<String, Column>;
                    {
                        let storage_instance = task::block_in_place(move || {
                            println!("Locking storage_instance {:?}", SystemTime::now());
                            self.storage_engine.blocking_lock()
                            // do some compute-heavy work or call synchronous code
                        });

                        match storage_instance.get_table_details(table_name) {
                            Ok(table_catalog) => {
                                table_id = table_catalog.id;
                                table_schema = table_catalog.schema.clone();
                            }
                            Err(_) => {
                                debug!(" Invalid Table {}", table_name);
                                return Err(false);
                            }
                        }
                    }

                    debug!(" Select query on Table {}", table_name);
                    let value = PlanExpr::Relational(Relational {
                        rel_type: RelationalExprType::ScanExpr(ScanExpr::new(
                            table_id,
                            table_schema,
                        )),
                        group: None,
                        stats: Some(Stats {
                            estimated_row_count: 0,
                            estimated_time: Duration::from_millis(0),
                        }),
                    });

                    let expr_group = Self::init_plan_expr(value).unwrap();
                    plan_group_list.push(expr_group);
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
                            stats: Some(Stats {
                                estimated_row_count: 0,
                                estimated_time: Duration::from_millis(0),
                            }),
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

    pub fn execute<'a>(
        &self,
        logical_plan: Vec<Plan<'a>>,
        physical_plan: Vec<PlanExecFn>,
    ) -> Result<Vec<u8>, bool> {
        debug!("Executing final query plan.");

        let mut exec_context = ExecContext::init();
        for plan in physical_plan {
            plan(&mut exec_context, None, None);
        }

        Ok(String::from("Plan executed success in 2ms").into())
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
