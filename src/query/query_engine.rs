use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use crate::query::kernel::{create_table, insert_table, scan_table};
use crate::query::plan::*;
use crate::storage::catalog::{Column, ColumnType, Offset, Table};
use crate::storage::{NaadanError, StorageEngine};

use log::{debug, error};
use sqlparser::ast::{SetExpr, Statement, TableFactor, Values};

use tokio::sync::Mutex;
use tokio::task;

use super::{NaadanQuery, RecordSet};

/// Execution statistics for a query
#[derive(Debug, Clone)]
pub struct ExecStats {
    begin_time: SystemTime,
    exec_time: Duration,
}

impl ExecStats {
    pub fn new(begin_time: SystemTime, exec_time: Duration) -> Self {
        Self {
            begin_time,
            exec_time,
        }
    }
}

/// Final query result for a sql query execution
#[derive(Debug, Clone)]
pub struct QueryResult {
    query_str: Option<String>,
    exec_stats: ExecStats,
    result: Option<RecordSet>,
}

impl QueryResult {
    pub fn new(exec_stats: ExecStats, result: Option<RecordSet>, query: Option<String>) -> Self {
        Self {
            exec_stats,
            result,
            query_str: query,
        }
    }
}

impl fmt::Display for QueryResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let query = match self.query_str.as_ref() {
            Some(val) => format!("## Query: {}\n", val),
            None => "".to_string(),
        };

        match self.result.as_ref() {
            Some(val) => write!(
                f,
                "{}{}Query execution succeeded in {:.3}ms",
                query,
                val.to_string(),
                self.exec_stats.exec_time.as_micros() as f64 / 1000.0,
            ),
            None => write!(
                f,
                "{}Query execution succeeded in {:.3}ms",
                query,
                self.exec_stats.exec_time.as_micros() as f64 / 1000.0,
            ),
        }
    }
}

/// Execution context for the physical plan executor
pub struct ExecContext {
    last_result: Option<RecordSet>,
    last_op_name: Option<String>,
    last_op_time: Option<Duration>,
    last_op_status: Option<bool>,

    pub storage_engine: Option<Arc<Mutex<Box<dyn StorageEngine>>>>,
}

impl ExecContext {
    pub fn init() -> Self {
        Self {
            last_result: None,
            last_op_name: None,
            last_op_time: None,
            storage_engine: None,
            last_op_status: None,
        }
    }

    pub fn last_result(&self) -> Option<&RecordSet> {
        self.last_result.as_ref()
    }

    pub fn set_storage_engine(&mut self, storage_engine: Arc<Mutex<Box<dyn StorageEngine>>>) {
        self.storage_engine = Some(storage_engine);
    }
    pub fn get_storage_engine(&mut self) -> Option<&Arc<Mutex<Box<dyn StorageEngine>>>> {
        self.storage_engine.as_ref()
    }

    pub fn set_last_result(&mut self, last_result: Option<RecordSet>) {
        self.last_result = last_result;
    }

    pub fn set_last_op_status(&mut self, last_op_status: Option<bool>) {
        self.last_op_status = last_op_status;
    }
}

/// The core query engine struct
/// - It takes a reference to the global reference counted storage engine instance
pub struct NaadanQueryEngine {
    pub storage_engine: Arc<Mutex<Box<dyn StorageEngine>>>,
}

impl NaadanQueryEngine {
    /// Initialize the query engine with the shared storage engine instance
    pub async fn init(storage_engine: Arc<Mutex<Box<dyn StorageEngine>>>) -> Self {
        Self {
            storage_engine: storage_engine,
        }
    }

    /// Process the sql query AST
    pub async fn process_query(&self, query: NaadanQuery) -> Vec<Result<QueryResult, NaadanError>> {
        let mut result: Vec<Result<QueryResult, NaadanError>> = Vec::new();

        for statement in query.ast.iter() {
            if let Some(value) = self.process_query_statement(statement).await {
                result.push(value);
            }
        }

        result
    }

    async fn process_query_statement(
        &self,
        statement: &Statement,
    ) -> Option<Result<QueryResult, NaadanError>> {
        let query_str = statement.to_string();
        println!("Processing query: {}", query_str);

        // Create logical plan for the query from the AST
        let logical_plan = match self.prepare_logical_plan(statement).await {
            Ok(val) => val,
            Err(err) => return Some(Err(err)),
        };
        println!("Logical Plan is : {:?}", logical_plan);

        // Prepare the physical plan
        let physical_plan = match self.prepare_physical_plan(&logical_plan).await {
            Ok(val) => val,
            Err(err) => return Some(Err(err)),
        };
        println!("Physical Plan is : {:?}", physical_plan);

        // Execute the query using the physical plan
        Some(self.execute(physical_plan).await)
    }

    /// Execute the physical query plan.
    pub async fn execute<'a>(
        &self,
        physical_plan: Vec<PhysicalPlan<'a>>,
    ) -> Result<QueryResult, NaadanError> {
        println!("Executing final query plan.");

        // TODO let the scheduler decide how and where the execution will take place.
        let mut exec_context = ExecContext::init();
        exec_context.set_storage_engine(self.storage_engine.clone());

        let begin_time = SystemTime::now();
        let now = Instant::now();

        for plan in physical_plan {
            (plan.plane_exec_fn)(&mut exec_context, plan.plan_expr);
        }

        let elapsed_time = now.elapsed();

        if exec_context.last_op_status.unwrap_or(true) {
            Ok(QueryResult::new(
                ExecStats::new(begin_time, elapsed_time),
                exec_context.last_result,
                None,
            ))
        } else {
            Err(NaadanError::QueryExecutionFailed)
        }
    }

    async fn prepare_physical_plan<'a>(
        &'a self,
        logical_plan: &Vec<Plan<'a>>,
    ) -> Result<Vec<PhysicalPlan>, NaadanError> {
        let mut exec_vec: Vec<PhysicalPlan> = Vec::new();

        match logical_plan.last() {
            Some(plan) => {
                let root_exp = plan.plan_expr_root.as_ref().unwrap().borrow();
                match &*root_exp {
                    PlanExpr::Relational(val) => match &val.rel_type {
                        RelationalExprType::ScanExpr(val) => {
                            let physical_plan_expr: PhysicalPlanExpr<'a> =
                                PhysicalPlanExpr::Relational(RelationalExprType::ScanExpr(
                                    val.clone(),
                                ));
                            let physical_plan: PhysicalPlan<'a> =
                                PhysicalPlan::new(physical_plan_expr, scan_table);

                            exec_vec.push(physical_plan);
                        }
                        RelationalExprType::CreateTableExpr(val) => {
                            let physical_plan_expr: PhysicalPlanExpr<'a> =
                                PhysicalPlanExpr::Relational(RelationalExprType::CreateTableExpr(
                                    val.clone(),
                                ));
                            let physical_plan: PhysicalPlan<'a> =
                                PhysicalPlan::new(physical_plan_expr, create_table);

                            exec_vec.push(physical_plan);
                        }
                        RelationalExprType::InsertExpr(val) => {
                            let physical_plan_expr: PhysicalPlanExpr<'a> =
                                PhysicalPlanExpr::Relational(RelationalExprType::InsertExpr(
                                    val.clone(),
                                ));
                            let physical_plan: PhysicalPlan<'a> =
                                PhysicalPlan::new(physical_plan_expr, insert_table);

                            exec_vec.push(physical_plan);
                        }
                        RelationalExprType::InnerJoinExpr(_, _, _) => todo!(),
                        RelationalExprType::FilterExpr(_) => todo!(),
                        RelationalExprType::IndexScanExpr { index_id: _ } => todo!(),
                    },

                    _ => {}
                }
            }
            None => todo!(),
        }

        println!("Physical_plan prepared.");
        Ok(exec_vec)
    }

    async fn prepare_logical_plan<'a>(
        &'a self,
        statement: &Statement,
    ) -> Result<Vec<Plan<'a>>, NaadanError> {
        let mut final_plan_list: Vec<Plan<'a>> = vec![];
        // Iterate through the AST and create best logical plan which potentially has the least f execution

        // TODO: Read from the DB catalog to get info about the the table (table name id, row count, columns etc..)
        //       based on the info from the catalog info, validate the query.

        let mut plan: Plan = Plan {
            plan_stats: Some(Stats {
                estimated_row_count: 0,
                estimated_time: Duration::from_millis(0),
            }),
            plan_expr_root: None,
        };

        match statement {
            Statement::CreateTable { name, columns, .. } => {
                let mut column_map: HashMap<String, Column> = HashMap::new();
                let table_name = name.0.last().unwrap().value.clone();
                {
                    let storage_instance = self.storage_engine.lock().await;

                    match storage_instance.get_table_details(&table_name) {
                        Ok(_) => {
                            println!("Table '{}' already exists", &table_name);
                            return Err(NaadanError::TableAlreadyExists);
                        }
                        Err(_) => {}
                    }
                }

                println!("Columns {:?}", columns);
                let mut offset = 0;
                for col in columns {
                    let mut is_nullable = false;

                    for opts in &col.options {
                        match &opts.option {
                            sqlparser::ast::ColumnOption::Null => {
                                is_nullable = true;
                            }
                            sqlparser::ast::ColumnOption::NotNull => todo!(),
                            sqlparser::ast::ColumnOption::Default(_) => todo!(),
                            sqlparser::ast::ColumnOption::Unique { .. } => todo!(),
                            sqlparser::ast::ColumnOption::ForeignKey { .. } => todo!(),
                            sqlparser::ast::ColumnOption::Check(_) => todo!(),
                            sqlparser::ast::ColumnOption::DialectSpecific(_) => todo!(),
                            sqlparser::ast::ColumnOption::CharacterSet(_) => todo!(),
                            sqlparser::ast::ColumnOption::Comment(_) => todo!(),
                            sqlparser::ast::ColumnOption::OnUpdate(_) => todo!(),
                            sqlparser::ast::ColumnOption::Generated { .. } => todo!(),
                            sqlparser::ast::ColumnOption::Options(_) => todo!(),
                        }
                    }

                    println!("Column offset is {:?}", offset);

                    column_map.insert(
                        col.name.value.clone(),
                        Column {
                            column_type: ColumnType::from(&col.data_type),
                            offset,
                            is_nullable,
                        },
                    );

                    offset += Offset::from(&col.data_type).get_value();
                }

                // TODO: Check if a table exist with the same name.

                // Create plan
                let local_plan = PlanExpr::Relational(Relational {
                    rel_type: RelationalExprType::CreateTableExpr(CreateTableExpr {
                        table_name: table_name,
                        columns: column_map,
                    }),
                    group: None,
                    stats: None,
                });

                plan.plan_expr_root = Some(Rc::new(RefCell::new(local_plan)));
            }
            Statement::AlterTable { .. } => {}

            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => {
                println!(
                    "Insert statement, table name is {} with columns {:?} and values {:?}",
                    table_name, columns, source
                );
                let t_name = table_name.0.last().unwrap().value.clone();

                {
                    let storage_instance = task::block_in_place(move || {
                        println!("Locking storage_instance {:?}", SystemTime::now());
                        self.storage_engine.blocking_lock()
                        // do some compute-heavy work or call synchronous code
                    });

                    match storage_instance.get_table_details(&t_name) {
                        Ok(table_catalog) => {
                            // TODO: validate the input columns againt the schema
                            let row_values = match &*(source.as_ref().unwrap().body) {
                                SetExpr::Values(val) => RecordSet::new(val.rows.clone()),
                                _ => return Err(NaadanError::LogicalPlanFailed),
                            };

                            // Create plan
                            let local_plan = PlanExpr::Relational(Relational {
                                rel_type: RelationalExprType::InsertExpr(InsertExpr {
                                    table_name: t_name,
                                    rows: row_values,
                                }),
                                group: None,
                                stats: None,
                            });

                            plan.plan_expr_root = Some(Rc::new(RefCell::new(local_plan)));
                        }
                        Err(err) => {
                            error!("{}", err.to_string());
                            return Err(err);
                        }
                    }
                }
            }
            Statement::Update {
                table,
                assignments,
                from,
                selection,
                returning,
            } => {}
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
                        let result = self.prepare_select_query_plan(select_query);
                        let expr_group = match result {
                            Ok(val) => val,
                            Err(err) => {
                                println!("Query failed with error: {}", err);
                                return Err(err);
                            }
                        };
                        //println!("{:?}", expr_group);
                        let first_vec = expr_group.first().unwrap();
                        let first_expr = Rc::clone(first_vec.borrow().exprs.first().unwrap());
                        plan.plan_expr_root = Some(first_expr);
                        //println!("{:?}", expr_group);
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
                    _ => println!("Provided Query is {:?} ", &*query_data.body),
                }
            }
            _ => {
                println!(
                    "Provided statement {:?} doesn't have plans for now",
                    statement
                )
            }
        }

        final_plan_list.push(plan);
        // TODO iterate the AST and do nomalization and pre-processing and create the Plan structure for all expressions
        // explore different combination of plan structure and emit a final plan, which will be sent for physical plan preparation.

        Ok(final_plan_list)
    }

    fn prepare_select_query_plan(
        self: &Self,
        select_query: &Box<sqlparser::ast::Select>,
    ) -> Result<Vec<Edge<PlanGroup>>, NaadanError> {
        let mut plan_group_list: Vec<Edge<PlanGroup>> = vec![];
        for table in select_query.from.iter() {
            match &table.relation {
                TableFactor::Table { name, .. } => {
                    let table_name = &name.0.first().unwrap().value;
                    let table_schema: Table;
                    {
                        let storage_instance = task::block_in_place(move || {
                            println!("Locking storage_instance {:?}", SystemTime::now());
                            self.storage_engine.blocking_lock()
                            // do some compute-heavy work or call synchronous code
                        });

                        match storage_instance.get_table_details(table_name) {
                            Ok(table_catalog) => {
                                table_schema = table_catalog.clone();
                            }
                            Err(err) => {
                                return Err(err);
                            }
                        }
                    }

                    println!(" Select query on Table {}", table_name);
                    let value = PlanExpr::Relational(Relational {
                        rel_type: RelationalExprType::ScanExpr(ScanExpr::new(
                            table_schema,
                            None,
                            ScanType::WildCardScan,
                        )),
                        group: None,
                        stats: Some(Stats {
                            estimated_row_count: 0,
                            estimated_time: Duration::from_millis(0),
                        }),
                    });

                    let expr_group = value.set_expr_group().unwrap();
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
                _ => println!(
                    "Provided 'Select FROM' refers {:?} relation",
                    &table.relation
                ),
            }
        }
        for projection in select_query.projection.iter() {
            match projection {
                sqlparser::ast::SelectItem::UnnamedExpr(expr) => match expr {
                    sqlparser::ast::Expr::Identifier(identifier) => {
                        println!("{:?}", identifier.value);

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

                        let expr_group = value.set_expr_group().unwrap();
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

        println!("Select query plan Groups: {:?}", plan_group_list);
        Ok(plan_group_list)
    }
}
