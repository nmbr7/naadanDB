use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use crate::query::kernel::ExecContext;
use crate::query::plan::{IndexScanExpr, *};
use crate::query::NaadanRecord;
use crate::server::{SessionContext, TransactionType};
use crate::storage::catalog::{Column, ColumnSize, ColumnType, Table};

use crate::storage::{NaadanError, StorageEngine};
use crate::transaction::TransactionManager;
use crate::{rc_ref_cell, utils};

use sqlparser::ast::{Expr, Insert, SetExpr, Statement, TableFactor, Values};
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

    pub fn ok() -> Self {
        Self {
            query_str: None,
            result: None,
            exec_stats: ExecStats::new(SystemTime::now(), Duration::from_secs(1)),
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

/// The core query engine struct
/// - It takes a reference to the global reference counted storage engine instance
pub struct NaadanQueryEngine<E: StorageEngine> {
    pub transaction_manager: Arc<Box<TransactionManager<E>>>,
}

impl<E: StorageEngine> NaadanQueryEngine<E> {
    /// Initialize the query engine with the shared storage engine instance
    pub async fn init(transaction_manager: Arc<Box<TransactionManager<E>>>) -> Self {
        Self {
            transaction_manager: transaction_manager,
        }
    }

    /// Process the sql query AST
    pub async fn process_query(
        &self,
        session_context: &mut SessionContext,
        query: NaadanQuery,
    ) -> Vec<Result<QueryResult, NaadanError>> {
        let mut result: Vec<Result<QueryResult, NaadanError>> = Vec::new();

        for statement in query.ast.iter() {
            if let Some(value) = self
                .process_query_statement(session_context, statement)
                .await
            {
                result.push(value);
            }
        }

        result
    }

    async fn process_query_statement(
        &self,
        session_context: &mut SessionContext,
        statement: &Statement,
    ) -> Option<Result<QueryResult, NaadanError>> {
        let query_str = statement.to_string();
        utils::log(
            format!("QueryEngine - TID: {:?}", session_context.transaction_id()),
            format!("Processing query: {}", query_str),
        );

        match *session_context.transaction_type() {
            TransactionType::Implicit => {
                let transaction = self
                    .transaction_manager
                    .start_new_transaction(self.transaction_manager.clone())
                    .unwrap();

                session_context.set_transaction_id(transaction.id());
                transaction.add_query(query_str);
            }
            TransactionType::Explicit => {
                let transaction = self
                    .transaction_manager
                    .get_active_transaction(session_context.transaction_id());

                transaction.add_query(query_str);
            }
        }

        // Create logical plan for the query from the AST
        let logical_plan = match self.prepare_logical_plan(session_context, statement).await {
            Ok(val) => val,
            Err(err) => {
                self.transaction_manager
                    .rollback_transaction(session_context.transaction_id())
                    .unwrap();
                return Some(Err(err));
            }
        };
        utils::log(
            format!("QueryEngine - TID: {:?}", session_context.transaction_id()),
            format!("Logical Plan is : {:?}", logical_plan),
        );

        if let None = logical_plan.last().unwrap().plan_expr_root {
            return Some(Ok(QueryResult::ok()));
        }

        // Prepare the physical plan
        let physical_plan = match self
            .prepare_physical_plan(session_context, &logical_plan)
            .await
        {
            Ok(val) => val,
            Err(err) => {
                self.transaction_manager
                    .rollback_transaction(session_context.transaction_id())
                    .unwrap();
                return Some(Err(err));
            }
        };
        utils::log(
            format!("QueryEngine - TID: {:?}", session_context.transaction_id()),
            format!("Physical Plan is : {:?}", physical_plan),
        );

        // Execute the query using the physical plan
        Some(self.execute(session_context, physical_plan).await)
    }

    /// Execute the physical query plan.
    pub async fn execute<'a>(
        &self,
        session_context: &mut SessionContext,
        physical_plan: Vec<PhysicalPlan<'a, E>>,
    ) -> Result<QueryResult, NaadanError> {
        utils::log(
            format!("QueryEngine - TID: {:?}", session_context.transaction_id()),
            format!("Executing final query plan."),
        );

        let mut retry = 0;
        loop {
            retry += 1;
            // TODO let the scheduler decide how and where the execution will take place.
            let mut exec_context = ExecContext::init();

            exec_context.set_transaction(
                self.transaction_manager
                    .get_active_transaction(session_context.transaction_id()),
            );

            let begin_time = SystemTime::now();
            let now = Instant::now();

            for plan in &physical_plan {
                (plan.plane_exec_fn)(&mut exec_context, plan.plan_expr.clone());
            }

            let elapsed_time = now.elapsed();

            if exec_context.last_op_status.unwrap_or(true) {
                match session_context.transaction_type() {
                    // Retry implicit internal transactions
                    TransactionType::Implicit => {
                        let result = self
                            .transaction_manager
                            .commit_transaction(session_context.transaction_id());

                        match result {
                            Ok(()) => {
                                return Ok(QueryResult::new(
                                    ExecStats::new(begin_time, elapsed_time),
                                    exec_context.last_result,
                                    None,
                                ))
                            }
                            Err(_) => {
                                let current_transaction = self
                                    .transaction_manager
                                    .get_active_transaction(session_context.transaction_id());

                                current_transaction.set_start_timestamp(
                                    self.transaction_manager.get_new_timestamp(),
                                );

                                utils::log(
                                    format!(
                                        "QueryEngine - TID: {:?}",
                                        session_context.transaction_id()
                                    ),
                                    format!(
                                        "Retry {} for transaction {}",
                                        retry,
                                        current_transaction.id()
                                    ),
                                );

                                continue;
                            }
                        }
                    }
                    // Return back error for explicit transactions, the client can rerun if required.
                    TransactionType::Explicit => {
                        if session_context.transaction_id() == 0 {
                            session_context.set_transaction_type(TransactionType::Implicit)
                        }
                        return Ok(QueryResult::new(
                            ExecStats::new(begin_time, elapsed_time),
                            exec_context.last_result,
                            None,
                        ));
                    }
                }
            } else {
                self.transaction_manager
                    .rollback_transaction(session_context.transaction_id())
                    .unwrap();
                session_context.set_transaction_type(TransactionType::Implicit);
                return Err(NaadanError::QueryExecutionFailed);
            }
        }
    }

    async fn prepare_physical_plan<'a>(
        &'a self,
        session_context: &mut SessionContext,
        logical_plan: &Vec<Plan<'a>>,
    ) -> Result<Vec<PhysicalPlan<E>>, NaadanError> {
        let mut exec_vec: Vec<PhysicalPlan<E>> = Vec::new();

        match logical_plan.last() {
            Some(plan) => {
                let root_exp = plan.plan_expr_root.as_ref().unwrap().borrow();
                match &*root_exp {
                    PlanExpr::Relational(rel_val) => match &rel_val.rel_type {
                        RelationalExprType::ScanExpr(_) => {
                            let physical_plan_expr =
                                PhysicalPlanExpr::Relational(rel_val.rel_type.clone());

                            exec_vec.push(PhysicalPlan::<E>::new(
                                physical_plan_expr,
                                ExecContext::<E>::scan_table,
                            ));
                        }
                        RelationalExprType::CreateTableExpr(_) => {
                            let physical_plan_expr =
                                PhysicalPlanExpr::Relational(rel_val.rel_type.clone());

                            exec_vec.push(PhysicalPlan::<E>::new(
                                physical_plan_expr,
                                ExecContext::<E>::create_table,
                            ));
                        }
                        RelationalExprType::InsertExpr(_) => {
                            let physical_plan_expr =
                                PhysicalPlanExpr::Relational(rel_val.rel_type.clone());

                            exec_vec.push(PhysicalPlan::<E>::new(
                                physical_plan_expr,
                                ExecContext::<E>::insert_table,
                            ));
                        }
                        RelationalExprType::InnerJoinExpr(_, _, _) => todo!(),
                        RelationalExprType::FilterExpr(_) => todo!(),
                        RelationalExprType::IndexScanExpr(IndexScanExpr { index_id: _ }) => todo!(),
                        RelationalExprType::UpdateExpr(_) => {
                            let physical_plan_expr =
                                PhysicalPlanExpr::Relational(rel_val.rel_type.clone());

                            exec_vec.push(PhysicalPlan::<E>::new(
                                physical_plan_expr,
                                ExecContext::<E>::update_table,
                            ));
                        }
                    },

                    _ => {}
                }
            }
            None => todo!(),
        }

        utils::log(
            format!("QueryEngine - TID: {:?}", session_context.transaction_id()),
            format!("Physical_plan prepared."),
        );
        Ok(exec_vec)
    }

    async fn prepare_logical_plan<'a>(
        &'a self,
        session_context: &mut SessionContext,
        statement: &Statement,
    ) -> Result<Vec<Plan<'a>>, NaadanError> {
        let mut final_plan_list: Vec<Plan<'a>> = vec![];
        // Iterate through the AST and create best logical plan which potentially has the least f execution

        let mut plan: Plan = Plan::init();

        match statement {
            Statement::CreateTable { name, columns, .. } => {
                let mut column_map: BTreeMap<String, Column> = BTreeMap::new();
                let table_name = name.0.last().unwrap().value.clone();
                {
                    let storage_instance = self.transaction_manager.storage_engine();

                    match storage_instance.get_table_details(&table_name) {
                        Ok(_) => {
                            utils::log(
                                format!(
                                    "QueryEngine - TID: {:?}",
                                    session_context.transaction_id()
                                ),
                                format!("Table '{}' already exists", &table_name),
                            );
                            return Err(NaadanError::TableAlreadyExists);
                        }
                        Err(_) => {}
                    }
                }

                utils::log(
                    format!("QueryEngine - TID: {:?}", session_context.transaction_id()),
                    format!("Columns {:?}", columns),
                );
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

                    utils::log(
                        format!("QueryEngine - TID: {:?}", session_context.transaction_id()),
                        format!("Column offset is {:?}", offset),
                    );

                    column_map.insert(
                        col.name.value.clone(),
                        Column {
                            column_type: ColumnType::from(&col.data_type),
                            offset,
                            is_nullable,
                        },
                    );

                    offset += ColumnSize::from(&col.data_type).get_value();
                }

                // Create plan
                let local_plan = PlanExpr::Relational(Relational {
                    rel_type: RelationalExprType::CreateTableExpr(CreateTableExpr {
                        table_name: table_name,
                        table_schema: column_map,
                        table_schema_size: offset,
                    }),
                    group: None,
                    stats: None,
                });

                plan.set_plan_expr_root(Some(rc_ref_cell!(local_plan)));
            }
            Statement::AlterTable { .. } => {}

            Statement::Insert(Insert {
                table_name,
                columns,
                source,
                ..
            }) => {
                utils::log(
                    format!("QueryEngine - TID: {:?}", session_context.transaction_id()),
                    format!(
                        "Insert statement, table name is {} with columns {:?} and values {:?}",
                        table_name, columns, source
                    ),
                );
                let table_name = table_name.0.last().unwrap().value.clone();

                {
                    let storage_instance = self.transaction_manager.storage_engine();

                    match storage_instance.get_table_details(&table_name) {
                        Ok(table_schema) => {
                            let row_values = match &*(source.as_ref().unwrap().body) {
                                SetExpr::Values(Values { rows, .. }) => {
                                    let records: Vec<NaadanRecord> = rows
                                        .iter()
                                        .map(|row| NaadanRecord::new(0, row.clone()))
                                        .collect();

                                    if !is_records_valid(&records, columns, table_schema) {
                                        return Err(NaadanError::SchemaValidationFailed);
                                    }

                                    RecordSet::new(records)
                                }
                                _ => return Err(NaadanError::LogicalPlanFailed),
                            };

                            {
                                // Create plan
                                let local_plan = PlanExpr::Relational(Relational::new(
                                    RelationalExprType::InsertExpr(InsertExpr::new(
                                        table_name, row_values,
                                    )),
                                    None,
                                    None,
                                ));

                                plan.set_plan_expr_root(Some(rc_ref_cell!(local_plan)));
                            }
                        }
                        Err(err) => {
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
            } => {
                let mut table_name = String::new();
                match &table.relation {
                    TableFactor::Table { name, .. } => {
                        table_name = name.0.last().unwrap().value.clone()
                    }
                    _ => {}
                }

                let storage_instance = self.transaction_manager.storage_engine();

                match storage_instance.get_table_details(&table_name) {
                    Ok(table_schema) => {
                        // TODO validate the columns

                        let mut columns: BTreeMap<String, Expr> = BTreeMap::new();
                        for assignment in assignments {
                            let id = &assignment.id;
                            let val = &assignment.value;

                            columns.insert(id.last().unwrap().value.clone(), val.clone());
                        }

                        let local_plan = PlanExpr::Relational(Relational::new(
                            RelationalExprType::UpdateExpr(UpdateExpr::new(
                                table_name, columns, None,
                            )),
                            None,
                            None,
                        ));

                        plan.set_plan_expr_root(Some(rc_ref_cell!(local_plan)));
                    }
                    Err(err) => {
                        return Err(err);
                    }
                }
            }
            Statement::Delete { .. } => {}

            Statement::StartTransaction {
                modes,
                begin,
                modifier,
            } => {
                session_context.set_transaction_type(TransactionType::Explicit);
            }
            Statement::Rollback { chain, savepoint } => {
                if session_context.transaction_id() > 0 {
                    self.transaction_manager
                        .rollback_transaction(session_context.transaction_id())
                        .unwrap();
                } else {
                    return Err(NaadanError::TransactionSessionInvalid);
                }
            }
            Statement::Commit { chain } => {
                if session_context.transaction_id() > 0 {
                    let result = self
                        .transaction_manager
                        .commit_transaction(session_context.transaction_id());

                    session_context.set_transaction_id(0);
                    session_context.set_transaction_type(TransactionType::Implicit);

                    match result {
                        Ok(()) => {}
                        Err(err) => return Err(err),
                    }
                } else {
                    return Err(NaadanError::TransactionSessionInvalid);
                }
            }

            Statement::CreateDatabase { .. } => {}
            Statement::Drop { .. } => {}

            Statement::CreateIndex { .. } => {}
            Statement::AlterIndex { .. } => {}

            Statement::Query(query_data) => {
                match &*query_data.body {
                    SetExpr::Select(select_query) => {
                        //  Get the query target Tables as in the 'FROM' expression
                        let result = self
                            .prepare_select_query_plan(session_context, select_query)
                            .await;
                        let expr_group = match result {
                            Ok(val) => val,
                            Err(err) => {
                                utils::log(
                                    format!(
                                        "QueryEngine - TID: {:?}",
                                        session_context.transaction_id()
                                    ),
                                    format!("Query failed with error: {}", err),
                                );
                                return Err(err);
                            }
                        };
                        //utils::log(format!("{:?}", expr_group));
                        let first_vec = expr_group.first().unwrap();
                        let first_expr = Rc::clone(first_vec.borrow().exprs.first().unwrap());
                        plan.plan_expr_root = Some(first_expr);
                        //utils::log(format!("{:?}", expr_group));
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

    async fn prepare_select_query_plan(
        &self,
        session_context: &SessionContext,
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
                            utils::log(
                                format!(
                                    "QueryEngine - TID: {:?}",
                                    session_context.transaction_id()
                                ),
                                format!("Locking storage_instance {:?}", SystemTime::now()),
                            );
                            self.transaction_manager.storage_engine()
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

                    utils::log(
                        format!("QueryEngine - TID: {:?}", session_context.transaction_id()),
                        format!(" Select query on Table {}", table_name),
                    );
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
                        utils::log(
                            format!("QueryEngine - TID: {:?}", session_context.transaction_id()),
                            format!("{:?}", identifier.value),
                        );

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

        utils::log(
            format!("QueryEngine - TID: {:?}", session_context.transaction_id()),
            format!("Select query plan Groups: {:?}", plan_group_list),
        );
        Ok(plan_group_list)
    }
}

fn is_records_valid(
    rows: &Vec<NaadanRecord>,
    columns: &Vec<sqlparser::ast::Ident>,
    table_schema: Table,
) -> bool {
    if columns.len() != 0 {
        // Validate by column names position and count
        if columns.len() == table_schema.schema.len() {
            rows.iter().all(|row: &NaadanRecord| {
                if row.columns.len() != table_schema.schema.len() {
                    return false;
                }
                row.columns()
                    .iter()
                    .zip(columns)
                    .enumerate()
                    .all(|(c_index, (c_value, c_name))| {
                        if let Some(col) = table_schema.schema.get(&c_name.value) {
                            // TODO do proper validation
                            true
                        } else {
                            false
                        }
                    })
            })
        } else {
            false
        }
    } else {
        // TODO: validate by column value position
        true
    }
}
