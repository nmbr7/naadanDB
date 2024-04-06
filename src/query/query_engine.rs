use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use crate::query::plan::*;
use crate::storage::catalog::{self, Column, ColumnType};
use crate::storage::StorageEngine;

use log::{debug, error};
use sqlparser::ast::{SetExpr, Statement, TableFactor};

use tokio::sync::Mutex;
use tokio::task;

use super::NaadanQuery;

pub struct ExecContext {
    last_result: Option<Vec<u8>>,
    last_op_name: Option<String>,
    last_op_time: Option<Duration>,
    last_op_status: Option<bool>,

    pub storage_engine: Option<Arc<Mutex<Box<dyn StorageEngine + Send>>>>,
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

    pub fn last_result(&self) -> Option<&Vec<u8>> {
        self.last_result.as_ref()
    }

    pub fn set_storage_engine(
        &mut self,
        storage_engine: Arc<Mutex<Box<dyn StorageEngine + Send>>>,
    ) {
        self.storage_engine = Some(storage_engine);
    }
    pub fn get_storage_engine(&mut self) -> Option<&Arc<Mutex<Box<dyn StorageEngine + Send>>>> {
        self.storage_engine.as_ref()
    }
}

fn insert_table(exec_context: &mut ExecContext, physical_plan: PhysicalPlanExpr) {
    debug!("insert into table");
    let mut error = false;
    let mut invalid_type = false;

    match physical_plan {
        PhysicalPlanExpr::Relational(val) => match val {
            RelationalExprType::InsertExpr(expr) => {
                {
                    let mut storage_instance = task::block_in_place(|| {
                        println!("Locking storage_instance {:?}", SystemTime::now());
                        exec_context.get_storage_engine().unwrap().blocking_lock()
                        // do some compute-heavy work or call synchronous code
                    });

                    match storage_instance.get_table_details(&expr.table_name) {
                        Ok(val) => {
                            // TODO check row constraints, and procceed only if there are no conflict.
                            debug!("Inserting into Table '{}'", &expr.table_name);
                            storage_instance
                                .add_row_into_table(expr.columns, &val)
                                .unwrap();
                            error = false;
                        }
                        Err(_) => error = false,
                    }
                }
            }
            _ => invalid_type = true,
        },
        _ => invalid_type = true,
    }

    if error {
        exec_context.last_op_status = Some(false);
        if invalid_type {
            error!("Create table with wrong expr type ");
        }
    } else {
        exec_context.last_op_status = Some(true);
    }
}

fn create_table(exec_context: &mut ExecContext, physical_plan: PhysicalPlanExpr) {
    debug!("creating table");

    let mut error = false;
    let mut invalid_type = false;

    match physical_plan {
        PhysicalPlanExpr::Relational(val) => match val {
            RelationalExprType::CreateTableExpr(expr) => {
                let mut table = catalog::Table {
                    name: expr.table_name,
                    schema: expr.columns,
                    indexes: HashSet::new(),
                    id: 0,
                };
                {
                    let mut storage_instance = task::block_in_place(|| {
                        println!("Locking storage_instance {:?}", SystemTime::now());
                        exec_context.get_storage_engine().unwrap().blocking_lock()
                        // do some compute-heavy work or call synchronous code
                    });

                    match storage_instance.get_table_details(&table.name) {
                        Ok(_) => {
                            debug!("Table '{}' already exists", &table.name);
                            error = true;
                        }
                        Err(_) => {
                            storage_instance.add_table(&mut table).unwrap();
                        }
                    }
                }
            }
            _ => invalid_type = true,
        },
        _ => invalid_type = true,
    }

    if error {
        exec_context.last_op_status = Some(false);
        if invalid_type {
            error!("Create table with wrong expr type ");
        }
    } else {
        exec_context.last_op_status = Some(true);
    }
}

fn scan_table(exec_context: &mut ExecContext, physical_plan: PhysicalPlanExpr) {
    debug!("scaning table");
}

pub struct NaadanQueryEngine {
    pub storage_engine: Arc<Mutex<Box<dyn StorageEngine + Send>>>,
}

impl NaadanQueryEngine {
    pub async fn init(storage_engine: Arc<Mutex<Box<dyn StorageEngine + Send>>>) -> Self {
        Self {
            storage_engine: storage_engine,
        }
    }

    pub fn prepare_exec_plan<'a>(
        &'a self,
        logical_plan: &Vec<Plan<'a>>,
    ) -> Result<Vec<PhysicalPlan>, bool> {
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

        debug!("Physical_plan prepared.");
        Ok(exec_vec)
    }

    pub async fn prepare_logical_plan<'a>(
        &'a self,
        query: &'a NaadanQuery,
    ) -> Result<Vec<Plan<'a>>, bool> {
        let mut final_plan_list: Vec<Plan<'a>> = vec![];
        // Iterate through the AST and create best logical plan which potentially has the least f execution

        // TODO: Read from the DB catalog to get info about the the table (table name id, row count, columns etc..)
        //       based on the info from the catalog info, validate the query.
        for statement in query.ast.iter() {
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
                        let storage_instance = task::block_in_place(move || {
                            println!("Locking storage_instance {:?}", SystemTime::now());
                            self.storage_engine.blocking_lock()
                        });

                        match storage_instance.get_table_details(&table_name) {
                            Ok(_) => {
                                debug!("Table '{}' already exists", &table_name);
                                return Err(false);
                            }
                            Err(_) => {}
                        }
                    }

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
                    debug!(
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
                                    SetExpr::Values(val) => val.clone(),
                                    _ => return Err(false),
                                };

                                // Create plan
                                let local_plan = PlanExpr::Relational(Relational {
                                    rel_type: RelationalExprType::InsertExpr(InsertExpr {
                                        table_name: t_name,
                                        columns: row_values,
                                    }),
                                    group: None,
                                    stats: None,
                                });

                                plan.plan_expr_root = Some(Rc::new(RefCell::new(local_plan)));
                            }
                            Err(_) => {
                                debug!(" Invalid Table {}", table_name);
                                return Err(false);
                            }
                        }
                    }
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

    pub fn execute<'a>(&self, physical_plan: Vec<PhysicalPlan>) -> Result<Vec<u8>, bool> {
        debug!("Executing final query plan.");

        // TODO let the scheduler decide how and where the execution will take place.
        let mut exec_context = ExecContext::init();
        exec_context.set_storage_engine(self.storage_engine.clone());
        let now = Instant::now();
        for plan in physical_plan {
            (plan.plane_exec_fn)(&mut exec_context, plan.plan_expr);
        }

        let elapsed_time = now.elapsed();

        if exec_context.last_op_status.unwrap_or(true) {
            Ok(format!(
                "Plan executed success in {:.3}ms",
                elapsed_time.as_micros() as f64 / 1000.0
            )
            .into())
        } else {
            Err(false)
        }
    }
}