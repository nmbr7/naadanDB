use std::{collections::HashSet, time::SystemTime};

use log::{debug, error};
use sqlparser::ast::Values;
use tokio::task;

use crate::{
    query::{plan::RelationalExprType, RecordSet},
    storage::catalog,
};

use super::{plan::PhysicalPlanExpr, query_engine::ExecContext};

pub fn insert_table(exec_context: &mut ExecContext, physical_plan: PhysicalPlanExpr) {
    println!("insert into table");
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
                            println!("Inserting into Table '{}'", &expr.table_name);
                            storage_instance.write_table_rows(expr.rows, &val).unwrap();
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
        exec_context.set_last_op_status(Some(false));
        if invalid_type {
            error!("Create table with wrong expr type ");
        }
    } else {
        exec_context.set_last_op_status(Some(true));
    }
}

pub fn create_table(exec_context: &mut ExecContext, physical_plan: PhysicalPlanExpr) {
    println!("creating table");

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
                            println!("Table '{}' already exists", &table.name);
                            error = true;
                        }
                        Err(_) => {
                            storage_instance.add_table_details(&mut table).unwrap();
                        }
                    }
                }
            }
            _ => invalid_type = true,
        },
        _ => invalid_type = true,
    }

    if error {
        exec_context.set_last_op_status(Some(false));
        if invalid_type {
            error!("Create table with wrong expr type ");
        }
    } else {
        exec_context.set_last_op_status(Some(true));
    }
}

pub fn scan_table(exec_context: &mut ExecContext, physical_plan: PhysicalPlanExpr) {
    println!("scaning table");

    let mut error = false;
    let mut invalid_type = false;
    let result: RecordSet;

    match physical_plan {
        PhysicalPlanExpr::Relational(val) => match val {
            RelationalExprType::ScanExpr(expr) => {
                {
                    let mut storage_instance = task::block_in_place(|| {
                        println!("Locking storage_instance {:?}", SystemTime::now());
                        exec_context.get_storage_engine().unwrap().blocking_lock()
                        // do some compute-heavy work or call synchronous code
                    });

                    match expr.op {
                        crate::query::plan::ScanType::WildCardScan => {
                            result = storage_instance.scan_table(None, &expr.schema).unwrap();
                        }
                        crate::query::plan::ScanType::Explicit(_) => {
                            result = storage_instance
                                .read_table_rows(&[1, 2], &expr.schema)
                                .unwrap();
                        }
                    }
                }
                exec_context.set_last_result(Some(result));
            }
            _ => invalid_type = true,
        },
        _ => invalid_type = true,
    }

    if error {
        exec_context.set_last_op_status(Some(false));
        if invalid_type {
            error!("select table with wrong expr type ");
        }
    } else {
        exec_context.set_last_op_status(Some(true));
    }
}

pub fn filter(exec_context: &mut ExecContext, physical_plan: PhysicalPlanExpr) {}

pub fn join_table(exec_context: &mut ExecContext, physical_plan: PhysicalPlanExpr) {}
