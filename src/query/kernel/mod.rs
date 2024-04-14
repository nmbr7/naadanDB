use std::{collections::HashSet, time::SystemTime};

use tokio::task;

use crate::{
    query::{plan::RelationalExprType, RecordSet},
    storage::catalog,
};

use super::{plan::PhysicalPlanExpr, query_engine::ExecContext};

pub fn insert_table(exec_context: &mut ExecContext, physical_plan: PhysicalPlanExpr) {
    println!("Insert into table");
    let mut error = false;

    if let PhysicalPlanExpr::Relational(RelationalExprType::InsertExpr(expr)) = physical_plan {
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
                Err(_) => {
                    error = true;
                    println!("Table '{}' not found", &expr.table_name);
                }
            }
        }
    }

    exec_context.set_last_op_status(Some(!error));
}

pub fn create_table(exec_context: &mut ExecContext, physical_plan: PhysicalPlanExpr) {
    println!("creating table");

    let mut error = false;

    if let PhysicalPlanExpr::Relational(RelationalExprType::CreateTableExpr(expr)) = physical_plan {
        let mut table = catalog::Table {
            name: expr.table_name,
            schema: expr.table_schema,
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

    exec_context.set_last_op_status(Some(!error));
}

pub fn update_table(exec_context: &mut ExecContext, physical_plan: PhysicalPlanExpr) {
    println!("Update table");
    let mut error = false;

    if let PhysicalPlanExpr::Relational(RelationalExprType::UpdateExpr(expr)) = physical_plan {
        {
            let mut storage_instance = task::block_in_place(|| {
                println!("Locking storage_instance {:?}", SystemTime::now());
                exec_context.get_storage_engine().unwrap().blocking_lock()
                // do some compute-heavy work or call synchronous code
            });

            match storage_instance.get_table_details(&expr.table_name) {
                Ok(schema) => {
                    // TODO check row constraints, and procceed only if there are no conflict.
                    println!("Inserting into Table '{}'", &expr.table_name);
                    storage_instance
                        .update_table_rows(None, expr.columns, &schema)
                        .unwrap();
                    error = false;
                }
                Err(_) => {
                    error = true;
                    println!("Table '{}' not found", &expr.table_name);
                }
            }
        }
    }

    exec_context.set_last_op_status(Some(!error));
}

pub fn scan_table(exec_context: &mut ExecContext, physical_plan: PhysicalPlanExpr) {
    println!("Scanning table");

    let mut error = false;
    let result: RecordSet;

    if let PhysicalPlanExpr::Relational(RelationalExprType::ScanExpr(expr)) = physical_plan {
        {
            let storage_instance = task::block_in_place(|| {
                println!("Locking storage_instance {:?}", SystemTime::now());
                exec_context.get_storage_engine().unwrap().blocking_lock()
                // do some compute-heavy work or call synchronous code
            });

            // TODO: do proper error check
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

    exec_context.set_last_op_status(Some(!error));
}

pub fn filter(exec_context: &mut ExecContext, physical_plan: PhysicalPlanExpr) {}

pub fn join_table(exec_context: &mut ExecContext, physical_plan: PhysicalPlanExpr) {}
