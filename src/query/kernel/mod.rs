use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, SystemTime},
};

use tokio::task;

use crate::{
    query::{plan::RelationalExprType, RecordSet},
    storage::{catalog, CatalogEngine, StorageEngine},
    transaction::MvccTransaction,
};

use super::plan::PhysicalPlanExpr;

/// Execution context for the physical plan executor
pub struct ExecContext<E: StorageEngine> {
    pub last_result: Option<RecordSet>,
    last_op_name: Option<String>,
    last_op_time: Option<Duration>,
    pub last_op_status: Option<bool>,

    pub transaction: Option<Arc<Box<MvccTransaction<E>>>>,
}

impl<E: StorageEngine> ExecContext<E> {
    pub fn init() -> Self {
        Self {
            last_result: None,
            last_op_name: None,
            last_op_time: None,
            transaction: None,
            last_op_status: None,
        }
    }

    pub fn last_result(&self) -> Option<&RecordSet> {
        self.last_result.as_ref()
    }

    pub fn set_transaction(&mut self, transaction: Arc<Box<MvccTransaction<E>>>) {
        self.transaction = Some(transaction);
    }
    pub fn get_transaction(&mut self) -> Option<&Arc<Box<MvccTransaction<E>>>> {
        self.transaction.as_ref()
    }

    pub fn set_last_result(&mut self, last_result: Option<RecordSet>) {
        self.last_result = last_result;
    }

    pub fn set_last_op_status(&mut self, last_op_status: Option<bool>) {
        self.last_op_status = last_op_status;
    }

    pub fn insert_table(&mut self, physical_plan: PhysicalPlanExpr) {
        println!("Insert into table");
        let mut error = false;

        if let PhysicalPlanExpr::Relational(RelationalExprType::InsertExpr(expr)) = physical_plan {
            {
                let mut transaction = task::block_in_place(|| {
                    println!("Locking storage_instance {:?}", SystemTime::now());
                    self.get_transaction().unwrap()
                    // do some compute-heavy work or call synchronous code
                });

                match transaction.get_table_details(&expr.table_name) {
                    Ok(val) => {
                        // TODO check row constraints, and procceed only if there are no conflict.
                        println!("Inserting into Table '{}'", &expr.table_name);
                        transaction.write_table_rows(expr.rows, &val).unwrap();
                        error = false;
                    }
                    Err(_) => {
                        error = true;
                        println!("Table '{}' not found", &expr.table_name);
                    }
                }
            }
        }

        self.set_last_op_status(Some(!error));
    }

    pub fn create_table(&mut self, physical_plan: PhysicalPlanExpr) {
        println!("creating table");

        let mut error = false;

        if let PhysicalPlanExpr::Relational(RelationalExprType::CreateTableExpr(expr)) =
            physical_plan
        {
            let mut table = catalog::Table {
                name: expr.table_name,
                schema: expr.table_schema,
                indexes: HashSet::new(),
                id: 0,
            };
            {
                let transaction = task::block_in_place(|| {
                    println!("Locking storage_instance {:?}", SystemTime::now());
                    self.get_transaction().unwrap()
                    // do some compute-heavy work or call synchronous code
                });

                match transaction.get_table_details(&table.name) {
                    Ok(_) => {
                        println!("Table '{}' already exists", &table.name);
                        error = true;
                    }
                    Err(_) => {
                        transaction.add_table_details(&mut table).unwrap();
                    }
                }
            }
        }

        self.set_last_op_status(Some(!error));
    }

    pub fn update_table(&mut self, physical_plan: PhysicalPlanExpr) {
        println!("Update table");
        let mut error = false;

        if let PhysicalPlanExpr::Relational(RelationalExprType::UpdateExpr(expr)) = physical_plan {
            {
                let transaction = task::block_in_place(|| {
                    println!("Locking storage_instance {:?}", SystemTime::now());
                    self.get_transaction().unwrap()
                    // do some compute-heavy work or call synchronous code
                });

                match transaction.get_table_details(&expr.table_name) {
                    Ok(schema) => {
                        // TODO check row constraints, and procceed only if there are no conflict.
                        println!("Inserting into Table '{}'", &expr.table_name);
                        transaction
                            .update_table_rows(None, &expr.columns, &schema)
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

        self.set_last_op_status(Some(!error));
    }

    pub fn scan_table(&mut self, physical_plan: PhysicalPlanExpr) {
        println!("Scanning table");

        let mut error = false;
        let mut result: RecordSet = RecordSet::new(vec![]);

        if let PhysicalPlanExpr::Relational(RelationalExprType::ScanExpr(expr)) = physical_plan {
            {
                let transaction = task::block_in_place(|| {
                    println!("Locking storage_instance {:?}", SystemTime::now());
                    self.get_transaction().unwrap()
                    // do some compute-heavy work or call synchronous code
                });

                // TODO: do proper error check
                match expr.op {
                    crate::query::plan::ScanType::WildCardScan => task::block_in_place(|| {
                        for row in transaction.scan_table(None, &expr.schema) {
                            match row {
                                Ok(row) => result.add_record(row),
                                Err(error) => {}
                            }
                        }
                    }),
                    crate::query::plan::ScanType::Explicit(_) => {
                        for row in transaction.read_table_rows(&[1, 2], &expr.schema) {
                            match row {
                                Ok(row) => result.add_record(row),
                                Err(error) => {}
                            }
                        }
                    }
                }
            }
            self.set_last_result(Some(result));
        }

        self.set_last_op_status(Some(!error));
    }

    pub fn filter(&mut self, physical_plan: PhysicalPlanExpr) {}

    pub fn join_table(&mut self, physical_plan: PhysicalPlanExpr) {}
}
