use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, SystemTime},
};

use sqlparser::ast::{Expr, Value};
use tokio::task;

use crate::{
    query::{
        plan::{RelationalExprType, ScalarExprType},
        RecordSet,
    },
    storage::{catalog, CatalogEngine, NaadanError, ScanType, StorageEngine},
    transaction::MvccTransaction,
    utils,
};

use super::{plan::PhysicalPlanExpr, NaadanRecord};

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
        utils::log(
            format!(
                "QueryEngine::Executor - TID: {:?}",
                self.transaction.as_ref().unwrap().id()
            ),
            format!("Insert into table"),
        );
        let mut error = false;

        if let PhysicalPlanExpr::Relational(RelationalExprType::InsertExpr(expr)) = physical_plan {
            {
                let transaction = task::block_in_place(|| {
                    utils::log(
                        format!(
                            "QueryEngine::Executor - TID: {:?}",
                            self.transaction.as_ref().unwrap().id()
                        ),
                        format!("Locking storage_instance {:?}", SystemTime::now()),
                    );
                    self.get_transaction().unwrap()
                    // do some compute-heavy work or call synchronous code
                });

                match transaction.get_table_details(&expr.table_name) {
                    Ok(val) => {
                        // TODO check row constraints, and procceed only if there are no conflict.
                        utils::log(
                            format!("QueryEngine::Executor - TID: {:?}", transaction.id()),
                            format!("Inserting into Table '{}'", &expr.table_name),
                        );
                        transaction.write_table_rows(expr.rows, &val).unwrap();
                        error = false;
                    }
                    Err(_) => {
                        error = true;
                        utils::log(
                            format!(
                                "QueryEngine::Executor - TID: {:?}",
                                self.transaction.as_ref().unwrap().id()
                            ),
                            format!("Table '{}' not found", &expr.table_name),
                        );
                    }
                }
            }
        }

        self.set_last_op_status(Some(!error));
    }

    pub fn create_table(&mut self, physical_plan: PhysicalPlanExpr) {
        utils::log(
            format!(
                "QueryEngine::Executor - TID: {:?}",
                self.transaction.as_ref().unwrap().id()
            ),
            format!("creating table"),
        );

        let mut error = false;

        if let PhysicalPlanExpr::Relational(RelationalExprType::CreateTableExpr(expr)) =
            physical_plan
        {
            let mut table = catalog::Table {
                name: expr.table_name,
                schema: expr.table_schema,
                column_schema_size: expr.table_schema_size,
                indexes: HashSet::new(),
                id: 0,
            };
            {
                let transaction = task::block_in_place(|| {
                    utils::log(
                        format!(
                            "QueryEngine::Executor - TID: {:?}",
                            self.transaction.as_ref().unwrap().id()
                        ),
                        format!("Locking storage_instance {:?}", SystemTime::now()),
                    );
                    self.get_transaction().unwrap()
                    // do some compute-heavy work or call synchronous code
                });

                match transaction.get_table_details(&table.name) {
                    Ok(_) => {
                        utils::log(
                            format!(
                                "QueryEngine::Executor - TID: {:?}",
                                self.transaction.as_ref().unwrap().id()
                            ),
                            format!("Table '{}' already exists", &table.name),
                        );
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
        utils::log("QueryEngine::Executor".to_string(), format!("Update table"));
        let mut error = false;

        if let PhysicalPlanExpr::Relational(RelationalExprType::UpdateExpr(expr)) = physical_plan {
            {
                let transaction = task::block_in_place(|| {
                    utils::log(
                        format!(
                            "QueryEngine::Executor - TID: {:?}",
                            self.transaction.as_ref().unwrap().id()
                        ),
                        format!("Locking storage_instance {:?}", SystemTime::now()),
                    );
                    self.get_transaction().unwrap()
                    // do some compute-heavy work or call synchronous code
                });

                match transaction.get_table_details(&expr.table_name) {
                    Ok(schema) => {
                        // TODO check row constraints, and procceed only if there are no conflict.
                        utils::log(
                            format!("QueryEngine::Executor - TID: {:?}", transaction.id()),
                            format!("Inserting into Table '{}'", &expr.table_name),
                        );

                        let update_predicate: ScalarExprType;
                        let update_scan_type = match expr.predicate {
                            Some(predicate) => {
                                update_predicate = predicate;
                                ScanType::Filter(update_predicate)
                            }
                            None => ScanType::Full,
                        };

                        transaction
                            .update_table_rows(&update_scan_type, &expr.columns, &schema)
                            .unwrap();
                        error = false;
                    }
                    Err(_) => {
                        error = true;
                        utils::log(
                            format!(
                                "QueryEngine::Executor - TID: {:?}",
                                self.transaction.as_ref().unwrap().id()
                            ),
                            format!("Table '{}' not found", &expr.table_name),
                        );
                    }
                }
            }
        }

        self.set_last_op_status(Some(!error));
    }

    pub fn scan_table(&mut self, physical_plan: PhysicalPlanExpr) {
        utils::log(
            format!(
                "QueryEngine::Executor - TID: {:?}",
                self.transaction.as_ref().unwrap().id()
            ),
            format!("Scanning table"),
        );

        let mut error = false;
        let mut result: RecordSet = RecordSet::new(vec![]);

        if let PhysicalPlanExpr::Relational(RelationalExprType::ScanExpr(expr)) = physical_plan {
            {
                utils::log(
                    format!(
                        "QueryEngine::Executor - TID: {:?}",
                        self.transaction.as_ref().unwrap().id()
                    ),
                    format!("Locking storage_instance {:?}", SystemTime::now()),
                );

                let transaction = self.get_transaction().unwrap();

                // TODO: do proper error check
                match expr.scan_type {
                    crate::query::plan::ScanType::WildCardScan => task::block_in_place(|| {
                        for row in
                            transaction.scan_table(&crate::storage::ScanType::Full, &expr.schema)
                        {
                            match row {
                                Ok(row) => result.add_record(row),
                                Err(error) => {}
                            }
                        }
                    }),
                    crate::query::plan::ScanType::Explicit(_) => todo!(),
                }
            }
            self.set_last_result(Some(result));
        }

        self.set_last_op_status(Some(!error));
    }

    pub fn filter(&mut self, physical_plan: PhysicalPlanExpr) {
        utils::log(
            format!(
                "QueryEngine::Executor - TID: {:?}",
                self.transaction.as_ref().unwrap().id()
            ),
            format!("Filtering"),
        );

        utils::log(
            format!(
                "QueryEngine::Executor - TID: {:?}",
                self.transaction.as_ref().unwrap().id()
            ),
            format!("Last result is {:?}", self.last_result),
        );

        if let PhysicalPlanExpr::Relational(RelationalExprType::FilterExpr(expr)) = physical_plan {
            {
                let mut error = false;
                let mut result: RecordSet = RecordSet::new(vec![]);

                for row in self.last_result.as_ref().unwrap() {
                    // TODO: do proper error check
                    let res = eval_predicate(&expr.borrow(), row);
                    match res {
                        Ok(Value::Boolean(true)) => result.add_record(row.clone()),
                        Ok(Value::Boolean(false)) => {}
                        _ => {
                            error = true;
                            break;
                        }
                    }
                }
                self.set_last_result(Some(result));
                self.set_last_op_status(Some(!error));
            }
        }
    }

    pub fn join_table(&mut self, physical_plan: PhysicalPlanExpr) {}
}

pub fn eval_predicate(expr: &ScalarExprType, record: &NaadanRecord) -> Result<Value, NaadanError> {
    match expr {
        ScalarExprType::Eq { left, right } => {
            let l = eval_predicate(&left, record).unwrap();
            let r = eval_predicate(&right, record).unwrap();
            let res = match (l, r) {
                (Value::Boolean(lv), Value::Boolean(rv)) => Value::Boolean(lv == rv),
                (Value::Number(lv, _), Value::Number(rv, _)) => Value::Boolean(lv.eq(&rv)),
                (Value::SingleQuotedString(lv), Value::SingleQuotedString(rv)) => {
                    Value::Boolean(lv.eq(&rv))
                }
                _ => Value::Boolean(false),
            };

            Ok(res)
        }
        ScalarExprType::NEq { left, right } => {
            let l = eval_predicate(&left, record).unwrap();
            let r = eval_predicate(&right, record).unwrap();
            let res = match (l, r) {
                (Value::Boolean(lv), Value::Boolean(rv)) => Value::Boolean(lv != rv),
                (Value::Number(lv, _), Value::Number(rv, _)) => Value::Boolean(lv.ne(&rv)),
                (Value::SingleQuotedString(lv), Value::SingleQuotedString(rv)) => {
                    Value::Boolean(lv.ne(&rv))
                }
                _ => Value::Boolean(false),
            };

            Ok(res)
        }
        ScalarExprType::Gt { left, right } => {
            let l = eval_predicate(&left, record).unwrap();
            let r = eval_predicate(&right, record).unwrap();
            let res = match (l, r) {
                (Value::Boolean(lv), Value::Boolean(rv)) => Err(NaadanError::Unknown),
                (Value::Number(lv, _), Value::Number(rv, _)) => {
                    let lv_int = lv.parse::<usize>().unwrap();
                    let rv_int = rv.parse::<usize>().unwrap();
                    Ok(Value::Boolean(lv_int.gt(&rv_int)))
                }
                (Value::SingleQuotedString(lv), Value::SingleQuotedString(rv)) => {
                    Ok({ Value::Boolean(lv.gt(&rv)) })
                }
                _ => Ok(Value::Boolean(false)),
            };

            res
        }
        ScalarExprType::Lt { left, right } => {
            let l = eval_predicate(&left, record).unwrap();
            let r = eval_predicate(&right, record).unwrap();
            let res = match (l, r) {
                (Value::Boolean(lv), Value::Boolean(rv)) => Err(NaadanError::Unknown),
                (Value::Number(lv, _), Value::Number(rv, _)) => {
                    let lv_int = lv.parse::<usize>().unwrap();
                    let rv_int = rv.parse::<usize>().unwrap();
                    Ok(Value::Boolean(lv_int.lt(&rv_int)))
                }
                (Value::SingleQuotedString(lv), Value::SingleQuotedString(rv)) => {
                    Ok(Value::Boolean(lv.lt(&rv)))
                }
                _ => Ok(Value::Boolean(false)),
            };

            res
        }

        ScalarExprType::Identifier { value } => {
            let col_val = record.column_schema().unwrap().get(value).unwrap();

            match col_val.column_type {
                catalog::ColumnType::UnSupported => Err(NaadanError::Unknown),
                _ => {
                    // TODO: Fix column index not known issue.
                    if let Some(Expr::Value(val)) = record.columns().get(col_val.offset as usize) {
                        Ok(val.clone())
                    } else {
                        Err(NaadanError::Unknown)
                    }
                }
            }
        }
        ScalarExprType::Const { value } => return Ok(value.clone()),
    }
}
