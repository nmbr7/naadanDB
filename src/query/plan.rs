use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap, HashSet},
    future::{self, Future},
    process::Output,
    rc::{Rc, Weak},
    time::Duration,
    vec,
};

use sqlparser::ast::{Expr, Value};
use tokio::task::futures;

use crate::{
    rc_ref_cell,
    storage::{
        catalog::{Column, Table},
        StorageEngine,
    },
    transaction::MvccTransaction,
};

use super::{kernel::ExecContext, RecordSet};

pub type Edge<T> = Rc<RefCell<T>>;
type WeakEdge<T> = Weak<RefCell<T>>;

#[derive(Debug, Clone)]
pub enum ScanType {
    WildCardScan,
    Explicit(HashSet<String>),
}

#[derive(Debug, Clone)]
pub struct ScanExpr {
    pub schema: Table,
    pub predicate: Option<ScalarExprType>,
    pub scan_type: ScanType,
}

impl ScanExpr {
    pub fn new(schema: Table, predicate: Option<ScalarExprType>, scan_type: ScanType) -> Self {
        Self {
            schema,
            predicate,
            scan_type,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreateTableExpr {
    pub table_name: String,
    pub table_schema: BTreeMap<String, Column>,
    pub table_schema_size: u64,
}

impl CreateTableExpr {
    pub fn new(
        table_name: String,
        table_schema: BTreeMap<String, Column>,
        table_schema_size: u64,
    ) -> Self {
        Self {
            table_name,
            table_schema,
            table_schema_size,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InsertExpr {
    pub table_name: String,
    pub rows: RecordSet,
}

impl InsertExpr {
    pub fn new(table_name: String, rows: RecordSet) -> Self {
        Self { table_name, rows }
    }
}

#[derive(Debug, Clone)]
pub struct UpdateExpr {
    pub table_name: String,
    pub columns: BTreeMap<String, Expr>,
    pub predicate: Option<ScalarExprType>,
}

impl UpdateExpr {
    pub fn new(
        table_name: String,
        columns: BTreeMap<String, Expr>,
        predicate: Option<ScalarExprType>,
    ) -> Self {
        Self {
            table_name,
            columns,
            predicate,
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndexScanExpr {
    pub index_id: u32,
}

#[derive(Debug, Clone)]
pub enum RelationalExprType<'a> {
    ScanExpr(ScanExpr),

    CreateTableExpr(CreateTableExpr),
    InsertExpr(InsertExpr),
    UpdateExpr(UpdateExpr),

    FilterExpr(Edge<ScalarExprType>),

    InnerJoinExpr(Edge<PlanExpr<'a>>, Edge<PlanExpr<'a>>, Edge<PlanExpr<'a>>),
    IndexScanExpr(IndexScanExpr),
}

#[derive(Debug, Clone)]
pub enum ScalarExprType {
    NEq {
        left: Box<ScalarExprType>,
        right: Box<ScalarExprType>,
    },
    Eq {
        left: Box<ScalarExprType>,
        right: Box<ScalarExprType>,
    },
    Gt {
        left: Box<ScalarExprType>,
        right: Box<ScalarExprType>,
    },
    Lt {
        left: Box<ScalarExprType>,
        right: Box<ScalarExprType>,
    },

    Identifier {
        value: String,
    },
    Const {
        value: Value,
    },
}

#[derive(Debug, Clone)]
pub enum PhysicalPlanExpr<'a> {
    Relational(RelationalExprType<'a>),
    Scalar(ScalarExprType),
}

pub type PlanExecFn<E> = fn(context: &mut ExecContext<E>, PhysicalPlanExpr);

#[derive(Debug)]
pub struct PhysicalPlan<'a, E: StorageEngine> {
    pub plan_expr: PhysicalPlanExpr<'a>,
    pub plane_exec_fn: PlanExecFn<E>,
    pub next_expr: Vec<Edge<PhysicalPlan<'a, E>>>,
}

impl<'a, E: StorageEngine> PhysicalPlan<'a, E> {
    pub fn new(plan_expr: PhysicalPlanExpr<'a>, plane_exec_fn: PlanExecFn<E>) -> Self {
        Self {
            plan_expr,
            plane_exec_fn,
            next_expr: vec![],
        }
    }
}

unsafe impl<'a, E: StorageEngine> Send for PhysicalPlan<'a, E> {}

unsafe impl<'a, E: StorageEngine> Sync for PhysicalPlan<'a, E> {}

#[derive(Debug, Clone)]
pub struct Relational<'a> {
    pub rel_type: RelationalExprType<'a>,
    pub group: Option<WeakEdge<PlanGroup<'a>>>,
    pub stats: Option<Stats>,
}

impl<'a> Relational<'a> {
    pub fn new(
        rel_type: RelationalExprType<'a>,
        group: Option<WeakEdge<PlanGroup<'a>>>,
        stats: Option<Stats>,
    ) -> Self {
        Self {
            rel_type,
            group,
            stats,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Scalar<'a> {
    pub rel_type: ScalarExprType,
    pub group: Option<WeakEdge<PlanGroup<'a>>>,
    pub stats: Option<Stats>,
}

#[derive(Debug, Clone)]
pub enum PlanExpr<'a> {
    Relational(Relational<'a>),
    Scalar(Scalar<'a>),
}

impl<'a> PlanExpr<'a> {
    pub fn init_expr_group(self) -> Result<Edge<PlanGroup<'a>>, bool> {
        let expr_grp = rc_ref_cell!(PlanGroup {
            exprs: vec![],
            best_expr: None,
        });

        let rel_expr = rc_ref_cell!(self);

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
}

// Logical group representing a query proocessing step
// Contains
#[derive(Debug)]
pub struct PlanGroup<'a> {
    // Collection of expressions with same symantics
    pub exprs: Vec<Edge<PlanExpr<'a>>>,
    // Best case expression from the `exprs` collection
    pub best_expr: Option<Edge<PlanExpr<'a>>>,
}

#[derive(Debug, Clone)]
pub struct Stats {
    pub estimated_row_count: u32,
    pub estimated_time: Duration,
}

impl Stats {
    pub fn init() -> Self {
        Self {
            estimated_row_count: 0,
            estimated_time: Duration::from_millis(0),
        }
    }
}

// The final cumulative Plan struct
#[derive(Debug)]
pub struct Plan<'a> {
    pub plan_stats: Option<Stats>,
    pub plan_expr: Option<Edge<PlanExpr<'a>>>,
    pub next_expr: Vec<Edge<Plan<'a>>>,
}

impl<'a> Plan<'a> {
    pub fn init() -> Self {
        Self {
            plan_stats: None,
            plan_expr: None,
            next_expr: vec![],
        }
    }

    pub fn set_plan_expr(&mut self, plan_expr: Option<Edge<PlanExpr<'a>>>) {
        self.plan_expr = plan_expr;
    }
}

unsafe impl<'a> Send for Plan<'a> {}

unsafe impl<'a> Sync for Plan<'a> {}
