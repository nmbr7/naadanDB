use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    rc::{Rc, Weak},
    time::Duration,
};

use sqlparser::ast::{Expr, Values};

use crate::storage::catalog::{Column, Table};

use super::{query_engine::ExecContext, NaadanRecord, RecordSet};

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
    pub op: ScanType,
}

impl ScanExpr {
    pub fn new(schema: Table, predicate: Option<ScalarExprType>, op: ScanType) -> Self {
        Self {
            schema,
            predicate,
            op,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreateTableExpr {
    pub table_name: String,
    pub table_schema: HashMap<String, Column>,
}

impl CreateTableExpr {
    pub fn new(table_name: String, table_schema: HashMap<String, Column>) -> Self {
        Self {
            table_name,
            table_schema,
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
    pub columns: HashMap<String, Expr>,
    pub predicate: Option<ScalarExprType>,
}

impl UpdateExpr {
    pub fn new(
        table_name: String,
        columns: HashMap<String, Expr>,
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

    FilterExpr(Edge<PlanExpr<'a>>),

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
        value: u64,
    },
}

#[derive(Debug)]
pub enum PhysicalPlanExpr<'a> {
    Relational(RelationalExprType<'a>),
    Scalar(ScalarExprType),
}

pub type PlanExecFn = fn(context: &mut ExecContext, PhysicalPlanExpr);

#[derive(Debug)]
pub struct PhysicalPlan<'a> {
    pub plan_expr: PhysicalPlanExpr<'a>,
    pub plane_exec_fn: PlanExecFn,
}

impl<'a> PhysicalPlan<'a> {
    pub fn new(plan_expr: PhysicalPlanExpr<'a>, plane_exec_fn: PlanExecFn) -> Self {
        Self {
            plan_expr,
            plane_exec_fn,
        }
    }
}

unsafe impl<'a> Send for PhysicalPlan<'a> {}

unsafe impl<'a> Sync for PhysicalPlan<'a> {}

#[derive(Debug)]
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

#[derive(Debug)]
pub struct Scalar<'a> {
    pub rel_type: ScalarExprType,
    pub group: Option<WeakEdge<PlanGroup<'a>>>,
    pub stats: Option<Stats>,
}

#[derive(Debug)]
pub enum PlanExpr<'a> {
    Relational(Relational<'a>),
    Scalar(Scalar<'a>),
}

impl<'a> PlanExpr<'a> {
    pub fn set_expr_group(self) -> Result<Edge<PlanGroup<'a>>, bool> {
        let expr_grp = Rc::new(RefCell::new(PlanGroup {
            exprs: vec![],
            best_expr: None,
        }));

        let rel_expr = Rc::new(RefCell::new(self));

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

#[derive(Debug)]
pub struct PlanGroup<'a> {
    pub exprs: Vec<Edge<PlanExpr<'a>>>,
    pub best_expr: Option<Edge<PlanExpr<'a>>>,
}

#[derive(Debug)]
pub struct Stats {
    pub estimated_row_count: u32,
    pub estimated_time: Duration,
}

// The final cumulative Plan struct
#[derive(Debug)]
pub struct Plan<'a> {
    pub plan_stats: Option<Stats>,
    pub plan_expr_root: Option<Edge<PlanExpr<'a>>>,
}

unsafe impl<'a> Send for Plan<'a> {}

unsafe impl<'a> Sync for Plan<'a> {}
