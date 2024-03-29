use std::{
    cell::RefCell,
    collections::HashMap,
    rc::{Rc, Weak},
    time::Duration,
};

use crate::catalog::Column;

pub type Edge<T> = Rc<RefCell<T>>;
type WeakEdge<T> = Weak<RefCell<T>>;

#[derive(Debug)]
pub struct ScanExpr {
    pub table_id: u16,
    pub schema: HashMap<String, Column>,
}

impl ScanExpr {
    pub fn new(table_id: u16, schema: HashMap<String, Column>) -> Self {
        Self { table_id, schema }
    }
}

#[derive(Debug)]
pub enum RelationalExprType<'a> {
    ScanExpr(ScanExpr),
    InnerJoinExpr(Edge<PlanExpr<'a>>, Edge<PlanExpr<'a>>, Edge<PlanExpr<'a>>),
    FilterExpr(Edge<PlanExpr<'a>>),
    IndexScanExpr { index_id: u32 },
}

#[derive(Debug)]
pub enum ScalarExprType {
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
pub struct Relational<'a> {
    pub rel_type: RelationalExprType<'a>,
    pub group: Option<WeakEdge<PlanGroup<'a>>>,
    pub stats: Option<Stats>,
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
