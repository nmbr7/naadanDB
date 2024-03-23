use std::{
    cell::RefCell,
    collections::HashSet,
    iter::Map,
    rc::{Rc, Weak},
    time::Duration,
};

pub type Edge<T> = Rc<RefCell<T>>;
type WeakEdge<T> = Weak<RefCell<T>>;

#[derive(Debug)]
pub enum RelationalExprType<'a> {
    ScanExpr { table_id: u32 },
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
}

#[derive(Debug)]
pub struct Scalar<'a> {
    pub rel_type: ScalarExprType,
    pub group: Option<WeakEdge<PlanGroup<'a>>>,
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

// The final cumulative Plan struct
#[derive(Debug)]
pub struct Plan<'a> {
    pub estimated_row_count: u32,
    pub plan_expr_root: Option<Edge<PlanExpr<'a>>>,
    pub estimated_time: Duration,
}
