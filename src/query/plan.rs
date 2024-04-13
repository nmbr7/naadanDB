use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    rc::{Rc, Weak},
    time::Duration,
};

use sqlparser::ast::Values;

use crate::storage::catalog::{Column, Table};

use super::{query_engine::ExecContext, RecordSet};

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
    pub columns: HashMap<String, Column>,
}

#[derive(Debug, Clone)]
pub struct InsertExpr {
    pub table_name: String,
    pub rows: RecordSet,
}

#[derive(Debug, Clone)]
pub enum RelationalExprType<'a> {
    ScanExpr(ScanExpr),
    CreateTableExpr(CreateTableExpr),
    InsertExpr(InsertExpr),

    FilterExpr(Edge<PlanExpr<'a>>),

    InnerJoinExpr(Edge<PlanExpr<'a>>, Edge<PlanExpr<'a>>, Edge<PlanExpr<'a>>),
    IndexScanExpr { index_id: u32 },
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
