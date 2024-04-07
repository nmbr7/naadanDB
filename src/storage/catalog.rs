use core::panic;
use sqlparser::ast::DataType;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Default)]
pub struct Database {
    pub id: u16,
    pub tables: HashMap<String, Table>,
    pub stored_procedures: HashMap<String, String>,
}

#[derive(Debug, Default, Clone)]
pub struct Table {
    pub id: u16,
    pub name: String,
    pub schema: HashMap<String, Column>,
    pub indexes: HashSet<u16>,
}

#[derive(Debug, Default, Clone)]
pub struct Column {
    pub column_type: ColumnType,
    pub offset: u64,
    pub is_nullable: bool,
}

impl Column {
    pub fn new(column_type: ColumnType, offset: u64, is_null: bool) -> Self {
        Self {
            column_type,
            offset,
            is_nullable: is_null,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum ColumnType {
    #[default]
    UnSupported = 0,
    Int,
    Float,
    Bool,
    String,
    Binary,
    DateTime,
}

impl From<&DataType> for ColumnType {
    fn from(item: &DataType) -> Self {
        match item {
            DataType::Varchar(Some(val)) => Self::String,
            DataType::Bool => Self::Bool,
            DataType::Int(val) => Self::Int,
            DataType::Float(Some(val)) => Self::Float,
            DataType::Datetime(Some(val)) => Self::DateTime,
            DataType::Binary(Some(val)) => Self::Binary,
            _ => Self::UnSupported,
        }
    }
}

pub struct Offset(u64);

impl Offset {
    pub fn get_value(&self) -> u64 {
        self.0
    }
}

impl From<&DataType> for Offset {
    fn from(item: &DataType) -> Self {
        match item {
            DataType::Varchar(Some(val)) => Offset(8),
            DataType::Bool => Offset(1),
            DataType::Int(val) => Offset(4),
            _ => Offset(0),
        }
    }
}

impl ColumnType {
    pub(crate) fn from_bytes(c_type: u8) -> ColumnType {
        match c_type {
            0 => Self::UnSupported,
            1 => Self::Int,
            2 => Self::Float,
            3 => Self::Bool,
            4 => Self::String,
            5 => Self::Binary,
            6 => Self::DateTime,
            _ => {
                panic!()
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct Session {
    current_db: String,
    current_user: String,
}

#[derive(Debug, Default)]
pub struct NaadanCatalog {
    pub databases: HashMap<String, Database>,
    pub users: Vec<String>,
    pub session: Session,
}

impl NaadanCatalog {
    pub fn get_table(self: &Self, name: &String) -> Result<&Table, bool> {
        let db = self.databases.get(&self.session.current_db).unwrap();
        Ok(db.tables.get(name).unwrap())
    }
}
