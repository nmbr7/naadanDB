use core::panic;
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
    pub is_null: bool,
}

impl Column {
    pub fn new(column_type: ColumnType, is_null: bool) -> Self {
        Self {
            column_type,
            is_null,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum ColumnType {
    #[default]
    Int = 0,
    Float,
    Bool,
    String,
    Binary,
    DateTime,
}

impl ColumnType {
    pub(crate) fn from_bytes(c_type: u8) -> ColumnType {
        match c_type {
            0 => Self::Int,
            1 => Self::Float,
            2 => Self::Bool,
            3 => Self::String,
            4 => Self::Binary,
            5 => Self::DateTime,
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
