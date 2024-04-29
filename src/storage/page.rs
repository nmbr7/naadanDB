use super::{
    catalog::{Column, ColumnType, Table},
    fs::NaadanFile,
    utils::read_string_from_buf,
    NaadanError,
};
use log::debug;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{Expr, Values};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{Cursor, Read, Seek, Write},
    ops::{Shl, Shr},
    vec,
};

use crate::{query::RecordSet, storage::utils::write_string_to_buf, utils::log};

const DB_DATAFILE: &str = "/tmp/DB_data_file.bin";
const DB_TABLE_CATALOG_FILE: &str = "/tmp/DB_table_catalog_file.bin";
const DB_TABLE_DATA_FILE: &str = "/tmp/DB_table_{table_id}_file.bin";

pub(crate) type Id = u32;
pub(crate) type Offset = u32;

pub trait CatalogPage {
    /// Get table count from the DB catalog page
    fn table_count(&self) -> Result<u32, NaadanError>;

    /// Get table details from the DB catalog page
    fn get_table_details(&self, name: &String) -> Result<Table, NaadanError>;

    /// Add table details to DB catalog page
    fn write_table_details(&mut self, table: &Table) -> Result<usize, NaadanError>;

    /// Write the catalog page to disk
    fn write_catalog_to_disk(&self) -> Result<(), NaadanError>;

    /// Read the catalog page from the disk
    fn read_catalog_from_disk() -> Result<Page, NaadanError>;

    // Get DB details from the catalog page
    // fn get_db_details(&self, name: &String) -> Vec<u32>;

    // Get all DB names
    // fn get_db_names() -> Vec<String>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageHeader {
    pub(crate) checksum: [u8; 4],
    pub(crate) extra: [u8; 4],
    pub(crate) offset: HashMap<Id, Offset>,
    pub(crate) last_offset: Offset,
    pub(crate) last_var_len_offset: Offset,
    pub(crate) page_capacity: u32,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PageData {
    /// Provides the offset to the values inside data field.
    ///
    /// We can also store the is_null flag inside the most significant bit of the Offset value.
    /// that will make (1 null_bit)+(2^31 offset_bits), though we might not need 31bit(2GB) of offset.
    pub offset: HashMap<Id, Offset>,

    /// The page row data field.
    /// will have dynamic values based on the table schema and other details,
    pub data: Vec<u8>,
}

/// ```
///             PAGE STRUCTURE FOR TABLE DATA
///
/// |  <---------- HEADER  --------->        | <-- DATA ---|
/// |------------------------------------------------------|
/// | checksum | extra | offset | last_offset|    ROW-1    |
/// |------------------------------------------------------|
/// | ROW-2 | ROW-3 | ...... | ROW-N |                     |
/// |------------------------------------------------------|
/// |                                                      |
/// |------------------------------------------------------|
///
///         PAGE STRUCTURE FOR DB CATALOG (DB) 4KB
///
/// |  <---------- HEADER  --------->        | <-- DATA ---|
/// |------------------------------------------------------|
/// | checksum | extra | offset | last_offset|  .......... |
/// |------------------------------------------------------|
/// | ................ DB-Section ........................ |
/// |------------------------------------------------------|
/// ||DB_1-Name|Table-ID-List-Offset|| DB_2-Name|Table-ID..|
/// |------------------------------------------------------|
/// |..List-Offset||..2KB..||DB_N-Name|Table-ID-List-Offset|     
/// |------------------------------------------------------|
/// |.............. Dynamic Table-ID List .................|
/// |------------------------------------------------------|
/// ||Table_Count| 32bit ID-1| ....... |32bit ID-{T_count}||                              
/// |------------------------------------------------------|
///
///          PAGE STRUCTURE FOR DB CATALOG (Table) 4KB
///
/// |  <---------- HEADER  --------->        | <-- DATA ---|
/// |------------------------------------------------------|
/// | checksum | extra | offset | last_offset|  .......... |
/// |------------------------------------------------------|
/// | <---------------- Table-Section -------------------->|
/// |------------------------------------------------------|
/// |TCount||T-Name offset|T-ID|T-Column-List-Offset|| ....|
/// |------------------------------------------------------|
/// |........2KB...........||T-Name|T-ID|T-Row-List-Offset||     
/// |------------------------------------------------------|
/// |<---------------- Dynamic Sized Section ------------->|
/// |------------------------------------------------------|
/// ||TName||T1_Col_count|16it Col1-Type|Col1-Name| .......|                              
/// |------------------------------------------------------|
/// |......|16it Col{T1_C_count}-Type|Col{T1_C_count}-Name||
/// |------------------------------------------------------|
/// |......|16it Col{TN_C_count}-Type|Col{TN_C_count}-Name||
/// |------------------------------------------------------|
/// ```
/// Note: Need to update the above diagrams (Might be out dated)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Page {
    pub header: PageHeader,
    pub data: PageData,
}

impl CatalogPage for Page {
    fn table_count(&self) -> Result<u32, NaadanError> {
        let data = &self.data;
        let mut buf_cursor = Cursor::new(&data.data);

        let mut table_count_buf = [0u8; 4];
        buf_cursor.read_exact(&mut table_count_buf).unwrap();

        let table_count = u32::from_be_bytes(table_count_buf);

        Ok(table_count)
    }

    fn get_table_details(&self, name: &String) -> Result<Table, NaadanError> {
        let split_offset: u64 = self.header.page_capacity as u64 * 1 / 4;
        let data = &self.data;
        let mut buf_cursor = Cursor::new(&data.data);

        let mut table_count_buf = [0u8; 4];
        buf_cursor.read_exact(&mut table_count_buf).unwrap();

        let mut offset_buf = [0u8; 8];
        let mut len_buf = [0u8; 2];

        buf_cursor.read_exact(&mut offset_buf).unwrap();

        let offset = u64::from_be_bytes(offset_buf);
        let mut table_name = read_string_from_buf(&mut buf_cursor, offset, true);

        while table_name != *name && buf_cursor.position() < split_offset {
            if table_name.is_empty() {
                return Err(NaadanError::TableNotFound);
            }
            buf_cursor.set_position(buf_cursor.position() + 10);
            buf_cursor.read_exact(&mut offset_buf).unwrap();

            let offset = u64::from_be_bytes(offset_buf);
            table_name = read_string_from_buf(&mut buf_cursor, offset, true);
        }

        if table_name == *name {
            let mut data_buf = [0u8; 2];
            buf_cursor.read_exact(&mut data_buf).unwrap();
            let table_id = u16::from_be_bytes(data_buf);

            buf_cursor.read_exact(&mut offset_buf).unwrap();
            let offset = u64::from_be_bytes(offset_buf);
            buf_cursor.set_position(offset);

            buf_cursor.read_exact(&mut len_buf).unwrap();
            let len = u16::from_be_bytes(len_buf) & (0xffff - 0xf400);

            let mut table = Table {
                id: table_id,
                indexes: HashSet::new(),
                name: table_name,
                schema: HashMap::new(),
            };
            for _ in 0..len {
                buf_cursor.read_exact(&mut data_buf).unwrap();
                let c_type = u16::from_be_bytes(data_buf);

                buf_cursor.read_exact(&mut offset_buf).unwrap();
                let col_offset = u64::from_be_bytes(offset_buf);

                let current_pos = buf_cursor.position();
                let c_name = read_string_from_buf(&mut buf_cursor, current_pos, false);

                table.schema.insert(
                    c_name,
                    Column::new(
                        ColumnType::from_bytes((c_type & 0x7fff) as u8),
                        col_offset,
                        false,
                    ),
                );
            }

            return Ok(table);
        }
        Err(NaadanError::TableNotFound)
    }

    fn write_table_details(&mut self, table: &Table) -> Result<usize, NaadanError> {
        println!("Creating new table with details {:?}", table);
        let split_offset: u64 = self.header.page_capacity as u64 * 1 / 4;
        let data = &mut self.data;

        let mut buf_cursor = Cursor::new(&mut data.data);

        let table_count = table.id as u32;
        // Write table count
        buf_cursor.write_all(&table_count.to_be_bytes()).unwrap();

        let mut cursor_base = 4 as u64;

        match self.header.offset.get(&0) {
            Some(&val) => {
                cursor_base = val as u64;
            }
            None => {}
        }

        buf_cursor
            .seek(std::io::SeekFrom::Start(cursor_base))
            .unwrap();

        let mut dyn_cursor_base = split_offset;

        match self.header.offset.get(&1) {
            Some(&val) => {
                dyn_cursor_base = val as u64;
            }
            None => {}
        }

        if cursor_base + 18 >= split_offset
            || (dyn_cursor_base + table.name.len() as u64 + 2 + table.schema.len() as u64 * 4 * 64)
                >= self.header.page_capacity as u64
        {
            println!("Buffer full cursor at {}", cursor_base);
            return Err(NaadanError::TableAddFailed);
        }

        let offset = dyn_cursor_base.to_be_bytes();

        buf_cursor.write_all(&offset).unwrap();

        // Seek to table column list id offset start location at 1024 * 2 bytes
        buf_cursor
            .seek(std::io::SeekFrom::Start(dyn_cursor_base as u64))
            .unwrap();

        // write table name length (max is 128) in 2 byte
        buf_cursor
            .write_all(&(table.name.len() as u16).to_be_bytes())
            .unwrap();

        // Write table name (max len is 128)
        buf_cursor.write_all(table.name.as_bytes()).unwrap();

        dyn_cursor_base += table.name.len() as u64 + 2;

        buf_cursor
            .seek(std::io::SeekFrom::Start(cursor_base + 8 as u64))
            .unwrap();

        // Write table id in 4 byte (32bit)
        buf_cursor.write_all(&table.id.to_be_bytes()).unwrap();

        // Write table column list id offset in 4 byte (32bit)
        buf_cursor
            .write_all(&dyn_cursor_base.to_be_bytes())
            .unwrap();

        self.header.offset.insert(0, buf_cursor.position() as u32);

        // Seek to table column list id offset start location at 1024 * 2 bytes
        buf_cursor
            .seek(std::io::SeekFrom::Start(dyn_cursor_base as u64))
            .unwrap();

        // Write table column len, max is 1024 in sql standard (we have 2 byte space availabe)
        // it can be used for other purposes.
        buf_cursor
            .write_all(&(table.schema.len() as u16 | (0xf400)).to_be_bytes())
            .unwrap();

        for column in &table.schema {
            let is_null = (column.1.is_nullable as u16).shl(15) as u16;
            let column_type = column.1.column_type.clone() as u8 as u16;

            buf_cursor
                .write_all(&(column_type | is_null).to_be_bytes())
                .unwrap();

            buf_cursor
                .write_all(&(column.1.offset as u64).to_be_bytes())
                .unwrap();

            buf_cursor
                .write_all(&(column.0.len() as u16).to_be_bytes())
                .unwrap();
            buf_cursor.write_all(column.0.as_bytes()).unwrap();
        }

        self.header.offset.insert(1, buf_cursor.position() as u32);

        Ok(buf_cursor.position() as usize)
    }

    fn write_catalog_to_disk(&self) -> Result<(), NaadanError> {
        match File::options().write(true).open(DB_TABLE_CATALOG_FILE) {
            Ok(mut file) => {
                let write_bytes = bincode::serialize(&self).unwrap();
                match file.write_all(write_bytes.as_slice()) {
                    Ok(_) => {}
                    Err(err) => return Err(NaadanError::PageFlushFailed),
                }

                Ok(())
            }
            Err(err) => return Err(NaadanError::PageFlushFailed),
        }
    }

    fn read_catalog_from_disk() -> Result<Page, NaadanError> {
        match File::options().read(true).open(DB_TABLE_CATALOG_FILE) {
            Ok(mut file) => {
                let mut buf = vec![0u8; 4 * 1024];
                match file.read_exact(&mut buf) {
                    Ok(_) => {}
                    Err(_) => {
                        file.read(&mut buf).unwrap();
                    }
                }
                let page: Page = bincode::deserialize_from(&buf[..]).unwrap();
                Ok(page)
            }
            Err(_) => {
                let mut file = File::options()
                    .create(true)
                    .write(true)
                    .open(DB_TABLE_CATALOG_FILE)
                    .unwrap();

                let mut page = Page::new();
                let schema = HashMap::from([
                    ("id".to_string(), Column::new(ColumnType::Int, 0, false)),
                    (
                        "name".to_string(),
                        Column::new(ColumnType::String, 0, false),
                    ),
                    (
                        "columns".to_string(),
                        Column::new(ColumnType::String, 0, false),
                    ),
                    (
                        "indexes".to_string(),
                        Column::new(ColumnType::Int, 0, false),
                    ),
                ]);

                let table = Table {
                    name: "schema_info".to_string(),
                    id: 1 as u16,
                    schema: schema,
                    indexes: HashSet::new(),
                };

                page.write_table_details(&table).unwrap();

                let write_bytes = bincode::serialize(&page).unwrap();
                file.write_all(write_bytes.as_slice()).unwrap();

                Ok(page)
            }
        }
    }
}

impl Page {
    /// Create new page instance with default capacity
    pub fn new() -> Self {
        Self::new_with_capacity(4 * 1024)
    }

    /// Create new page instance with the provided capacity
    pub fn new_with_capacity(capacity: u32) -> Self {
        let page_header = PageHeader {
            extra: [0; 4],
            checksum: [0; 4],
            offset: HashMap::new(),
            last_offset: 0,
            last_var_len_offset: 0,
            page_capacity: capacity,
        };

        Self {
            header: page_header,
            data: PageData::default(),
        }
    }

    /// Write a new row into the table page
    pub fn write_table_row(
        &mut self,
        mut row_id: u32,
        row_data: RecordSet,
        schema: &Table,
    ) -> Result<usize, NaadanError> {
        log(format!("Writing row {} to page", row_id));
        // TODO: If there are no variable length attribute in the schema,
        //       then remove the dynamic sized buffer space.
        let var_len_buf_offset: u64 = self.header.page_capacity as u64 * 1 / 4;

        //println!("Page Data {:?}", self.data);
        let data = &mut self.data;

        let mut buf_cursor = Cursor::new(&mut data.data);
        let mut dyn_cursor_base: u64 = self.header.last_var_len_offset as u64;

        if dyn_cursor_base < var_len_buf_offset {
            dyn_cursor_base = var_len_buf_offset;
        }

        buf_cursor.set_position(self.header.last_offset as u64);

        // TODO: check the available space
        for row in row_data {
            for (index, column) in row.iter().enumerate() {
                data.offset
                    .insert(index as u32, buf_cursor.position() as u32);
                if let Some(value) =
                    write_column_value(column, &mut buf_cursor, &mut dyn_cursor_base)
                {
                    return value;
                }
            }
            println!("Done inserting row {}", row_id);

            self.header
                .offset
                .insert(row_id as u32, self.header.last_offset as u32);
            self.header.last_offset = buf_cursor.position() as u32;
            self.header.last_var_len_offset = dyn_cursor_base as u32;
            row_id += 1;
        }

        Ok(row_id as usize)
    }

    /// Read a row from the table page
    pub fn read_table_row(&self, row_id: &u64, table: &Table) -> Result<Vec<Expr>, NaadanError> {
        log(format!("Read row {} from page", row_id));
        let row_offset = *self.header.offset.get(&(*row_id as u32)).unwrap() as u64;

        let data = &self.data;
        let mut buf_cursor = Cursor::new(&data.data);
        buf_cursor.set_position(row_offset as u64);

        let mut row: Vec<(String, Expr)> = vec![];
        for (c_name, c_type) in &table.schema {
            let read_offset = row_offset + c_type.offset;

            buf_cursor.set_position(read_offset);
            read_column_value(c_type, c_name, &mut buf_cursor, &mut row);
        }

        Ok(row.iter().map(|val| val.1.clone()).collect::<Vec<Expr>>())
    }

    /// Update a row in the table page
    pub fn update_table_row(
        &mut self,
        row_id: &u64,
        updated_columns: &HashMap<String, Expr>,
        table: &Table,
        //row_handler: F,
    ) -> Result<(), NaadanError>
// where
    //     F: FnOnce(&Expr) -> Result<R, NaadanError>,
    {
        log(format!("Read row {} from page", row_id));
        let row_offset = *self.header.offset.get(&(*row_id as u32)).unwrap() as u64;

        let data = &mut self.data;
        let mut buf_cursor = Cursor::new(&mut data.data);
        buf_cursor.set_position(row_offset);

        let var_len_buf_offset: u64 = self.header.page_capacity as u64 * 1 / 4;

        let mut dyn_cursor_base: u64 = self.header.last_var_len_offset as u64;

        if dyn_cursor_base < var_len_buf_offset {
            dyn_cursor_base = var_len_buf_offset;
        }

        for (col_name, col_value) in updated_columns {
            let col_offset = table.schema.get(col_name).unwrap().offset;
            let update_offset = row_offset + col_offset;

            buf_cursor.set_position(update_offset);

            //row_handler(col_value);
            write_column_value(&col_value, &mut buf_cursor, &mut dyn_cursor_base);
        }

        self.header.last_var_len_offset = dyn_cursor_base as u32;

        Ok(())
    }

    /// Write a page to disk
    pub fn write_to_disk(&self, page_id: PageId) -> Result<(), NaadanError> {
        log(format!("Flushing Data: {:?} to disk.", self));
        let table_data_file = String::from(DB_TABLE_DATA_FILE)
            .replace("{table_id}", page_id.get_table_id().to_string().as_str());

        match File::options()
            .read(true)
            .write(true)
            .open(table_data_file.clone())
        {
            Ok(mut file) => {
                println!(
                    "Page Index is :{}\nPage offset is at {}",
                    page_id.get_page_index(),
                    page_id.get_page_index() as u64 * 8 * 1024
                );
                file.seek(std::io::SeekFrom::Start(
                    page_id.get_page_index() as u64 * 8 * 1024,
                ))
                .unwrap();
                let write_bytes = bincode::serialize(&self).unwrap();
                file.write_all(write_bytes.as_slice()).unwrap();
                file.flush().unwrap();

                return Ok(());
            }
            Err(_) => {
                let mut file = File::options()
                    .create(true)
                    .write(true)
                    .open(table_data_file)
                    .unwrap();

                let write_bytes = bincode::serialize(&self).unwrap();
                file.write_all(write_bytes.as_slice()).unwrap();
                file.flush().unwrap();

                return Ok(());
            }
        }
    }

    /// Read a page from disk
    pub fn read_from_disk(page_id: PageId) -> Result<Page, NaadanError> {
        log(format!(
            "Reading page with id: {} from disk",
            page_id.get_page_id()
        ));

        let table_data_file = String::from(DB_TABLE_DATA_FILE)
            .replace("{table_id}", page_id.get_table_id().to_string().as_str());

        let mut file = NaadanFile::read(&table_data_file).unwrap();
        file.seek_from_start(page_id.get_page_index() as u64 * 8 * 1024)
            .unwrap();

        let mut buf = [0; 8 * 1024];

        match file.read_to_buf(&mut buf) {
            Ok(_) => {}
            Err(err) => return Err(err),
        }

        let page: Page = bincode::deserialize_from(&buf[..]).unwrap();
        Ok(page)
    }
}

fn read_column_value(
    c_type: &Column,
    c_name: &String,
    buf_cursor: &mut Cursor<&Vec<u8>>,
    row: &mut Vec<(String, Expr)>,
) {
    match c_type.column_type {
        ColumnType::UnSupported => {}
        ColumnType::Int => {
            let mut buf = [0u8; 4];
            buf_cursor.read_exact(&mut buf).unwrap();
            let number = i32::from_be_bytes(buf);

            row.push((
                c_name.to_string(),
                Expr::Value(sqlparser::ast::Value::Number(
                    number.to_string(),
                    c_type.is_nullable,
                )),
            ))
        }
        ColumnType::Float => {}
        ColumnType::Bool => {
            let mut buf = [0u8; 1];
            buf_cursor.read_exact(&mut buf).unwrap();
            let bool_val = match buf {
                [0x1] => true,
                _ => false,
            };

            row.push((
                c_name.to_string(),
                Expr::Value(sqlparser::ast::Value::Boolean(bool_val)),
            ))
        }
        ColumnType::String => {
            let mut offset_buf = [0u8; 8];
            buf_cursor.read_exact(&mut offset_buf).unwrap();

            let offset = u64::from_be_bytes(offset_buf);

            let str_val = read_string_from_buf(buf_cursor, offset, true);

            row.push((
                c_name.to_string(),
                Expr::Value(sqlparser::ast::Value::SingleQuotedString(str_val)),
            ))
        }
        ColumnType::Binary => {}
        ColumnType::DateTime => {}
    }
}

fn write_column_value(
    column: &Expr,
    buf_cursor: &mut Cursor<&mut Vec<u8>>,
    dyn_cursor_base: &mut u64,
) -> Option<Result<usize, NaadanError>> {
    match column {
        sqlparser::ast::Expr::Value(value) => match value {
            sqlparser::ast::Value::Number(val, _) => {
                let number = val.parse::<i32>().unwrap();
                buf_cursor.write_all(&number.to_be_bytes()).unwrap();
            }
            sqlparser::ast::Value::SingleQuotedString(val) => {
                write_string_to_buf(dyn_cursor_base, buf_cursor, val);
            }
            sqlparser::ast::Value::DoubleQuotedString(val) => {
                write_string_to_buf(dyn_cursor_base, buf_cursor, val);
            }
            sqlparser::ast::Value::Boolean(val) => {
                let b_val: &[u8] = match val {
                    true => &[0b1],
                    false => &[0b0],
                };

                buf_cursor.write_all(b_val).unwrap();
            }
            sqlparser::ast::Value::Null => {
                // Null is set a 8 bytes for now, need to be allocated based
                // on the schema and column index in the input row
                buf_cursor.write_all(&(0 as u64).to_be_bytes()).unwrap();
            }
            _ => {}
        },
        _ => {
            println!("Table insert source is not explicit values.");
            return Some(Err(NaadanError::RowAddFailed));
        }
    }
    None
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageId(pub u64);

impl PageId {
    pub fn new(table_id: u16, page_index: u32) -> Self {
        let page_id = (table_id as u64).shl(48) | page_index as u64;

        Self(page_id)
    }
    pub fn get_table_id(&self) -> u16 {
        let table_id = self.0.shr(48) & 0xff_ff as u64;

        table_id as u16
    }

    pub fn get_page_id(&self) -> u64 {
        self.0
    }

    pub fn get_page_index(&self) -> u32 {
        let index = self.0 & 0x00_00_00_00_ff_ff_ff_ff as u64;

        index as u32 - 1
    }
}
#[cfg(test)]
mod tests {
    use crate::storage::catalog::{Column, ColumnType};
    use crate::storage::page::PageData;
    use crate::storage::storage_engine::NaadanStorageEngine;
    use crate::storage::StorageEngine;

    use super::*;
    extern crate stats_alloc;

    const ASCII_U8_1: u8 = '1' as u8;
    const ASCII_1: u32 = ASCII_U8_1 as u32;

    use stats_alloc::{Region, StatsAlloc, INSTRUMENTED_SYSTEM};
    use std::alloc::System;
    use std::collections::HashSet;
    use std::fs;

    #[global_allocator]
    static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

    fn get_test_data() -> PageData {
        PageData {
            data: "column1Xcolumn2XintcoulmnXbool1".into(),
            offset: HashMap::from([
                (ASCII_1, ASCII_1),
                (ASCII_1 + 1, ASCII_1 + 7),
                (ASCII_1 + 2, ASCII_1 + 8),
            ]),
        }
    }

    #[test]
    fn catalog_write() {
        let mut page = Page::new_with_capacity(4 * 1024);

        let mut schema = HashMap::from([
            ("id".to_string(), Column::new(ColumnType::Int, 0, false)),
            ("nam".to_string(), Column::new(ColumnType::String, 0, false)),
            ("pass".to_string(), Column::new(ColumnType::Bool, 0, false)),
            ("ic3".to_string(), Column::new(ColumnType::Float, 0, false)),
        ]);

        for i in 1..2 as u32 {
            let table = Table {
                name: "user".to_string(),
                id: 60 as u16,
                schema: schema.clone(),
                indexes: HashSet::new(),
            };

            page.write_table_details(&table).unwrap();

            schema.retain(|_k, v| v.column_type == ColumnType::String);

            let table = Table {
                name: "class".to_string(),
                id: 60 as u16,
                schema: schema.clone(),
                indexes: HashSet::new(),
            };

            match page.write_table_details(&table) {
                Ok(size) => {
                    //println!("Data buffer: {:?}", page.data.get(&0).unwrap());
                    // if i % 100000 == 0 {
                    println!("Wrote table {} details at Offset: {}", i, size);
                    // }
                }
                Err(_) => break,
            }
        }

        let t_data = page.get_table_details(&"class".to_string());

        match t_data {
            Ok(data) => println!("Table data {:?}", data),
            Err(_) => println!("Table not found"),
        }
    }
}
