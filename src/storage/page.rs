use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{Cursor, Read, Seek, Write},
    ops::Shl,
};

use log::debug;
use serde::{Deserialize, Serialize};
use sqlparser::ast::Values;

use super::{
    catalog::{Column, ColumnType, Table},
    utils::read_string_from_buf,
    StorageEngineError,
};

use crate::{helper::log, storage::utils::write_string_to_buf};

const DB_DATAFILE: &str = "/tmp/DB_data_file.bin";
const DB_TABLE_CATALOG_FILE: &str = "/tmp/DB_table_catalog_file.bin";
const DB_TABLE_DATA_FILE: &str = "/tmp/DB_table_{table_id}_file.bin";

pub(crate) type Id = u32;
pub(crate) type Offset = u32;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageHeader {
    pub(crate) checksum: [u8; 4],
    pub(crate) extra: [u8; 4],
    pub(crate) offset: HashMap<Id, Offset>,
    pub(crate) last_offset: Offset,
    pub(crate) last_var_len_offset: Offset,
    pub(crate) page_capacity: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowData {
    /// Provides the offset to the values inside data field.
    ///
    /// We can also store the is_null flag inside the most significant bit of the Offset value.
    /// that will make (1 null_bit)+(2^31 offset_bits), though we might not need 31bit(2GB) of offset.
    ///
    pub offset: HashMap<Id, Offset>,

    /// The page row data field.
    /// will have dynamic values based on the table schema and other details,
    ///
    pub data: Vec<u8>,
}

pub trait CatalogPage {
    //fn init();

    fn get_table_details(&self, name: &String) -> Result<Table, StorageEngineError>;

    fn write_table_details(&mut self, table: &Table) -> Result<usize, StorageEngineError>;

    // fn get_db_details(&self, name: &String) -> Vec<u32>;

    // fn get_db_names() -> Vec<String>;
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
/// ||T-Name|T-ID|T-Column-List-Offset|| ..................|
/// |------------------------------------------------------|
/// |........2KB...........||T-Name|T-ID|T-Row-List-Offset||     
/// |------------------------------------------------------|
/// |<--------- Dynamic T-Column-List-Offset ------------->|
/// |------------------------------------------------------|
/// ||T1_Col_count|16it Col1-Type|Col1-Name| ..............|                              
/// |------------------------------------------------------|
/// |......|16it Col{T1_C_count}-Type|Col{T1_C_count}-Name||
/// |------------------------------------------------------|
/// |......|16it Col{TN_C_count}-Type|Col{TN_C_count}-Name||
/// |------------------------------------------------------|
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Page {
    pub header: PageHeader,
    pub data: HashMap<u32, RowData>,
}

impl CatalogPage for Page {
    fn get_table_details(&self, name: &String) -> Result<Table, StorageEngineError> {
        let split_offset: u64 = self.header.page_capacity as u64 * 1 / 4;
        let data = match self.data.contains_key(&0) {
            true => self.data.get(&0).unwrap(),
            false => return Err(StorageEngineError::TableNotFound),
        };
        let mut buf_cursor = Cursor::new(&data.data);

        let mut offset_buf = [0u8; 8];
        let mut len_buf = [0u8; 2];

        buf_cursor.read_exact(&mut offset_buf).unwrap();

        let offset = u64::from_be_bytes(offset_buf);
        let mut table_name = read_string_from_buf(&mut buf_cursor, offset, true);

        while table_name != *name && buf_cursor.position() < split_offset {
            if table_name.is_empty() {
                return Err(StorageEngineError::TableNotFound);
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
                let current_pos = buf_cursor.position();
                let c_name = read_string_from_buf(&mut buf_cursor, current_pos, false);

                table.schema.insert(
                    c_name,
                    Column::new(ColumnType::from_bytes((c_type & 0x7fff) as u8), false),
                );
            }

            return Ok(table);
        }
        Err(StorageEngineError::TableNotFound)
    }

    fn write_table_details(&mut self, table: &Table) -> Result<usize, StorageEngineError> {
        let split_offset: u64 = self.header.page_capacity as u64 * 1 / 4;
        let data = match self.data.contains_key(&0) {
            true => self.data.get_mut(&0).unwrap(),
            false => {
                self.data.insert(
                    0,
                    RowData {
                        offset: HashMap::new(),
                        data: vec![],
                    },
                );

                self.data.get_mut(&0).unwrap()
            }
        };

        let mut buf_cursor = Cursor::new(&mut data.data);
        let mut cursor_base = 0 as u64;

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
            return Err(StorageEngineError::TableAddFailed);
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
                .write_all(&(column.0.len() as u16).to_be_bytes())
                .unwrap();
            buf_cursor.write_all(column.0.as_bytes()).unwrap();
        }

        self.header.offset.insert(1, buf_cursor.position() as u32);

        Ok(buf_cursor.position() as usize)
    }
}

impl Page {
    pub fn new() -> Self {
        Self::new_with_capacity(4 * 1024)
    }
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
            data: HashMap::new(),
        }
    }

    pub fn flush(&self, table_id: usize, expected_page_id: usize) -> Result<(), u32> {
        log(format!("Flushing Data: {:?} to disk.", self));
        let table_data_file =
            String::from(DB_TABLE_DATA_FILE).replace("{table_id}", table_id.to_string().as_str());

        match File::options()
            .read(true)
            .write(true)
            .open(table_data_file.clone())
        {
            Ok(mut file) => {
                let mut buf = vec![0u8; 8 * 1024];
                match file.read_exact(&mut buf) {
                    Ok(_) => {}
                    Err(_) => {
                        file.read(&mut buf).unwrap();
                    }
                }

                let mut buf_cursor = Cursor::new(buf);

                let mut count = 1;

                loop {
                    let mut offset_buf = [0u8; 8];
                    buf_cursor.read_exact(&mut offset_buf).unwrap();
                    let page_id = u64::from_be_bytes(offset_buf);
                    if page_id == 0 {
                        break;
                    } else if page_id == expected_page_id as u64 {
                        break;
                    }
                    count += 1;
                }

                file.seek(std::io::SeekFrom::Start(count * 8 * 1024))
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

                file.seek(std::io::SeekFrom::Start(8 * 1024)).unwrap();
                let write_bytes = bincode::serialize(&self).unwrap();
                file.write_all(write_bytes.as_slice()).unwrap();
                file.flush().unwrap();

                return Ok(());
            }
        }

        Ok(())
    }

    pub fn read_row(&self, row_id: &usize) -> Result<&RowData, bool> {
        log(format!("Read row {} from page", row_id));
        log(format!("{:?}", self));
        let row_offset = self.header.offset.get(&(*row_id as u32)).unwrap();

        log(format!("{}", row_offset));
        let res = self.data.get(row_offset).unwrap();

        log(format!("{:?}", res));

        Ok(res)
    }

    pub fn write_table_row(
        &mut self,
        mut row_id: u32,
        row_data: Values,
        table: &Table,
    ) -> Result<usize, bool> {
        log(format!("Writing row {} to page", row_id));
        // TODO: If there are no variable length attribute in the schema,
        //       then remove the dynamic sized buffer space.
        let var_len_buf_offset: u64 = self.header.page_capacity as u64 * 1 / 4;

        //debug!("Page Data {:?}", self.data);
        let data = match self.data.contains_key(&0) {
            true => self.data.get_mut(&0).unwrap(),
            false => {
                self.data.insert(
                    0,
                    RowData {
                        offset: HashMap::new(),
                        data: vec![],
                    },
                );

                self.data.get_mut(&0).unwrap()
            }
        };

        let mut buf_cursor = Cursor::new(&mut data.data);

        let mut dyn_cursor_base: u64 = self.header.last_var_len_offset as u64;

        if dyn_cursor_base < var_len_buf_offset {
            dyn_cursor_base = var_len_buf_offset;
        }

        buf_cursor.set_position(self.header.last_offset as u64);

        // TODO: check the available space
        for row in row_data.rows {
            for (index, column) in row.iter().enumerate() {
                data.offset
                    .insert(index as u32, buf_cursor.position() as u32);
                match column {
                    sqlparser::ast::Expr::Value(value) => match value {
                        sqlparser::ast::Value::Number(val, _) => {
                            let number = val.parse::<i32>().unwrap();
                            buf_cursor.write_all(&number.to_be_bytes()).unwrap();
                        }
                        sqlparser::ast::Value::SingleQuotedString(val) => {
                            write_string_to_buf(&mut dyn_cursor_base, &mut buf_cursor, val);
                        }
                        sqlparser::ast::Value::DoubleQuotedString(val) => {
                            write_string_to_buf(&mut dyn_cursor_base, &mut buf_cursor, val);
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
                        debug!("Table insert source is not explicit values.");
                        return Err(false);
                    }
                }
            }
            debug!("Done inserting row {}", row_id);
            self.header
                .offset
                .insert(row_id as u32, self.header.last_offset as u32);
            self.header.last_offset = buf_cursor.position() as u32;
            self.header.last_var_len_offset = dyn_cursor_base as u32;
            row_id += 1;
        }

        Ok(200)
    }

    pub fn write_row(&mut self, row_id: &usize, row_data: RowData) -> Result<usize, usize> {
        log(format!("Writing row {} to page", row_id));

        self.header.offset.insert(*row_id as u32, *row_id as u32);
        //log(format!("RowIndex {:?}", self.header.offset));
        self.data.insert(*row_id as u32, row_data);

        Ok(200)
    }

    pub fn read_from_disk(page_id: &usize) -> Result<Page, u32> {
        log(format!("Reading page with id: {} from disk", page_id));

        // TODO: read specific page by offset
        let mut f = File::options().read(true).open(DB_DATAFILE).unwrap();
        f.seek(std::io::SeekFrom::Start(1024 * 4 * (*page_id as u64 - 1)))
            .unwrap();
        let mut buf = [0; 1024 * 4];

        match f.read_exact(&mut buf) {
            Ok(_) => {}
            Err(_) => {
                f.read(&mut buf).unwrap();
            }
        }
        let page: Page = bincode::deserialize_from(&buf[..]).unwrap();
        Ok(page)
    }

    pub fn flush_catalog_page(&self) -> Result<(), StorageEngineError> {
        match File::options().write(true).open(DB_TABLE_CATALOG_FILE) {
            Ok(mut file) => {
                let write_bytes = bincode::serialize(&self).unwrap();
                match file.write_all(write_bytes.as_slice()) {
                    Ok(_) => {}
                    Err(err) => return Err(StorageEngineError::PageFlushFailed),
                }

                Ok(())
            }
            Err(err) => return Err(StorageEngineError::PageFlushFailed),
        }
    }

    pub fn read_catalog_from_disk() -> Result<Page, u32> {
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
                    ("id".to_string(), Column::new(ColumnType::Int, false)),
                    ("name".to_string(), Column::new(ColumnType::String, false)),
                    (
                        "columns".to_string(),
                        Column::new(ColumnType::String, false),
                    ),
                    ("indexes".to_string(), Column::new(ColumnType::Int, false)),
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

    pub fn write_to_disk(&self, page_id: &usize) -> Result<(), u32> {
        log(format!("Writing page with id: {} to disk", page_id));
        log(format!("{:?}", self));
        // Encode to something implementing `Write`
        let mut f = File::options()
            .create(true)
            .write(true)
            .open(DB_DATAFILE)
            .unwrap();
        f.seek(std::io::SeekFrom::Start(1024 * 4 * (*page_id as u64 - 1)))
            .unwrap();
        let write_bytes = bincode::serialize(&self).unwrap();
        f.write_all(write_bytes.as_slice()).unwrap();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::catalog::{Column, ColumnType};
    use crate::storage::page::RowData;
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

    fn get_test_data() -> RowData {
        RowData {
            data: "column1Xcolumn2XintcoulmnXbool1".into(),
            offset: HashMap::from([
                (ASCII_1, ASCII_1),
                (ASCII_1 + 1, ASCII_1 + 7),
                (ASCII_1 + 2, ASCII_1 + 8),
            ]),
        }
    }

    #[test]
    fn read_page_from_disk() {
        fs::remove_file(DB_DATAFILE);
        let mut reg = Region::new(&GLOBAL);
        println!("*** Test Starting <read_page_from_disk> ***\n");

        let mut s_engine = NaadanStorageEngine::init(1);
        //println!("Stats at 1: {:#?}", reg.change());
        let row_data = get_test_data();
        let row2_data = get_test_data();

        let row3_data = get_test_data();

        //println!("Stats at 2: {:#?}", reg.change_and_reset());

        let _ = s_engine.write_row(&0x41, row_data);
        println!("Stats at 4: {:#?}", reg.change());

        let _ = s_engine.write_row(&0x42, row2_data);
        // // println!("Stats at 4: {:#?}", reg.change());

        let _ = s_engine.write_row(&0x45, row3_data.clone());
        // println!("Stats at 4: {:#?}", reg.change());

        // let _ = s_engine.write_row(&0x56, row3_data.clone());

        // for i in 0..2 {
        //     //sleep(Duration::from_millis(1000));
        //     //println!("{}", i);
        //     let _ = s_engine.write_row(&(0x43 + i), row3_data.clone());
        // }

        let _ = s_engine.read_row(0x45);
        println!("Stats at 5: {:#?}", reg.change());
    }

    #[test]
    fn catalog_write() {
        let mut page = Page::new_with_capacity(4 * 1024);

        let mut schema = HashMap::from([
            ("id".to_string(), Column::new(ColumnType::Int, false)),
            ("nam".to_string(), Column::new(ColumnType::String, false)),
            ("pass".to_string(), Column::new(ColumnType::Bool, false)),
            ("ic3".to_string(), Column::new(ColumnType::Float, false)),
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

    #[test]
    fn it_works() {
        fs::remove_file(DB_DATAFILE);
        println!("*** Test Starting ***\n");

        let mut s_engine = NaadanStorageEngine::init(200);
        let mut row_data = get_test_data();

        let mut row_id: usize = 7;
        s_engine.write_row(&row_id, row_data);

        println!("\n Reading 1 \n");
        s_engine.read_row(row_id);

        row_id = 2;
        row_data = get_test_data();
        s_engine.write_row(&row_id, row_data);

        println!("\n Reading 2 \n");
        s_engine.read_row(row_id);

        s_engine.reset_memory();

        println!("\n Reading 3 \n");
        s_engine.read_row(row_id);

        row_id = 3;

        row_data = get_test_data();
        s_engine.write_row(&row_id, row_data);

        row_id = 4;

        row_data = get_test_data();
        s_engine.write_row(&row_id, row_data);

        row_id = 5;

        row_data = get_test_data();
        s_engine.write_row(&row_id, row_data);

        row_id = 6;

        row_data = get_test_data();
        s_engine.write_row(&row_id, row_data);

        println!("\n Reading 4 \n");
        s_engine.read_row(row_id);

        s_engine.reset_memory();

        println!("\n Reading 5 \n");
        s_engine.read_row(row_id);

        println!("\n *** Test Ended ***\n");
    }
}
