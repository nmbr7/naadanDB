use serde::{Deserialize, Serialize};
use sqlparser::ast::Table;

use crate::{
    catalog::{self, Column, ColumnType, NaadanCatalog, Session},
    utils::log,
};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufWriter, Bytes, Cursor, Read, Seek, Write},
    mem,
    ops::{Shl, ShlAssign},
    string,
};

const DB_DATAFILE: &str = "/tmp/DB_data_file.bin";
const DB_TABLE_CATALOG_FILE: &str = "/tmp/DB_table_catalog_file.bin";

#[derive(PartialEq, Eq)]
enum WriteType {
    Insert,
    Update,
    Delete,
}

pub trait StorageEngine: std::fmt::Debug {
    fn get_table_details(&self, name: &String) -> Result<catalog::Table, bool>;

    fn read_row(&mut self, row_id: usize) -> Result<&RowData, bool>;

    fn read_rows(&mut self, row_id: &[usize]) -> Result<Vec<&RowData>, bool>;

    fn write_row(&mut self, row_id: &usize, row_data: RowData) -> Result<usize, bool>;

    fn write_rows(&mut self, row_data: Vec<Vec<u8>>) -> Result<Vec<RowData>, bool>;

    fn reset_memory(&mut self);
}

// Storage Engine implementation
#[derive(Debug)]
pub struct OurStorageEngine {
    catalog_page: HashMap<usize, Page>,
    buffer_pool: BufferPool,
    row_index: HashMap<usize, usize>,
    engine_metadata_store: Vec<usize>,
}
impl OurStorageEngine {
    pub fn init(page_count: usize) -> Self {
        // TODO: load catalog data
        let buffer_pool = BufferPool {
            page_pool: HashMap::with_capacity(page_count),
            buffer_metadata: PoolMetadata::default(),
        };
        let catalog_page = Page::read_catalog_from_disk().unwrap();
        let row_index = HashMap::new();

        Self {
            buffer_pool,
            row_index,
            engine_metadata_store: vec![],
            catalog_page: HashMap::from([(1, catalog_page)]),
        }
    }

    fn write_to_existing_page(
        &mut self,
        free_page: FreePage,
        row_id: &usize,
        row_data: RowData,
        row_size: usize,
        mode: WriteType,
    ) {
        match self.buffer_pool.get_mut(free_page.page_id()) {
            Ok(page) => {
                log(format!("Reading page is present in buffer pool!!"));

                page.write_row(row_id, row_data).unwrap();
                let page_id = free_page.page_id();
                let _ = page.write_to_disk(page_id);
                if mode == WriteType::Insert {
                    self.buffer_pool
                        .update_available_page_size(*page_id, free_page.1 - row_size);
                }
                self.row_index.insert(*row_id, *free_page.page_id());
                log(format!("RowIndex: {:?}", self.row_index));
            }
            Err(_) => {
                log(format!("Reading page from the disk!!"));
                // Page is not present in the buffer pool, need to fetch it from the read_from_disk
                match Page::read_from_disk(&free_page.page_id()) {
                    Ok(page) => {
                        let new_page = self.buffer_pool.add(free_page.page_id(), page).unwrap();

                        let _ = new_page.write_row(row_id, row_data);
                        let _ = new_page.write_to_disk(&free_page.page_id());

                        if mode == WriteType::Insert {
                            self.buffer_pool.update_available_page_size(
                                *free_page.page_id(),
                                free_page.1 - row_size,
                            );
                        }

                        self.row_index.insert(*row_id, *free_page.page_id());
                        log(format!("RowIndex: {:?}", self.row_index));
                    }
                    Err(_err) => {
                        unreachable!();
                    }
                };
            }
        };
    }

    fn write_to_page(
        &mut self,
        free_page: Option<FreePage>,
        row_id: &usize,
        row_data: RowData,
        row_size: usize,
        mode: WriteType,
    ) {
        match free_page {
            None => {
                log(format!("Creating new page."));

                let page_id = self.buffer_pool.buffer_metadata.free_pages.len() + 1 as usize;
                let mut page = Page::new();

                let _ = page.write_row(row_id, row_data);
                let _ = page.write_to_disk(&page_id);
                self.buffer_pool.add(&page_id, page).unwrap();
                self.buffer_pool
                    .update_available_page_size(page_id, 4 * 1024 - row_size);
                self.row_index.insert(*row_id, page_id);
                log(format!("RowIndex: {:?}", self.row_index));
            }
            Some(free_page) => {
                self.write_to_existing_page(free_page, row_id, row_data, row_size, mode);
            }
        };
    }
}

impl StorageEngine for OurStorageEngine {
    fn get_table_details(&self, name: &String) -> Result<catalog::Table, bool> {
        self.catalog_page.get(&1).unwrap().get_table_details(name)
    }

    fn reset_memory(&mut self) {
        log(format!("Resetting StorageEngine memory !!"));
        self.buffer_pool.page_pool.clear();
    }

    fn read_row(&mut self, row_id: usize) -> Result<&RowData, bool> {
        let index_result = self.row_index.get(&row_id);
        match index_result {
            Some(page_id) => {
                log(format!("RowId {} is in Page {}.", row_id, page_id));
                if self.buffer_pool.page_exist(page_id) {
                    // Page present in the buffer pool.
                    let page = self.buffer_pool.get(page_id).unwrap();
                    log(format!("Reading page {} from buffer pool.", page_id));
                    return page.read_row(&row_id);
                } else {
                    // Page is not present in the buffer pool, need to fetch it from the disk
                    log(format!("Reading page {} from disk.", page_id));
                    match Page::read_from_disk(&page_id) {
                        Ok(page) => {
                            let new_page = self.buffer_pool.add(page_id, page);
                            return new_page.unwrap().read_row(&row_id);
                        }
                        Err(err) => {
                            log(format!("Fetching latest page gave error: {}", err));
                            return Err(false);
                        }
                    };
                }
            }
            None => {}
        }

        Err(false)
    }

    fn read_rows(&mut self, row_id: &[usize]) -> Result<Vec<&RowData>, bool> {
        Err(false)
    }

    fn write_row(&mut self, row_id: &usize, row_data: RowData) -> Result<usize, bool> {
        println!("\n\n");
        let row_size = 500 * 3;
        assert!(row_size < (4 * 1024) as usize, "Invalid row size");
        let index_result = self.row_index.get(&row_id);
        match index_result {
            Some(page_id) => {
                let free_page = self.buffer_pool.get_available_page(&page_id);
                self.write_to_page(free_page, row_id, row_data, row_size, WriteType::Update);
            }
            None => {
                // Check if any Page is having space
                let free_page = self.buffer_pool.get_any_available_page(&row_size);
                self.write_to_page(free_page, row_id, row_data, row_size, WriteType::Insert);
            }
        }

        Ok(200)
    }

    fn write_rows(&mut self, row_data: Vec<Vec<u8>>) -> Result<Vec<RowData>, bool> {
        Err(false)
    }
}

type Id = u32;
type Offset = u32;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageHeader {
    checksum: [u8; 4],
    extra: [u8; 4],
    offset: HashMap<Id, Offset>,
    last_offset: Offset,
    page_capacity: u32,
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

trait CatalogPage {
    fn init();

    fn get_table_details(&self, name: &String) -> Result<catalog::Table, bool>;

    fn write_table_details(&mut self, table: &catalog::Table) -> Result<usize, bool>;

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
    fn init() {
        todo!()
    }

    fn get_table_details(&self, name: &String) -> Result<catalog::Table, bool> {
        let split_offset: u64 = self.header.page_capacity as u64 * 1 / 4;
        let data = match self.data.contains_key(&0) {
            true => self.data.get(&0).unwrap(),
            false => return Err(false),
        };
        let mut buf_cursor = Cursor::new(&data.data);

        let mut offset_buf = [0u8; 8];
        let mut len_buf = [0u8; 2];

        buf_cursor.read_exact(&mut offset_buf).unwrap();

        let offset = u64::from_be_bytes(offset_buf);
        let mut table_name = read_string_from_buf(&mut buf_cursor, offset, true);

        while table_name != *name && buf_cursor.position() < split_offset {
            if table_name.is_empty() {
                return Err(false);
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

            let mut table = catalog::Table {
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
                    catalog::Column::new(ColumnType::from_bytes((c_type & 0x7fff) as u8), false),
                );
            }

            return Ok(table);
        }

        println!("Table 1 : {:?}", table_name);

        Err(false)
    }

    fn write_table_details(&mut self, table: &catalog::Table) -> Result<usize, bool> {
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
            return Err(false);
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
            let is_null = (column.1.is_null as u16).shl(15) as u16;
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

fn read_string_from_buf(
    buf_cursor: &mut Cursor<&Vec<u8>>,
    offset: u64,
    reset_offset: bool,
) -> String {
    let mut len_buf = [0u8; 2];
    let current_pos = buf_cursor.position();
    buf_cursor.set_position(offset);

    buf_cursor.read_exact(&mut len_buf).unwrap();

    let len = u16::from_be_bytes(len_buf);

    let mut data_buf = vec![0; len as usize];

    buf_cursor.read_exact(&mut data_buf).unwrap();

    let table_name = String::from_utf8(data_buf).unwrap();

    if reset_offset {
        buf_cursor.set_position(current_pos);
    }

    table_name
}

impl Page {
    pub fn new() -> Self {
        Self::new_with_capacity(4 * 1024)
    }
    pub fn new_with_capacity(capacity: u32) -> Self {
        let page_header = PageHeader {
            extra: [0; 4],
            checksum: ['X' as u8, 'X' as u8, 'X' as u8, 'X' as u8],
            offset: HashMap::from([('O' as u8 as u32, 'O' as u8 as u32)]),
            last_offset: 0,
            page_capacity: capacity,
        };

        Self {
            header: page_header,
            data: HashMap::new(),
        }
    }

    pub fn flush(&self) -> Result<(), u32> {
        log(format!("Flushing Data: {:?} to disk.", self));
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

                let table = catalog::Table {
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

#[derive(PartialEq, Eq, Debug, Clone, Default)]
struct FreePage(usize, usize);

impl FreePage {
    pub fn page_id(&self) -> &usize {
        &self.0
    }
}

impl PartialOrd for FreePage {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FreePage {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let left = self.1;
        let right = other.1;

        left.cmp(&right)
    }
}

#[derive(Debug, Default)]
pub struct PoolMetadata {
    pub free_pages: Vec<FreePage>,
}

#[derive(Debug)]
pub struct BufferPool {
    pub buffer_metadata: PoolMetadata,
    pub page_pool: HashMap<usize, Page>,
}

impl BufferPool {
    pub fn page_exist(&self, page_id: &usize) -> bool {
        self.page_pool.contains_key(page_id)
    }

    pub fn get_mut(&mut self, page_id: &usize) -> Result<&mut Page, bool> {
        match self.page_pool.get_mut(page_id) {
            Some(page) => Ok(page),
            None => Err(false),
        }
    }

    pub fn get(&self, page_id: &usize) -> Result<&Page, bool> {
        match self.page_pool.get(page_id) {
            Some(page) => Ok(page),
            None => Err(false),
        }
    }

    pub fn add(&mut self, page_id: &usize, page: Page) -> Result<&mut Page, bool> {
        self.buffer_metadata
            .free_pages
            .push(FreePage(*page_id, 1024 * 4));
        self.page_pool.insert(*page_id, page);
        self.get_mut(page_id)
    }

    pub fn get_any_available_page(&mut self, size: &usize) -> Option<FreePage> {
        println!("Get: {:#?}", self.buffer_metadata.free_pages);
        self.buffer_metadata.free_pages.sort();
        match self.buffer_metadata.free_pages.last() {
            Some(fp) if fp.1 > (size + 8 * 8) => Some(fp.clone()),
            _ => None,
        }
    }

    pub fn get_available_page(&self, page_id: &usize) -> Option<FreePage> {
        self.buffer_metadata.free_pages.iter().find_map(|s| {
            if s.0 == *page_id {
                Some(s.clone())
            } else {
                None
            }
        })
    }

    pub fn update_available_page_size(&mut self, page_id: usize, size: usize) -> bool {
        println!("Getin Update: {:#?}", self.buffer_metadata.free_pages);
        let np = self.buffer_metadata.free_pages.pop().unwrap();
        if np.0 == page_id {
            println!("Updating page {} with current {:#?} ", np.0, np.1);
            self.buffer_metadata.free_pages.push(FreePage(np.0, size));
            println!("Updated value: {:#?}", self.buffer_metadata.free_pages);
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use self::catalog::{Column, ColumnType};

    use super::*;
    extern crate stats_alloc;

    const ASCII_u8_1: u8 = '1' as u8;
    const ASCII_1: u32 = ASCII_u8_1 as u32;
    use sqlparser::ast::Table;
    use stats_alloc::{Region, StatsAlloc, INSTRUMENTED_SYSTEM};
    use std::alloc::System;
    use std::collections::HashSet;
    use std::fs;
    use std::thread::sleep;
    use std::time::Duration;

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

        let mut s_engine = OurStorageEngine::init(1);
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
            let table = catalog::Table {
                name: "user".to_string(),
                id: 60 as u16,
                schema: schema.clone(),
                indexes: HashSet::new(),
            };

            page.write_table_details(&table).unwrap();

            schema.retain(|_k, v| v.column_type == ColumnType::String);

            let table = catalog::Table {
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

        let mut s_engine = OurStorageEngine::init(200);
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
