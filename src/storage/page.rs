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
    collections::{BTreeMap, HashMap, HashSet},
    fmt::format,
    fs::File,
    io::{Cursor, Read, Seek, Write},
    ops::{Shl, Shr},
    vec,
};

use crate::{
    query::{NaadanRecord, RecordSet},
    storage::utils::write_string_to_buf,
    utils::{self, log},
};

const DB_DATAFILE: &str = "/tmp/DB_data_file.bin";
const DB_TABLE_CATALOG_FILE: &str = "/tmp/DB_table_catalog_file.bin";
const DB_TABLE_DATA_FILE: &str = "/tmp/DB_table_{table_id}_file.bin";
const DEFAULT_DYNAMIC_PAGE_REGION_OFFSET: u32 = 1024;

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
pub enum PageType {
    // Page which doesn't contain variable length columns
    FixedPage,

    // Page with varlen columns with - Value representing the dynamic memory base offset
    DynamicPage(Offset),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageHeader {
    pub(crate) checksum: [u8; 4],
    pub(crate) extra: [u8; 4],
    pub(crate) row_offset: HashMap<Id, Offset>,
    // Last fixed size start offset (grows right -> left)
    pub(crate) last_fixed_size_offset: Offset,
    // Last dynamic size end offset (grows left -> right)
    pub(crate) last_dynamic_size_offset: Offset,
    pub(crate) page_capacity: u32,
    pub(crate) page_type: PageType,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PageData {
    /// The page row data field.
    /// will have dynamic values based on the table schema and other details,
    pub data: Vec<u8>,
}

/// ```
///   PAGE STRUCTURE FOR TABLE DATA (Slotted page structure)
///
/// | <---------- HEADER  --------->  | <---- DATA ------>|
///                                  /                     \
///                                 /                       \
///                                /                         \
///                               / Dynamic size | Fixed size \
///                              /     data      |    data     \
///                             ---------------------------------
///
/// |------------------------------------------------------|\
/// || checksum | extra | row_offset... | last_fs_offset|  | \
/// |------------------------------------------------------|  \
/// | last_ds_offset | page_capacity | page_type|| ....--> |   \
/// |------------------------------------------------------|    \
/// | ...Dynamic size memory (If page_type is dynamic).... |   Page Size (4KB)    
/// |------------------------------------------------------|    /
/// | ....---->    || ROW-N (fixed size) |   <---......    |   /
/// |------------------------------------------------------|  /
/// |          <---.............      |  ROW-2  |  ROW-1  || /
/// |------------------------------------------------------|/
///
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
                schema: BTreeMap::new(),
                column_schema_size: 0,
            };
            let mut schema_size = 0;
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

                schema_size += col_offset
            }

            table.column_schema_size = schema_size;

            return Ok(table);
        }
        Err(NaadanError::TableNotFound)
    }

    fn write_table_details(&mut self, table: &Table) -> Result<usize, NaadanError> {
        utils::log(
            "Page".to_string(),
            format!("Creating new table with details {:?}", table),
        );
        let split_offset: u64 = self.header.page_capacity as u64 * 1 / 4;
        let data = &mut self.data;

        let mut buf_cursor = Cursor::new(&mut data.data);

        let table_count = table.id as u32;
        // Write table count
        buf_cursor.write_all(&table_count.to_be_bytes()).unwrap();

        let mut cursor_base = 4 as u64;

        match self.header.row_offset.get(&0) {
            Some(&val) => {
                cursor_base = val as u64;
            }
            None => {}
        }

        buf_cursor
            .seek(std::io::SeekFrom::Start(cursor_base))
            .unwrap();

        let mut dyn_cursor_base = split_offset;

        match self.header.row_offset.get(&1) {
            Some(&val) => {
                dyn_cursor_base = val as u64;
            }
            None => {}
        }

        if cursor_base + 18 >= split_offset
            || (dyn_cursor_base + table.name.len() as u64 + 2 + table.schema.len() as u64 * 4 * 64)
                >= self.header.page_capacity as u64
        {
            utils::log(
                "Page".to_string(),
                format!("Buffer full cursor at {}", cursor_base),
            );
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

        self.header
            .row_offset
            .insert(0, buf_cursor.position() as u32);

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

        self.header
            .row_offset
            .insert(1, buf_cursor.position() as u32);

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

                let mut page = Page::new_dynamic_page();

                let schema = BTreeMap::from([
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
                    column_schema_size: 0,
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
    /// Create new fixed page instance with default capacity
    pub fn new_fixed_page() -> Self {
        Self::new_fixed_page_with_capacity(4 * 1024)
    }
    pub fn new_fixed_page_with_capacity(capacity: u32) -> Self {
        Self::new_with_capacity(capacity, PageType::FixedPage)
    }

    /// Create new dynamic sized page instance with default capacity
    pub fn new_dynamic_page() -> Self {
        Self::new_dynamic_page_with_capacity(4 * 1024)
    }
    pub fn new_dynamic_page_with_capacity(capacity: u32) -> Self {
        Self::new_with_capacity(
            capacity,
            PageType::DynamicPage(DEFAULT_DYNAMIC_PAGE_REGION_OFFSET),
        )
    }

    /// Create new page instance with the provided capacity and type
    pub fn new_with_capacity(capacity: u32, page_type: PageType) -> Self {
        let page_header = PageHeader {
            extra: [0; 4],
            checksum: [0; 4],
            row_offset: HashMap::new(),
            last_fixed_size_offset: capacity - 1,
            last_dynamic_size_offset: 0,
            page_capacity: capacity,
            page_type,
        };

        Self {
            header: page_header,
            data: PageData::default(),
        }
    }

    /// Write a new row into the table page
    pub fn write_table_row(
        &mut self,
        // Start ID of rows to be inserted
        mut row_id: u64,
        // Row collection to be inserted
        row_data: &RecordSet,
        // Table schema of rows to be inserted
        schema: &Table,
    ) -> Result<usize, NaadanError> {
        utils::log(
            "Page".to_string(),
            format!("Writing row {} to page and rows {:?}", row_id, row_data),
        );

        let data = &mut self.data;
        let mut buf_cursor = Cursor::new(&mut data.data);

        // TODO: Occasionaly analyse and reorder dynamic memory to free up space for row or header data.
        let dyn_cursor_base: u64;
        let mut last_dyn_offset: u64;

        match self.header.page_type {
            PageType::FixedPage => {
                dyn_cursor_base = 0;
                last_dyn_offset = 0;
            }
            PageType::DynamicPage(offset) => {
                dyn_cursor_base = offset as u64;
                last_dyn_offset = self.header.last_dynamic_size_offset as u64;
            }
        };

        for row in row_data {
            if row.row_id() != 0 {
                row_id = row.row_id()
            }
            // TODO: move these calculations outside the loop
            // Final_size -> (last inserted row start offset - row size - serialized header size -additional header size for new row metadata(16 bytes))
            let space_available_after_insertion = self.header.last_fixed_size_offset as i32
                - schema.column_schema_size as i32
                - bincode::serialized_size(&self.header).unwrap() as i32
                - 16
                - last_dyn_offset as i32;

            utils::log(
                "Page".to_string(),
                format!(
                    "Free page space available after insertion will be {}",
                    space_available_after_insertion
                ),
            );

            if space_available_after_insertion > 0 {
                let row_offset =
                    self.header.last_fixed_size_offset as u64 - schema.column_schema_size;

                buf_cursor.set_position(row_offset);

                utils::log(
                    "Page".to_string(),
                    format!(
                        "Writing row at offset {} and last dyn offset is {}\n",
                        row_offset, last_dyn_offset
                    ),
                );

                utils::log("Page".to_string(), format!("Row: {:?}", row));
                for (index, column) in row.columns().iter().enumerate() {
                    utils::log(
                        "Page".to_string(),
                        format!(
                            "Writing column at offset {} and last dyn offset is {}\n",
                            buf_cursor.position(),
                            last_dyn_offset
                        ),
                    );

                    match row.column_schema() {
                        Some(record) => {
                            let col_offset = record.iter().nth(index).unwrap().1.offset;
                            let write_offset = row_offset + col_offset;

                            buf_cursor.set_position(write_offset);
                        }
                        None => {}
                    }

                    if let Some(value) = write_column_value(
                        column,
                        &mut buf_cursor,
                        &dyn_cursor_base,
                        &mut last_dyn_offset,
                    ) {
                        return value;
                    }
                }
                self.header.last_fixed_size_offset -= schema.column_schema_size as u32;
                self.header
                    .row_offset
                    .insert(row_id as u32, self.header.last_fixed_size_offset as u32);

                self.header.last_dynamic_size_offset = last_dyn_offset as u32;

                utils::log("Page".to_string(), format!("Done inserting row {}", row_id));
                if row.row_id() == 0 {
                    row_id += 1;
                }
            } else {
                utils::log(
                    "Page".to_string(),
                    format!("Page capacity full, couldn't insert row {}", row_id),
                );
                return Err(NaadanError::PageCapacityFull(row_id));
            }
        }

        Ok(row_id as usize)
    }

    /// Read a row from the table page
    pub fn read_table_row(&self, row_id: &u64, table: &Table) -> Result<NaadanRecord, NaadanError> {
        utils::log(
            "Page".to_string(),
            format!("Reading row {} from page", row_id),
        );
        utils::log(
            "Page".to_string(),
            format!("row_offset: {:?}", self.header.row_offset),
        );

        let row_offset = *self.header.row_offset.get(&(*row_id as u32)).unwrap() as u64;

        let data = &self.data;
        let mut buf_cursor = Cursor::new(&data.data);

        buf_cursor.set_position(row_offset as u64);

        let dyn_cursor_base: u64 = match self.header.page_type {
            PageType::FixedPage => 0,
            PageType::DynamicPage(offset) => offset as u64,
        };

        utils::log(
            "Page".to_string(),
            format!(
                "Reading row at offset {}\n - dyn_cursor_base at {}\n - last_dynamic_size_offset at {}\n - last_fixed_size_offset at {}",
                row_offset, dyn_cursor_base, self.header.last_dynamic_size_offset, self.header.last_fixed_size_offset
            ),
        );

        let mut row: Vec<(String, Expr)> = vec![];

        utils::log(
            "Page".to_string(),
            format!("Row schema: {:?}", table.schema),
        );
        // Read the column values provided the table schema
        for (c_name, c_type) in &table.schema {
            let read_offset = row_offset + c_type.offset;
            buf_cursor.set_position(read_offset);
            utils::log(
                "Page".to_string(),
                format!("Reading column: {} and offset is {:?}", c_name, read_offset),
            );
            read_column_value(c_type, c_name, &mut buf_cursor, &mut row, &dyn_cursor_base);
            utils::log(
                "Page".to_string(),
                format!("Read column: {} and value is {:?} \n", c_name, row),
            );
        }

        let mut column_names: BTreeMap<String, Column> = BTreeMap::new();
        let columns = row
            .iter()
            .map(|val| {
                let column = val.1.clone();
                let col = table.schema.get(&val.0).unwrap();
                column_names.insert(val.0.clone(), col.clone());
                column
            })
            .collect::<Vec<Expr>>();

        Ok(NaadanRecord::new_with_column_schema(
            *row_id,
            columns,
            column_names,
        ))
    }

    /// Update a row in the table page
    pub fn update_table_row(
        &mut self,
        row_id: u64,
        updated_columns: &BTreeMap<String, Expr>,
        table: &Table,
        contains_var_type: bool,
        //row_handler: F,
    ) -> Result<(), NaadanError>
// where
    //     F: FnOnce(&Expr) -> Result<R, NaadanError>,
    {
        utils::log("Page".to_string(), format!("Updating row {} ", row_id));
        let row_offset = *self.header.row_offset.get(&(row_id as u32)).unwrap() as u64;

        let data = &mut self.data;
        let mut buf_cursor = Cursor::new(&mut data.data);
        buf_cursor.set_position(row_offset);

        let dyn_cursor_base: u64;
        let mut last_dyn_offset: u64;

        match self.header.page_type {
            PageType::FixedPage => {
                dyn_cursor_base = 0;
                last_dyn_offset = 0;
            }
            PageType::DynamicPage(offset) => {
                dyn_cursor_base = offset as u64;
                last_dyn_offset = self.header.last_dynamic_size_offset as u64;
            }
        };
        utils::log(
            "Page".to_string(),
            format!(
                "Updating row at offset {}\n - dyn_cursor_base at {}\n - last_dynamic_size_offset at {}\n - last_fixed_size_offset at {}",
                row_offset, dyn_cursor_base, self.header.last_dynamic_size_offset, self.header.last_fixed_size_offset
            ),
        );

        // If the updated columns have variable types and the length of the variable columns + last dynmaic offset
        // is greater than last fixed size offset, return page capacity full error.
        if contains_var_type {
            let var_column_len = updated_columns
                .iter()
                .map(|val| get_var_column_len(val.1))
                .reduce(|x1, x2| x1 + x2 + 2)
                .unwrap() as u64;

            if dyn_cursor_base + last_dyn_offset + var_column_len
                >= self.header.last_fixed_size_offset as u64
            {
                return Err(NaadanError::PageCapacityFull(row_id));
            }
        }

        for (col_name, col_value) in updated_columns {
            let col_offset = table.schema.get(col_name).unwrap().offset;
            let update_offset = row_offset + col_offset;

            buf_cursor.set_position(update_offset);
            utils::log(
                "Page".to_string(),
                format!(
                    "Updating column at offset {} and last dyn offset is {}\n",
                    update_offset, last_dyn_offset
                ),
            );

            // TODO: Handle existing string allocation or mark it free
            // TODO: Add check for available space.
            // row_handler(col_value);
            write_column_value(
                &col_value,
                &mut buf_cursor,
                &dyn_cursor_base,
                &mut last_dyn_offset,
            );
        }

        self.header.last_dynamic_size_offset = last_dyn_offset as u32;

        Ok(())
    }

    /// Write a page to disk
    pub fn write_to_disk(&self, page_id: PageId) -> Result<(), NaadanError> {
        utils::log(
            "Page".to_string(),
            format!("Flushing Data: {:?} to disk.", self),
        );
        let table_data_file = String::from(DB_TABLE_DATA_FILE)
            .replace("{table_id}", page_id.get_table_id().to_string().as_str());

        match File::options()
            .read(true)
            .write(true)
            .open(table_data_file.clone())
        {
            Ok(mut file) => {
                file.seek(std::io::SeekFrom::Start(
                    page_id.get_page_index() as u64 * 8 * 1024,
                ))
                .unwrap();
                let write_bytes = bincode::serialize(&self).unwrap();

                utils::log(
                    "Page".to_string(),
                    format!(
                        "Flushing page {} to page offset {}, writing total {} bytes",
                        page_id.get_page_index(),
                        page_id.get_page_index() as u64 * 8 * 1024,
                        write_bytes.len()
                    ),
                );

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
        utils::log(
            "Page".to_string(),
            format!("Reading page with id: {} from disk", page_id.get_page_id()),
        );

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

fn get_var_column_len(col: &Expr) -> usize {
    match col {
        sqlparser::ast::Expr::Value(value) => match value {
            sqlparser::ast::Value::SingleQuotedString(val)
            | sqlparser::ast::Value::DoubleQuotedString(val) => return val.len(),
            _ => return 0,
        },
        _ => 0,
    }
}

fn read_column_value(
    c_type: &Column,
    c_name: &String,
    buf_cursor: &mut Cursor<&Vec<u8>>,
    row: &mut Vec<(String, Expr)>,
    dyn_cursor_base: &u64,
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
            let offset = *dyn_cursor_base + u64::from_be_bytes(offset_buf);
            utils::log(
                "Page".to_string(),
                format!(
                    "Dynamic string offset at {} - Absolute offset is {}",
                    u64::from_be_bytes(offset_buf),
                    offset
                ),
            );
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
    dyn_cursor_base: &u64,
    last_dyn_offset: &mut u64,
) -> Option<Result<usize, NaadanError>> {
    utils::log("Page".to_string(), format!("Column value {:?}", column));
    match column {
        sqlparser::ast::Expr::Value(value) => match value {
            sqlparser::ast::Value::Number(val, _) => {
                let number = val.parse::<i32>().unwrap();
                buf_cursor.write_all(&number.to_be_bytes()).unwrap();
            }
            sqlparser::ast::Value::SingleQuotedString(val) => {
                write_string_to_buf(dyn_cursor_base, last_dyn_offset, buf_cursor, val);
            }
            sqlparser::ast::Value::DoubleQuotedString(val) => {
                write_string_to_buf(dyn_cursor_base, last_dyn_offset, buf_cursor, val);
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
            utils::log(
                "Page".to_string(),
                format!("Table insert source is not explicit values."),
            );
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

    #[test]
    fn catalog_write() {
        let mut page = Page::new_dynamic_page_with_capacity(4 * 1024);

        let mut schema = BTreeMap::from([
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
                column_schema_size: 0,
                indexes: HashSet::new(),
            };

            page.write_table_details(&table).unwrap();

            schema.retain(|_k, v| v.column_type == ColumnType::String);

            let table = Table {
                name: "class".to_string(),
                id: 60 as u16,
                schema: schema.clone(),
                column_schema_size: 0,
                indexes: HashSet::new(),
            };

            match page.write_table_details(&table) {
                Ok(size) => {
                    //utils::utils::log("Page".to_string(),format!("Data buffer: {:?}", page.data.get(&0).unwrap()));
                    // if i % 100000 == 0 {
                    utils::log(
                        "Page".to_string(),
                        format!("Wrote table {} details at Offset: {}", i, size),
                    );
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
