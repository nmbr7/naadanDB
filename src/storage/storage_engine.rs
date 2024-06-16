use sqlparser::ast::Expr;
use std::{
    collections::{BTreeMap, HashMap},
    vec,
};
use tokio::{
    sync::{RwLock, RwLockReadGuard},
    task,
};

use super::{
    page::{CatalogPage, Page, PageId, PageType},
    CatalogEngine, NaadanError, RowIdType, ScanType, StorageEngine, TableIdType,
};
use crate::storage::catalog::*;
use crate::{
    query::{plan::ScalarExprType, NaadanRecord, RecordSet},
    utils,
};

/// Storage Engine
#[derive(Debug)]
pub struct NaadanStorageEngine {
    pub(crate) catalog_page: RwLock<HashMap<usize, Page>>,
    pub(crate) buffer_pool: RwLock<BufferPool>,
    pub(crate) row_index: RwLock<HashMap<u64, PageId>>,
    pub(crate) table_index: RwLock<HashMap<TableIdType, TableMetadata>>,
    pub(crate) engine_metadata: RwLock<StorageEngineMetadata>,
}

impl NaadanStorageEngine {
    pub fn init(page_count: usize) -> Self {
        let buffer_pool = BufferPool {
            page_pool: HashMap::with_capacity(page_count),
            buffer_metadata: PoolMetadata::default(),
        };
        let catalog_page = Page::read_catalog_from_disk().unwrap();
        let table_count = catalog_page.table_count().unwrap();
        let row_index = HashMap::new();

        Self {
            buffer_pool: RwLock::new(buffer_pool),
            row_index: RwLock::new(row_index),
            engine_metadata: RwLock::new(StorageEngineMetadata::new(table_count as usize)),
            catalog_page: RwLock::new(HashMap::from([(1, catalog_page)])),
            table_index: RwLock::new(HashMap::new()),
        }
    }
}

impl StorageEngine for NaadanStorageEngine {
    type ScanIterator<'a> = ScanIterator<'a>;

    fn write_table_rows(
        &self,
        row_values: RecordSet,
        table: &Table,
    ) -> Result<RowIdType, NaadanError> {
        task::block_in_place(|| {
            let mut table_pages: Vec<PageId> = vec![];
            let mut page_id: PageId;
            let mut row_id: u64;

            // utils::log("BufferPool".to_string(),format!("Storage engine: {:?}", self));

            let mut table_index = self.table_index.blocking_write();
            let mut row_index = self.row_index.blocking_write();
            let mut buffer_pool = self.buffer_pool.blocking_write();

            let mut current_available_page;
            let mut page: &mut Page = match table_index.get_mut(&(table.id as usize)) {
                Some(val) => {
                    table_pages.append(&mut val.page_ids.clone());
                    current_available_page = table_pages.len();
                    page_id = table_pages.last().unwrap().clone();
                    // TODO: make these updated atomic
                    row_id = val.row_count;
                    if row_values.records().iter().all(|x| x.row_id() == 0) {
                        row_id += 1;
                        val.row_count += row_values.count() as u64;
                    }

                    let p = buffer_pool.page_pool.get_mut(&page_id).unwrap();
                    p
                }
                None => {
                    page_id = PageId::new(table.id, 1);
                    row_id = 1;

                    let page = if table.is_fixed_length() {
                        Page::new_fixed_page_with_capacity(8 * 1024)
                    } else {
                        Page::new_dynamic_page_with_capacity(8 * 1024)
                    };

                    buffer_pool.page_pool.insert(page_id, page);

                    table_index.insert(
                        table.id as usize,
                        TableMetadata {
                            page_ids: vec![page_id],
                            row_count: row_values.count() as u64,
                        },
                    );

                    let page = buffer_pool.page_pool.get_mut(&page_id).unwrap();

                    current_available_page = 1;
                    page
                }
            };

            let mut write_complete = false;

            let mut rows_to_insert = row_values;

            let mut is_single_write;
            while !write_complete {
                // println!("Writing table Rows {:?}", rows_to_insert);
                let mut row_end: u64 = match page.write_table_row(row_id, &rows_to_insert, table) {
                    Ok(last_row) => {
                        write_complete = true;
                        last_row as u64
                    }
                    Err(NaadanError::PageCapacityFull(last_row)) => last_row,
                    _ => unreachable!(),
                };

                page.write_to_disk(page_id).unwrap();

                if row_end <= row_id {
                    row_id = row_end;
                    row_end = row_end + 1;
                    is_single_write = true;
                } else {
                    is_single_write = false
                }

                // utils::log(
                //     "Storage_engine".to_string(),
                //     format!(
                //         "Wrote {:?} rows, last inserted row id is {} and start is {}.",
                //         rows_to_insert.count(),
                //         row_end,
                //         row_id
                //     ),
                // );

                for r_id in row_id..row_end {
                    row_index.insert(r_id as u64, page_id);
                    utils::log(
                        "Storage_engine".to_string(),
                        format!(
                            "Updating row_index with values row id {} and page id {}",
                            r_id,
                            page_id.get_page_index()
                        ),
                    );
                }

                if !write_complete {
                    page_id = PageId::new(table.id, current_available_page as u32 + 1);

                    {
                        let l_page = if table.is_fixed_length() {
                            Page::new_fixed_page_with_capacity(8 * 1024)
                        } else {
                            Page::new_dynamic_page_with_capacity(8 * 1024)
                        };

                        buffer_pool.page_pool.insert(page_id, l_page);
                    }

                    let t_metadata = table_index.get_mut(&(table.id as usize)).unwrap();

                    t_metadata.page_ids.push(page_id);
                    page = buffer_pool.page_pool.get_mut(&page_id).unwrap();

                    current_available_page += 1;
                    if !is_single_write {
                        let remaining_rows = rows_to_insert
                            .records()
                            .to_vec()
                            .split_off((row_end - row_id) as usize);

                        rows_to_insert = RecordSet::new(remaining_rows);

                        row_id = row_end;
                    }
                }
            }

            Ok(row_id)
        })
    }

    fn delete_table_rows(&self, row_ids: &[u64], schema: &Table) -> Result<RecordSet, NaadanError> {
        todo!()
    }

    fn update_table_rows<'a>(
        &self,
        scan_types: &'a ScanType,
        updated_columns: &BTreeMap<String, Expr>,
        schema: &Table,
    ) -> Result<Vec<RowIdType>, NaadanError> {
        task::block_in_place(|| {
            let mut records = RecordSet::new(vec![]);
            let mut row_ids: Vec<RowIdType> = vec![];
            {
                let table_index = self.table_index.blocking_read();
                let row_id_list: Vec<u64> = match scan_types {
                    ScanType::Filter(predicate) => {
                        // TODO: Read the row_ids for row with predicate
                        vec![1]
                    }
                    ScanType::RowIds(row_ids) => row_ids.to_vec(),
                    ScanType::Full => {
                        let table_metadata = table_index.get(&(schema.id as usize)).unwrap();
                        Vec::from_iter(1..table_metadata.row_count as u64 + 1)
                    }
                };

                let row_index = self.row_index.blocking_read();
                let mut buffer_pool = self.buffer_pool.blocking_write();

                let contains_var_type = updated_columns.iter().any(|col| match col.1 {
                    sqlparser::ast::Expr::Value(value) => match value {
                        sqlparser::ast::Value::SingleQuotedString(_)
                        | sqlparser::ast::Value::DoubleQuotedString(_) => return true,
                        _ => return false,
                    },
                    _ => false,
                });

                for r_id in row_id_list {
                    let page_id = match row_index.get(&r_id) {
                        Some(pageid) => {
                            row_ids.push(r_id);
                            pageid
                        }
                        None => continue,
                    };

                    let page = buffer_pool.get_mut(&page_id).unwrap();

                    match page.update_table_row(r_id, &updated_columns, schema, contains_var_type) {
                        Err(NaadanError::PageCapacityFull(row_id)) => {
                            utils::log(
                                "Storage_engine".to_string(),
                                format!("Page capacity full, moving row to another page",),
                            );
                            let mut record = page.read_table_row(&row_id, schema).unwrap();

                            updated_columns
                                .iter()
                                .for_each(|col| record.update_column_value(col));

                            // TODO: delete the current record from it's page and add it to free space in page
                            // self.delete_table_rows(&[row_id], schema);

                            records.add_record(record);
                        }
                        Ok(_) => {
                            utils::log(
                                "Storage_engine".to_string(),
                                format!(
                                    "Done updating row {} to page {}",
                                    r_id,
                                    page_id.get_page_index()
                                ),
                            );
                        }
                        _ => (),
                    }

                    page.write_to_disk(*page_id).unwrap();
                }
            }

            if records.count() > 0 {
                // println!("update pending records: {:?}", records.records());
                self.write_table_rows(records, schema).unwrap();
            }
            // for res in self.scan_table(&ScanType::RowIds(vec![1, 2]), schema) {
            //     utils::log(
            //         "Storage_engine".to_string(),
            //         format!("Row after update {:?}", res.unwrap()),
            //     );
            // }
            return Ok(row_ids);
        })
    }

    /// This will block on read lock for storage layer structures
    fn scan_table<'a>(&'a self, scan_types: &'a ScanType, schema: &'a Table) -> ScanIterator {
        let row_index_guard = self.row_index.blocking_read();
        let buffer_pool_guard = self.buffer_pool.blocking_read();

        let guard = ScanGuard::new(Some(row_index_guard), Some(buffer_pool_guard));

        match scan_types {
            ScanType::Filter(predicate) => {
                let row_count = self
                    .table_index
                    .blocking_read()
                    .get(&(schema.id as usize))
                    .unwrap()
                    .row_count as u64;
                return ScanIterator::new(RowScanType::Filter(predicate, row_count), schema, guard);
            }
            ScanType::RowIds(row_ids) => {
                return ScanIterator::new(RowScanType::ExplicitRowScan(row_ids), schema, guard);
            }
            ScanType::Full => {
                let row_count = self
                    .table_index
                    .blocking_read()
                    .get(&(schema.id as usize))
                    .unwrap()
                    .row_count as u64;
                return ScanIterator::new(RowScanType::FullScan(row_count), schema, guard);
            }
        }
    }
}

enum RowScanType<'a> {
    FullScan(u64),
    ExplicitRowScan(&'a [u64]),
    Filter(&'a crate::query::plan::ScalarExprType, u64),
}
struct ScanGuard<'a> {
    row_index_guard: Option<RwLockReadGuard<'a, HashMap<u64, PageId>>>,
    buffer_pool_guard: Option<RwLockReadGuard<'a, BufferPool>>,
    //table_index_guard:  Option<RwLockReadGuard<'a, HashMap<TableIdType, TableMetadata>>>,
}

impl<'a> ScanGuard<'a> {
    fn new(
        row_index_guard: Option<RwLockReadGuard<'a, HashMap<u64, PageId>>>,
        buffer_pool_guard: Option<RwLockReadGuard<'a, BufferPool>>,
    ) -> Self {
        Self {
            row_index_guard,
            buffer_pool_guard,
        }
    }
}

pub struct ScanIterator<'a> {
    scan_type: RowScanType<'a>,
    current_index: usize,
    guard: ScanGuard<'a>,
    schema: &'a Table,
}

impl<'a> ScanIterator<'a> {
    fn new(scan_type: RowScanType<'a>, schema: &'a Table, guard: ScanGuard<'a>) -> Self {
        Self {
            scan_type,
            current_index: 0,
            guard,
            schema,
        }
    }
}

impl<'a> Iterator for ScanIterator<'a> {
    type Item = Result<NaadanRecord, NaadanError>;

    fn next(&mut self) -> Option<Self::Item> {
        let row_id_vec: Vec<u64>;

        let mut scan_predicate: Option<&ScalarExprType> = None;

        let row_ids = match self.scan_type {
            RowScanType::FullScan(row_count) => {
                row_id_vec = (1..row_count + 1).collect::<Vec<u64>>();
                row_id_vec.as_slice()
            }
            RowScanType::ExplicitRowScan(row_ids) => row_ids,
            RowScanType::Filter(predicate, row_count) => {
                scan_predicate = Some(predicate);
                row_id_vec = (1..row_count + 1).collect::<Vec<u64>>();
                row_id_vec.as_slice()
            }
        };

        loop {
            let row_id = match row_ids.get(self.current_index) {
                Some(row_id) => row_id,
                None => return None,
            };

            let page_id = match self.guard.row_index_guard.as_ref().unwrap().get(row_id) {
                Some(pageid) => pageid,
                None => return Some(Err(NaadanError::RowNotFound)),
            };

            let page = self
                .guard
                .buffer_pool_guard
                .as_ref()
                .unwrap()
                .get(&page_id)
                .unwrap();

            utils::log(
                "Storage_engine".to_string(),
                format!("Row id {} in Page id: {}", row_id, page_id.get_page_index()),
            );
            let row = page.read_table_row(row_id, self.schema).unwrap();

            self.current_index += 1;

            if let Some(_predicate) = scan_predicate {
                // TODO check predicate againt the row
                // if predicate fails, get net row,
            }

            return Some(Ok(row));
        }
    }
}

impl CatalogEngine for NaadanStorageEngine {
    fn get_table_details(&self, name: &String) -> Result<Table, NaadanError> {
        task::block_in_place(|| {
            self.catalog_page
                .blocking_read()
                .get(&1)
                .unwrap()
                .get_table_details(name)
        })
    }

    fn add_table_details(&self, table: &mut Table) -> Result<TableIdType, NaadanError> {
        task::block_in_place(|| {
            let mut catalog_page_map = self.catalog_page.blocking_write();
            let catalog_page = catalog_page_map.get_mut(&1).unwrap();
            table.id = self.engine_metadata.blocking_read().table_count as u16 + 1;
            {
                let mut engine_metadata = self.engine_metadata.blocking_write();
                engine_metadata.table_count += 1;
            }
            match catalog_page.write_table_details(table) {
                Ok(_) => {}
                Err(err) => return Err(err),
            }
            match catalog_page.write_catalog_to_disk() {
                Ok(_) => {}
                Err(err) => return Err(err),
            }

            Ok(10)
        })
    }

    fn delete_table_details(&self, name: &String) -> Result<Table, NaadanError> {
        todo!()
    }
}

/// Storage Engine Metadata
#[derive(Debug)]
pub struct StorageEngineMetadata {
    table_count: usize,
}

impl StorageEngineMetadata {
    pub fn new(table_count: usize) -> Self {
        Self { table_count }
    }
}

#[derive(Debug)]
pub struct TableMetadata {
    pub page_ids: Vec<PageId>,
    pub row_count: u64,
}

#[derive(PartialEq, Eq, Debug, Clone, Default)]
pub struct FreePage(PageId, usize);

impl FreePage {
    pub fn page_id(&self) -> &PageId {
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
    pub page_pool: HashMap<PageId, Page>,
}

impl BufferPool {
    pub fn page_exist(&self, page_id: &PageId) -> bool {
        self.page_pool.contains_key(page_id)
    }

    pub fn get_mut(&mut self, page_id: &PageId) -> Result<&mut Page, bool> {
        match self.page_pool.get_mut(page_id) {
            Some(page) => Ok(page),
            None => Err(false),
        }
    }

    pub fn get(&self, page_id: &PageId) -> Result<&Page, bool> {
        match self.page_pool.get(page_id) {
            Some(page) => Ok(page),
            None => Err(false),
        }
    }

    pub fn add(&mut self, page_id: &PageId, page: Page) -> Result<&mut Page, bool> {
        self.buffer_metadata
            .free_pages
            .push(FreePage(*page_id, 1024 * 4));
        self.page_pool.insert(*page_id, page);
        self.get_mut(page_id)
    }

    pub fn get_any_available_page(&mut self, size: &usize) -> Option<FreePage> {
        utils::log(
            "BufferPool".to_string(),
            format!("Get: {:#?}", self.buffer_metadata.free_pages),
        );
        self.buffer_metadata.free_pages.sort();
        match self.buffer_metadata.free_pages.last() {
            Some(fp) if fp.1 > (size + 8 * 8) => Some(fp.clone()),
            _ => None,
        }
    }

    pub fn get_available_page(&self, page_id: &PageId) -> Option<FreePage> {
        self.buffer_metadata.free_pages.iter().find_map(|s| {
            if s.0 == *page_id {
                Some(s.clone())
            } else {
                None
            }
        })
    }

    pub fn update_available_page_size(&mut self, page_id: PageId, size: usize) -> bool {
        utils::log(
            "BufferPool".to_string(),
            format!("Getin Update: {:#?}", self.buffer_metadata.free_pages),
        );
        let np = self.buffer_metadata.free_pages.pop().unwrap();
        if np.0 == page_id {
            utils::log(
                "BufferPool".to_string(),
                format!("Updating page {:?} with current {:#?} ", np.0, np.1),
            );
            self.buffer_metadata.free_pages.push(FreePage(np.0, size));
            utils::log(
                "BufferPool".to_string(),
                format!("Updated value: {:#?}", self.buffer_metadata.free_pages),
            );
        }

        true
    }
}
