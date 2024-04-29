use sqlparser::ast::Expr;
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};
use tokio::{
    sync::{Mutex, RwLock, RwLockReadGuard},
    task,
};

use super::{
    page::{CatalogPage, Page, PageId},
    CatalogEngine, NaadanError, RowIdType, StorageEngine, TableIdType,
};
use crate::query::{NaadanRecord, RecordSet};
use crate::storage::catalog::*;

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
            let mut row_id: u32;

            // println!("Storage engine: {:?}", self);

            let mut table_index = self.table_index.blocking_write();
            let mut row_index = self.row_index.blocking_write();
            let mut buffer_pool = self.buffer_pool.blocking_write();

            let page: &mut Page = match table_index.get_mut(&(table.id as usize)) {
                Some(val) => {
                    table_pages.append(&mut val.page_ids.clone());
                    page_id = table_pages.last().unwrap().clone();
                    // TODO: make these updated atomic
                    row_id = val.row_count + 1;
                    val.row_count += row_values.count() as u32;

                    let p = buffer_pool.page_pool.get_mut(&page_id).unwrap();
                    // TODO: check if the page has enough space
                    p
                }
                None => {
                    page_id = PageId::new(table.id, 1);
                    row_id = 1;

                    let page = Page::new_with_capacity(8 * 1024);
                    buffer_pool.page_pool.insert(page_id, page);

                    table_index.insert(
                        table.id as usize,
                        TableMetadata {
                            page_ids: vec![page_id],
                            row_count: row_values.count() as u32,
                        },
                    );

                    let page = buffer_pool.page_pool.get_mut(&page_id).unwrap();

                    page
                }
            };

            let row_end: u32 = page.write_table_row(row_id, row_values, table).unwrap() as u32;
            page.write_to_disk(page_id).unwrap();

            for r_id in row_id..row_end {
                row_index.insert(r_id as u64, page_id);
            }

            println!("{:?}", self.row_index);

            Ok(row_id as usize)
        })
    }

    /// This will block on read lock for storage layer structures
    fn read_table_rows<'a>(&'a self, row_ids: &'a [u64], schema: &'a Table) -> ScanIterator {
        let row_index_guard = self.row_index.blocking_read();
        let buffer_pool_guard = self.buffer_pool.blocking_read();

        let guard = ScanGuard::new(Some(row_index_guard), Some(buffer_pool_guard));
        ScanIterator::new(ScanType::ExplicitRowScan(row_ids), schema, guard)
    }

    fn delete_table_rows(&self, row_ids: &[u64], schema: &Table) -> Result<RecordSet, NaadanError> {
        todo!()
    }

    fn update_table_rows(
        &self,
        row_ids: Option<Vec<u64>>,
        updated_columns: &HashMap<String, Expr>,
        schema: &Table,
    ) -> Result<&[RowIdType], NaadanError> {
        task::block_in_place(|| {
            let table_index = self.table_index.blocking_read();
            let row_id_list = match row_ids {
                Some(val) => val,
                None => {
                    let table_metadata = table_index.get(&(schema.id as usize)).unwrap();
                    Vec::from_iter(1..table_metadata.row_count as u64 + 1)
                }
            };

            let row_index = self.row_index.blocking_read();
            let mut buffer_pool = self.buffer_pool.blocking_write();

            for r_id in &row_id_list {
                let page_id = match row_index.get(r_id) {
                    Some(pageid) => pageid,
                    None => continue,
                };

                let page = buffer_pool.get_mut(&page_id).unwrap();

                // TODO: Write transaction row version chain for current rowid
                page.update_table_row(r_id, &updated_columns, schema)
                    .unwrap();
            }

            Ok([0 as usize].as_slice())
        })
    }

    /// This will block on read lock for storage layer structures
    fn scan_table<'a>(
        &'a self,
        predicate: Option<crate::query::plan::ScalarExprType>,
        schema: &'a Table,
    ) -> ScanIterator {
        let mut row_collection: RecordSet = RecordSet::new(vec![]);
        let row_index_guard = self.row_index.blocking_read();
        let buffer_pool_guard = self.buffer_pool.blocking_read();

        let guard = ScanGuard::new(Some(row_index_guard), Some(buffer_pool_guard));
        let row_count = self
            .table_index
            .blocking_read()
            .get(&(schema.id as usize))
            .unwrap()
            .row_count as u64;

        ScanIterator::new(ScanType::FullScan(row_count), schema, guard)
    }
}

enum ScanType<'a> {
    FullScan(u64),
    ExplicitRowScan(&'a [u64]),
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
    scan_type: ScanType<'a>,
    current_index: usize,
    guard: ScanGuard<'a>,
    schema: &'a Table,
}

impl<'a> ScanIterator<'a> {
    fn new(scan_type: ScanType<'a>, schema: &'a Table, guard: ScanGuard<'a>) -> Self {
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
        let row_ids = match self.scan_type {
            ScanType::FullScan(row_count) => {
                row_id_vec = (1..row_count + 1).collect::<Vec<u64>>();
                row_id_vec.as_slice()
            }
            ScanType::ExplicitRowScan(row_ids) => row_ids,
        };

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
        let row = page.read_table_row(row_id, self.schema).unwrap();
        self.current_index += 1;

        Some(Ok(row))
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
    pub row_count: u32,
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
        println!("Get: {:#?}", self.buffer_metadata.free_pages);
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
        println!("Getin Update: {:#?}", self.buffer_metadata.free_pages);
        let np = self.buffer_metadata.free_pages.pop().unwrap();
        if np.0 == page_id {
            println!("Updating page {:?} with current {:#?} ", np.0, np.1);
            self.buffer_metadata.free_pages.push(FreePage(np.0, size));
            println!("Updated value: {:#?}", self.buffer_metadata.free_pages);
        }

        true
    }
}
