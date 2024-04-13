use core::hash;
use log::debug;
use sqlparser::ast::{Expr, Values};
use std::{
    collections::HashMap,
    hash::Hash,
    ops::{Shl, Shr},
};

use crate::{query::NaadanRecord, storage::catalog::*};
use crate::{query::RecordSet, utils::log};

use super::{
    page::{CatalogPage, Page, PageData, PageId},
    CatalogEngine, NaadanError, RowIdType, StorageEngine, TableIdType,
};

/// Storage Engine
#[derive(Debug)]
pub struct NaadanStorageEngine {
    pub(crate) catalog_page: HashMap<usize, Page>,
    pub(crate) buffer_pool: BufferPool,
    pub(crate) row_index: HashMap<usize, PageId>,
    pub(crate) table_index: HashMap<TableIdType, TableMetadata>,
    pub(crate) engine_metadata: StorageEngineMetadata,
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
            buffer_pool,
            row_index,
            engine_metadata: StorageEngineMetadata::new(table_count as usize),
            catalog_page: HashMap::from([(1, catalog_page)]),
            table_index: HashMap::new(),
        }
    }
}

impl StorageEngine for NaadanStorageEngine {
    fn write_table_rows(
        &mut self,
        row_values: RecordSet,
        table: &Table,
    ) -> Result<RowIdType, NaadanError> {
        let mut table_pages: Vec<PageId> = vec![];
        let mut page_id: PageId;
        let mut row_id: u32;

        // println!("Storage engine: {:?}", self);
        let page: &mut Page = match self.table_index.get_mut(&(table.id as usize)) {
            Some(val) => {
                table_pages.append(&mut val.page_ids.clone());
                page_id = table_pages.last().unwrap().clone();
                // TODO: make these updated atomic
                row_id = val.row_count + 1;
                val.row_count += row_values.count() as u32;
                let page = self.buffer_pool.page_pool.get_mut(&page_id).unwrap();
                // TODO: check if the page has enough space
                page
            }
            None => {
                page_id = PageId::new(table.id, 1);
                row_id = 1;
                let page = Page::new_with_capacity(8 * 1024);
                self.buffer_pool.page_pool.insert(page_id, page);
                self.table_index.insert(
                    table.id as usize,
                    TableMetadata {
                        page_ids: vec![page_id],
                        row_count: row_values.count() as u32,
                    },
                );
                let page = self.buffer_pool.page_pool.get_mut(&page_id).unwrap();
                page
            }
        };

        let row_end: u32 = page.write_table_row(row_id, row_values, table).unwrap() as u32;
        page.write_to_disk(page_id).unwrap();

        for r_id in row_id..row_end {
            self.row_index.insert(r_id as usize, page_id);
        }

        println!("{:?}", self.row_index);

        let read_page = Page::read_from_disk(page_id).unwrap();
        read_page.read_table_row(&(row_id as usize), table).unwrap();

        //println!("{:?}", page);

        Ok(row_id as usize)
    }

    fn read_table_rows(&self, row_ids: &[usize], schema: &Table) -> Result<RecordSet, NaadanError> {
        let mut rows = RecordSet::new(vec![]);

        for r_id in row_ids {
            let page_id = match self.row_index.get(r_id) {
                Some(pageid) => pageid,
                None => return Ok(RecordSet::new(vec![])),
            };

            let page = self.buffer_pool.get(&page_id).unwrap();
            let row = page.read_table_row(&r_id, schema).unwrap();

            rows.add_record(row);
        }

        Ok(rows)
    }

    fn delete_table_rows(
        &self,
        row_ids: &[usize],
        schema: &Table,
    ) -> Result<RecordSet, NaadanError> {
        todo!()
    }

    fn update_table_rows(
        &mut self,
        row_ids: &[usize],
        updated_columns: HashMap<&str, Expr>,
        schema: &Table,
    ) -> Result<&[RowIdType], NaadanError> {
        for r_id in row_ids {
            let page_id = match self.row_index.get(r_id) {
                Some(pageid) => pageid,
                None => continue,
            };

            let page = self.buffer_pool.get_mut(&page_id).unwrap();
            let row = page
                .update_table_row(&r_id, &updated_columns, schema)
                .unwrap();
        }

        Ok(&[0])
    }

    fn scan_table(
        &self,
        predicate: Option<crate::query::plan::ScalarExprType>,
        schema: &Table,
    ) -> Result<RecordSet, NaadanError> {
        let mut row_collection: RecordSet = RecordSet::new(vec![]);

        match self.table_index.get(&(schema.id as usize)) {
            Some(val) => {
                for r_id in 1..val.row_count + 1 {
                    let page_id = self.row_index.get(&(r_id as usize)).unwrap();
                    let page = self.buffer_pool.get(&page_id).unwrap();

                    // TODO: push down predicate
                    let row = page.read_table_row(&(r_id as usize), schema).unwrap();

                    row_collection.add_record(row);
                }
            }
            _ => return Err(NaadanError::TableNotFound),
        };

        Ok(row_collection)
    }
}

impl CatalogEngine for NaadanStorageEngine {
    fn get_table_details(&self, name: &String) -> Result<Table, NaadanError> {
        self.catalog_page.get(&1).unwrap().get_table_details(name)
    }

    fn add_table_details(&mut self, table: &mut Table) -> Result<TableIdType, NaadanError> {
        let catalog_page = self.catalog_page.get_mut(&1).unwrap();
        table.id = self.engine_metadata.table_count as u16 + 1;
        self.engine_metadata.table_count += 1;
        match catalog_page.write_table_details(table) {
            Ok(_) => {}
            Err(err) => return Err(err),
        }
        match catalog_page.write_catalog_to_disk() {
            Ok(_) => {}
            Err(err) => return Err(err),
        }

        Ok(10)
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
