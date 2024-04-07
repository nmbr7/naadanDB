use core::hash;
use sqlparser::ast::Values;
use std::{
    collections::HashMap,
    ops::{Shl, Shr},
};

use crate::helper::log;
use crate::storage::catalog::*;

use super::{
    page::{CatalogPage, Page, PageId, RowData},
    NaadanError, RowIdType, StorageEngine, TableIdType,
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
        // TODO: load catalog data
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

    // pub(crate) fn write_to_existing_page(
    //     &mut self,
    //     free_page: FreePage,
    //     row_id: &usize,
    //     row_data: RowData,
    //     row_size: usize,
    //     mode: WriteType,
    // ) {
    //     match self.buffer_pool.get_mut(free_page.page_id()) {
    //         Ok(page) => {
    //             log(format!("Reading page is present in buffer pool!!"));

    //             page.write_row(row_id, row_data).unwrap();
    //             let page_id = free_page.page_id();
    //             let _ = page.write_to_disk(page_id);
    //             if mode == WriteType::Insert {
    //                 self.buffer_pool
    //                     .update_available_page_size(*page_id, free_page.1 - row_size);
    //             }
    //             self.row_index.insert(*row_id, *free_page.page_id());
    //             log(format!("RowIndex: {:?}", self.row_index));
    //         }
    //         Err(_) => {
    //             log(format!("Reading page from the disk!!"));
    //             // Page is not present in the buffer pool, need to fetch it from the read_from_disk
    //             match Page::read_from_disk(&free_page.page_id()) {
    //                 Ok(page) => {
    //                     let new_page = self.buffer_pool.add(free_page.page_id(), page).unwrap();

    //                     let _ = new_page.write_row(row_id, row_data);
    //                     let _ = new_page.write_to_disk(&free_page.page_id());

    //                     if mode == WriteType::Insert {
    //                         self.buffer_pool.update_available_page_size(
    //                             *free_page.page_id(),
    //                             free_page.1 - row_size,
    //                         );
    //                     }

    //                     self.row_index.insert(*row_id, *free_page.page_id());
    //                     log(format!("RowIndex: {:?}", self.row_index));
    //                 }
    //                 Err(_err) => {
    //                     unreachable!();
    //                 }
    //             };
    //         }
    //     };
    // }

    // pub(crate) fn write_to_page(
    //     &mut self,
    //     free_page: Option<FreePage>,
    //     row_id: &usize,
    //     row_data: RowData,
    //     row_size: usize,
    //     mode: WriteType,
    // ) {
    //     match free_page {
    //         None => {
    //             log(format!("Creating new page."));

    //             let page_id = self.buffer_pool.buffer_metadata.free_pages.len() + 1 as usize;
    //             let mut page = Page::new();

    //             let _ = page.write_row(row_id, row_data);
    //             let _ = page.write_to_disk(&page_id);
    //             self.buffer_pool.add(&page_id, page).unwrap();
    //             self.buffer_pool
    //                 .update_available_page_size(page_id, 4 * 1024 - row_size);
    //             self.row_index.insert(*row_id, page_id);
    //             log(format!("RowIndex: {:?}", self.row_index));
    //         }
    //         Some(free_page) => {
    //             self.write_to_existing_page(free_page, row_id, row_data, row_size, mode);
    //         }
    //     };
    // }
}

impl StorageEngine for NaadanStorageEngine {
    fn get_table_details(&self, name: &String) -> Result<Table, NaadanError> {
        self.catalog_page.get(&1).unwrap().get_table_details(name)
    }

    fn add_table(&mut self, table: &mut Table) -> Result<TableIdType, NaadanError> {
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

    fn add_row_into_table(
        &mut self,
        row_values: Values,
        table: &Table,
    ) -> Result<RowIdType, NaadanError> {
        let mut table_pages: Vec<PageId> = vec![];
        let mut page_id: PageId;
        let mut row_id: u32;

        // debug!("Storage engine: {:?}", self);
        let page: &mut Page = match self.table_index.get_mut(&(table.id as usize)) {
            Some(val) => {
                table_pages.append(&mut val.page_ids.clone());
                page_id = table_pages.last().unwrap().clone();
                // TODO: make these updated atomic
                row_id = val.row_count + 1;
                val.row_count += row_values.rows.len() as u32;
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
                        row_count: row_values.rows.len() as u32,
                    },
                );
                let page = self.buffer_pool.page_pool.get_mut(&page_id).unwrap();
                page
            }
        };

        page.write_table_row(row_id, row_values, table).unwrap();
        page.write_to_disk(page_id).unwrap();

        self.row_index.insert(row_id as usize, page_id);

        let read_page = Page::read_from_disk(page_id).unwrap();
        read_page.read_table_row(&(row_id as usize), table).unwrap();

        //debug!("{:?}", page);

        Ok(row_id as usize)
    }

    // fn reset_memory(&mut self) {
    //     log(format!("Resetting StorageEngine memory !!"));
    //     self.buffer_pool.page_pool.clear();
    // }

    // fn read_row(&mut self, row_id: usize) -> Result<&RowData, bool> {
    //     let index_result = self.row_index.get(&row_id);
    //     match index_result {
    //         Some(page_id) => {
    //             log(format!("RowId {} is in Page {}.", row_id, page_id));
    //             if self.buffer_pool.page_exist(page_id) {
    //                 // Page present in the buffer pool.
    //                 let page = self.buffer_pool.get(page_id).unwrap();
    //                 log(format!("Reading page {} from buffer pool.", page_id));
    //                 return page.read_row(&row_id);
    //             } else {
    //                 // Page is not present in the buffer pool, need to fetch it from the disk
    //                 log(format!("Reading page {} from disk.", page_id));
    //                 match Page::read_from_disk(&page_id) {
    //                     Ok(page) => {
    //                         let new_page = self.buffer_pool.add(page_id, page);
    //                         return new_page.unwrap().read_row(&row_id);
    //                     }
    //                     Err(err) => {
    //                         log(format!("Fetching latest page gave error: {}", err));
    //                         return Err(false);
    //                     }
    //                 };
    //             }
    //         }
    //         None => {}
    //     }

    //     Err(false)
    // }

    fn read_rows(&mut self, row_id: &[usize], schema: &Table) -> Result<Values, NaadanError> {
        let row_id_value = &(row_id.last().unwrap());
        let page_id = self.row_index.get(row_id_value).unwrap();
        let page = self.buffer_pool.get(&page_id).unwrap();

        let row = page.read_table_row(&row_id_value, schema).unwrap();

        Ok(row)
    }

    // fn write_row(&mut self, row_id: &usize, row_data: RowData) -> Result<usize, bool> {
    //     println!("\n\n");
    //     let row_size = 500 * 3;
    //     assert!(row_size < (4 * 1024) as usize, "Invalid row size");
    //     let index_result = self.row_index.get(&row_id);
    //     match index_result {
    //         Some(page_id) => {
    //             let free_page = self.buffer_pool.get_available_page(&page_id);
    //             self.write_to_page(free_page, row_id, row_data, row_size, WriteType::Update);
    //         }
    //         None => {
    //             // Check if any Page is having space
    //             let free_page = self.buffer_pool.get_any_available_page(&row_size);
    //             self.write_to_page(free_page, row_id, row_data, row_size, WriteType::Insert);
    //         }
    //     }

    //     Ok(200)
    // }

    // fn write_rows(&mut self, row_data: Vec<Vec<u8>>) -> Result<Vec<RowData>, bool> {
    //     Err(false)
    // }
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

#[derive(PartialEq, Eq)]
enum WriteType {
    Insert,
    Update,
    Delete,
}
