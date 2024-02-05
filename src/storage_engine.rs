use crate::utils::log;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use std::fs::File;
use std::io::prelude::*;

pub trait StorageEngine: std::fmt::Debug {
    fn read_row(&mut self, row_id: usize) -> Result<RowData, usize>;

    fn read_rows(&mut self, row_id: &[usize]) -> Result<(), ()>;

    fn write_row(&mut self, row_id: &usize, row_data: &RowData) -> Result<usize, ()>;

    fn write_rows(&mut self, row_data: Vec<Vec<u8>>) -> Result<(), ()>;

    fn reset_memory(&mut self);
}

// Storage Engine implementation
#[derive(Debug)]
pub struct OurStorageEngine {
    buffer_pool: BufferPool,
    row_index: HashMap<usize, usize>,
    engine_metadata_store: Vec<usize>,
}
impl OurStorageEngine {
    pub fn init(page_count: usize) -> Self {
        Self {
            buffer_pool: BufferPool {
                buffer_metadata: "metadata".to_string(),
                page_pool: HashMap::with_capacity(page_count),
            },
            row_index: HashMap::new(),
            engine_metadata_store: vec![],
        }
    }
}

impl StorageEngine for OurStorageEngine {
    fn reset_memory(&mut self) {
        log(format!("Resetting StorageEngine memory !!"));
        self.buffer_pool.page_pool.clear();
    }

    fn read_row(&mut self, row_id: usize) -> Result<RowData, usize> {
        let index_result = self.row_index.get(&row_id);
        match index_result {
            Some(page_id) => {
                log(format!(
                    "Page Id equivalent to row id {} is {}",
                    row_id, page_id
                ));

                let page_result = self.buffer_pool.get(&page_id);
                match page_result {
                    Some(page) => {
                        log(format!("Reading page from the buffer pool!!"));
                        // Page present in the buffer pool.
                        page.read_row(&row_id)
                    }
                    None => {
                        log(format!("Reading page from the disk!!"));
                        // Page is not present in the buffer pool, need to fetch it from the read_from_disk
                        let res = match Page::read_from_disk(&page_id) {
                            Ok(result) => {
                                let _ = self.buffer_pool.add(page_id.clone(), &result);
                                return Ok(result.read_row(&row_id).unwrap().clone());
                            }
                            Err(err) => {
                                log(format!("Fetching latest page gave error: {}", err));
                                return Err(400);
                            }
                        };
                    }
                };
            }
            None => {}
        }

        Err(400)
    }

    fn read_rows(&mut self, row_id: &[usize]) -> Result<(), ()> {
        Err(())
    }

    fn write_row(&mut self, row_id: &usize, row_data: &RowData) -> Result<usize, ()> {
        // Check if any Page is having space
        let page = match self.engine_metadata_store.last() {
            None => {
                let page_id = 1 as usize;
                let mut page = Page::new();
                self.engine_metadata_store.push(page_id);
                page.write_row(row_id, row_data);
                let _ = page.write_to_disk(&page_id);

                self.row_index.insert(*row_id, page_id);
            }
            Some(page_id) => {
                let page_result = self.buffer_pool.get(page_id);
                match page_result.clone() {
                    Some(page) => {
                        log(format!("Reading page is present in buffer pool!!"));
                        // Page present in the buffer pool.

                        let mut page_copy = page.clone();
                        page_copy.write_row(row_id, row_data);
                        let _ = page_copy.write_to_disk(&page_id);

                        self.buffer_pool.add(*page_id, &page_copy);
                        self.row_index.insert(*row_id, *page_id);
                    }
                    None => {
                        log(format!("Reading page from the disk!!"));
                        // Page is not present in the buffer pool, need to fetch it from the read_from_disk
                        let res = match Page::read_from_disk(&page_id) {
                            Ok(mut page) => {
                                let _ = self.buffer_pool.add(page_id.clone(), &page);
                                page.write_row(row_id, row_data);
                                let _ = page.write_to_disk(&page_id);

                                self.row_index.insert(*row_id, *page_id);
                            }
                            Err(err) => {
                                unreachable!();
                            }
                        };
                    }
                };
            }
        };

        Ok(200)
    }

    fn write_rows(&mut self, row_data: Vec<Vec<u8>>) -> Result<(), ()> {
        Err(())
    }
}

type RowId = usize;
type RowOffset = usize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageHeader {
    checksum: Vec<u8>,
    offset: HashMap<RowId, RowOffset>,
    page_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowData {
    pub null_map: Vec<usize>,
    pub row_data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Page {
    header: PageHeader,
    data: HashMap<usize, RowData>,
}

impl Page {
    pub fn new() -> Self {
        let page_header = PageHeader {
            checksum: vec![],
            offset: HashMap::new(),
            page_size: 0,
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

    pub fn read_row(&self, row_id: &usize) -> Option<&RowData> {
        log(format!("Read row from page"));
        log(format!("{:?}", self));
        let row_offset = self.header.offset.get(&row_id).unwrap();

        log(format!("{}", row_offset));
        let res = self.data.get(row_offset);

        log(format!("{:?}", res));

        res
    }

    pub fn write_row(&mut self, row_id: &usize, row_data: &RowData) -> Result<usize, usize> {
        log(format!("writing row to page"));

        self.header.offset.insert(*row_id, *row_id);
        self.data.insert(*row_id, row_data.clone());

        Ok(200)
    }

    pub fn read_from_disk(page_id: &usize) -> Result<Page, u32> {
        log(format!("Reading page with id: {} from disk", page_id));

        let mut f = File::open("/tmp/output.bin").unwrap();
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).unwrap();

        let page: Page = bincode::deserialize_from(&buf[..]).unwrap();

        Ok(page)
    }

    pub fn write_to_disk(&self, page_id: &usize) -> Result<(), u32> {
        log(format!("Writing page with id: {} to disk", page_id));
        log(format!("{:?}", self));
        // Encode to something implementing `Write`
        let mut f = File::create("/tmp/output.bin").unwrap();
        bincode::serialize_into(&mut f, &self).unwrap();

        Ok(())
    }
}

#[derive(Debug)]
pub struct BufferPool {
    buffer_metadata: String,
    page_pool: HashMap<usize, Page>,
}

impl BufferPool {
    pub fn get(&mut self, page_id: &usize) -> Option<&Page> {
        match self.page_pool.get(page_id) {
            Some(page) => Some(&page),
            None => None,
        }
    }

    pub fn add(&mut self, page_id: usize, page: &Page) -> Result<usize, u32> {
        self.page_pool.insert(page_id, page.clone());
        Ok(page_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        println!("*** Test Starting ***\n");

        let mut s_engine = OurStorageEngine::init(200);
        let row_data = RowData {
            null_map: vec![1, 2],
            row_data: vec![1, 2],
        };

        let mut row_id: usize = 7;
        s_engine.write_row(&row_id, &row_data);

        println!("\n Reading 1 \n");
        s_engine.read_row(row_id);

        row_id = 2;
        s_engine.write_row(&row_id, &row_data);

        println!("\n Reading 2 \n");
        s_engine.read_row(row_id);

        s_engine.reset_memory();

        println!("\n Reading 3 \n");
        s_engine.read_row(row_id);

        row_id = 3;
        s_engine.write_row(&row_id, &row_data);

        row_id = 4;
        s_engine.write_row(&row_id, &row_data);

        row_id = 5;
        s_engine.write_row(&row_id, &row_data);

        row_id = 6;
        s_engine.write_row(&row_id, &row_data);

        println!("\n Reading 4 \n");
        s_engine.read_row(row_id);

        s_engine.reset_memory();

        println!("\n Reading 5 \n");
        s_engine.read_row(row_id);

        println!("\n *** Test Ended ***\n");
    }
}
