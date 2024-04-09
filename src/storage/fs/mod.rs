use std::{
    fs::File,
    io::{Read, Seek},
};

use super::NaadanError;

pub struct NaadanFile(File);

impl NaadanFile {
    pub fn read(filename: &str, ) -> Result<NaadanFile, NaadanError> {
        match File::options().read(true).open(filename) {
            Ok(file) => Ok(NaadanFile(file)),
            Err(err) => Err(NaadanError::FileSystemError(err)),
        }
    }

    pub fn seek_from_start(&mut self, offset: u64) -> Result<u64, NaadanError> {
        match self.0.seek(std::io::SeekFrom::Start(offset)) {
            Ok(pos) => Ok(pos),
            Err(err) => Err(NaadanError::FileSystemError(err)),
        }
    }

    pub fn read_to_buf(&mut self, buf: &mut [u8]) -> Result<(), NaadanError> {
        match self.0.read_exact(buf) {
            Ok(_) => Ok(()),
            Err(_) => match self.0.read(buf) {
                // TODO might need to re-read to buffer since read does not guarante read to end
                Ok(_) => Ok(()),
                Err(err) => Err(NaadanError::FileSystemError(err)),
            },
        }
    }
}
