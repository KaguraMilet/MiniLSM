use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::{ensure, Result};
use bytes::{Buf, BufMut};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = Arc::new(Mutex::new(File::create_new(path).map_err(|e| {
            anyhow::anyhow!("Failed to create manifest file, error: {:?}", e)
        })?));
        Ok(Self { file })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .map_err(|e| anyhow::anyhow!("Failed to open manifest file, error: {:?}", e))?;
        let mut data = Vec::with_capacity(file.metadata()?.len() as usize);
        file.read_to_end(&mut data)?;
        let mut data = data.as_slice();
        let mut records = Vec::new();
        while data.has_remaining() {
            let len = data.get_u64() as usize;
            let record_slice = &data[..len];
            data.advance(len);
            let checksum = data.get_u32();
            ensure!(
                crc32fast::hash(record_slice) == checksum,
                "Manifest record corrupted, checksum mismatch"
            );
            records.push(serde_json::from_slice(record_slice).map_err(|e| {
                anyhow::anyhow!("Failed to deserialize manifest record, error: {:?}", e)
            })?);
        }
        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut record = serde_json::to_vec(&record)?;
        let record_len = record.len();
        let checksum = crc32fast::hash(&record);
        record.put_u32(checksum);
        let mut file = self.file.lock();
        file.write_all(&(record_len as u64).to_be_bytes())?;
        file.write_all(&record)?;
        file.sync_all()?;
        Ok(())
    }
}
