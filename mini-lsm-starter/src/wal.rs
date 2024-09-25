use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, ensure, Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::key::{KeyBytes, KeySlice, TimeStamp};

const SIZE_U64: usize = std::mem::size_of::<u64>();

pub struct Wal {
    // LSM shares a single WAL
    file: Arc<Mutex<BufWriter<File>>>,
    max_ts: TimeStamp,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .append(true)
                    .create_new(true)
                    .open(path)
                    .context("Failed to create wal file")?,
            ))),
            max_ts: TimeStamp::default(),
        })
    }

    pub fn recover(
        path: impl AsRef<Path>,
        skiplist: &SkipMap<KeyBytes, Bytes>,
        table_size: &mut usize,
    ) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .open(path)
            .context("Failed to open wal file")?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let mut reader = buffer.as_slice();
        let mut max_ts = TimeStamp::default();

        while reader.has_remaining() {
            let batch_size = reader.get_u32() as usize;
            if reader.remaining() < batch_size {
                bail!("Incomplete WAL")
            }
            let mut body = &reader[..batch_size];
            reader.advance(batch_size);
            let checksum = reader.get_u32();
            let calculate_checksum = crc32fast::hash(body);
            ensure!(
                calculate_checksum == checksum,
                "WAL record corrupted, checksum mismatch"
            );
            while body.has_remaining() {
                let key_len = body.get_u16() as usize;
                let key = Bytes::copy_from_slice(&body[..key_len]);
                body.advance(key_len);
                let ts = body.get_u64();
                max_ts = max_ts.max(ts);
                let value_len = body.get_u16() as usize;
                let value = Bytes::copy_from_slice(&body[..value_len]);
                body.advance(value_len);
                skiplist.insert(KeyBytes::from_bytes_with_ts(key, ts), value);
                *table_size += key_len + value_len + SIZE_U64;
            }
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
            max_ts,
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    pub fn put_batch(&self, batch: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut body = BytesMut::new();
        batch.iter().for_each(|(key, value)| {
            body.put_u16(key.key_len() as u16);
            body.put_slice(key.key_ref());
            body.put_u64(key.ts());
            body.put_u16(value.len() as u16);
            body.put_slice(value);
        });
        let batch_size = body.len();
        let checksum = crc32fast::hash(&body);
        body.put_u32(checksum);
        let mut file = self.file.lock();
        file.write_all(&(batch_size as u32).to_be_bytes())?;
        file.write_all(&body)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.get_mut().sync_all()?;
        Ok(())
    }

    pub fn max_ts(&self) -> TimeStamp {
        self.max_ts
    }
}
