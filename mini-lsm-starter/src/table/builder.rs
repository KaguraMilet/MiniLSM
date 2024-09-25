use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;
use farmhash::fingerprint32;

use super::bloom::Bloom;
use super::{BlockMeta, FileObject, SsTable};
use crate::key::{Key, KeyVec, TimeStamp};
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec, // First key of every block
    last_key: KeyVec,  // Last key of every block
    last_hash: u32,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>, // Each block contains meta info
    block_size: usize,
    hashs: Vec<u32>,
    max_ts: TimeStamp,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            last_hash: Default::default(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            hashs: Vec::new(),
            max_ts: TimeStamp::default(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }
        // Hash sharing
        if self.last_key.as_key_slice().key_ref() != key.key_ref() {
            let hash = fingerprint32(key.key_ref());
            self.hashs.push(hash);
            self.last_hash = hash;
        } else {
            self.hashs.push(self.last_hash);
        }
        if self.builder.add(key, value) {
            self.last_key.set_from_slice(key);
            self.max_ts = self.max_ts.max(key.ts());
            return;
        }
        self.freeze_block();
        assert!(self.builder.add(key, value));
        self.first_key.set_from_slice(key);
        self.last_key.set_from_slice(key);
        self.max_ts = self.max_ts.max(key.ts());
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    /// build() method will encode the SSTable
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.freeze_block();
        let mut buf = self.data;
        let metadata_offset = buf.len();
        // Encoding largest timestamp with block metadata section
        BlockMeta::encode_block_meta_with_ts(&self.meta, &mut buf, self.max_ts);
        // Metadata offset represents the start position of blocks metadata
        buf.put_u32(metadata_offset as u32);
        let filter = Bloom::build_from_key_hashes(
            &self.hashs,
            Bloom::bloom_bits_per_key(self.hashs.len(), 0.01),
        );
        let filter_offset = buf.len();
        filter.encode(&mut buf);
        buf.put_u32(filter_offset as u32);
        let first_key: KeyBytes = self
            .meta
            .first()
            .map_or(Key::default(), |blk_meta| blk_meta.first_key.clone());
        let last_key: KeyBytes = self
            .meta
            .last()
            .map_or(Key::default(), |blk_meta| blk_meta.last_key.clone());
        // Encode the SSTable and write it to disk
        let file = FileObject::create(path.as_ref(), buf)?;
        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset: metadata_offset,
            id,
            block_cache,
            first_key,
            last_key,
            filter: Some(filter),
            max_ts: self.max_ts,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty() && self.builder.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }

    fn freeze_block(&mut self) {
        // Get the original block
        let block = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        log::debug!(
            "Froze block and current block size: {}",
            block.current_size()
        );
        let encoded_block = block.build().encode();
        let blk_meta = BlockMeta {
            offset: self.data.len(),
            first_key: std::mem::take(&mut self.first_key).into_key_bytes(),
            last_key: std::mem::take(&mut self.last_key).into_key_bytes(),
        };
        self.meta.push(blk_meta);
        let checksum = crc32fast::hash(&encoded_block);
        self.data.extend(encoded_block);
        self.data.put_u32(checksum);
    }
}
