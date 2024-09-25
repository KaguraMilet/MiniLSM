pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, ensure, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice, TimeStamp};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block(The start offset in the SSTable).
    pub offset: usize,
    /// Fence pointer
    /// The first key of the data block.(smallest key)
    pub first_key: KeyBytes,
    /// The last key of the data block.(largest key)
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta_with_ts(
        block_meta: &[BlockMeta],
        buf: &mut Vec<u8>,
        max_ts: TimeStamp,
    ) {
        let mut estimated_size = std::mem::size_of::<TimeStamp>();
        block_meta.iter().for_each(|blk_meta| {
            // The size of offset of this block
            estimated_size += std::mem::size_of::<u32>();
            // The length of first key
            estimated_size += std::mem::size_of::<u16>(); // Refer block format
                                                          // The actual data of first key
            estimated_size += blk_meta.first_key.raw_len();
            // The length of last key
            estimated_size += std::mem::size_of::<u16>();
            // The actual data of last key
            estimated_size += blk_meta.last_key.raw_len();
        });

        buf.reserve(estimated_size);
        // Buf represent the SSTable in memory
        let original_len = buf.len();
        block_meta.iter().for_each(|blk_meta| {
            buf.put_u32(blk_meta.offset as u32);
            buf.put_u16(blk_meta.first_key.key_len() as u16);
            buf.put_slice(blk_meta.first_key.key_ref());
            buf.put_u64(blk_meta.first_key.ts());
            buf.put_u16(blk_meta.last_key.key_len() as u16);
            buf.put_slice(blk_meta.last_key.key_ref());
            buf.put_u64(blk_meta.last_key.ts());
        });
        buf.put_u64(max_ts);
        assert_eq!(original_len + estimated_size, buf.len());
        let checksum = crc32fast::hash(&buf[original_len..]);
        buf.put_u32(checksum);
    }

    /// Decode block meta from a buffer.
    /// This function assume the buffer only contain meta data section
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut block_meta = Vec::new();
        while buf.has_remaining() {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key = buf.copy_to_bytes(first_key_len);
            let first_key_ts = buf.get_u64();
            let last_key_len = buf.get_u16() as usize;
            let last_key = buf.copy_to_bytes(last_key_len);
            let last_key_ts = buf.get_u64();
            block_meta.push(BlockMeta {
                offset,
                first_key: KeyBytes::from_bytes_with_ts(first_key, first_key_ts),
                last_key: KeyBytes::from_bytes_with_ts(last_key, last_key_ts),
            })
        }
        block_meta
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    // Fence pointer of SSTable
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) filter: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: TimeStamp,
}

const SIZE_U32: usize = std::mem::size_of::<u32>();
const SIZE_TIMESTAMP: usize = std::mem::size_of::<u64>();

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        // Get the filter offset
        let filter_meta_pos = file.size() - (SIZE_U32 as u64);
        let raw_filter_offset = file.read(filter_meta_pos, 4)?;
        let filter_start_pos = (&raw_filter_offset[..]).get_u32() as u64;
        // Get the filter section
        let filter_with_checksum = file.read(
            filter_start_pos,
            file.size() - filter_start_pos - SIZE_U32 as u64,
        )?;
        let checksum = (&filter_with_checksum[filter_with_checksum.len() - SIZE_U32..]).get_u32();
        ensure!(
            crc32fast::hash(&filter_with_checksum[..filter_with_checksum.len() - SIZE_U32])
                == checksum,
            "SSTable {id} filter corrupted, checksum mismatch"
        );
        let filter = Bloom::decode(&filter_with_checksum[..filter_with_checksum.len() - SIZE_U32])?;
        // Get the block metadata
        let block_meta_start_pos = filter_start_pos - (SIZE_U32 as u64);
        // Get the metadata section start position
        let raw_meta_offset = file.read(block_meta_start_pos, 4)?;
        // Meta offset represents the start position of blocks metadata and the end of the block section
        let meta_offset = (&raw_meta_offset[..]).get_u32() as u64;
        // Get the block metadata section
        let meta_length = filter_start_pos - meta_offset - 4;
        let meta_with_ts_checksum = file.read(meta_offset, meta_length)?;
        let meta_with_ts_length = meta_with_ts_checksum.len() - SIZE_U32;
        let checksum = (&meta_with_ts_checksum[meta_with_ts_length..]).get_u32();
        let max_ts = (&meta_with_ts_checksum[meta_with_ts_length - SIZE_TIMESTAMP..]).get_u64();
        ensure!(
            crc32fast::hash(&meta_with_ts_checksum[..meta_with_ts_length]) == checksum,
            "SSTable {id} block metadata corrupted, checksum mismatch"
        );
        let block_meta = BlockMeta::decode_block_meta(
            &meta_with_ts_checksum[..meta_with_ts_length - SIZE_TIMESTAMP],
        );
        Ok(Self {
            file,
            first_key: block_meta.first().unwrap().first_key.clone(),
            last_key: block_meta.last().unwrap().last_key.clone(),
            block_meta,
            block_meta_offset: meta_offset as usize,
            id,
            block_cache,
            filter: Some(filter),
            max_ts,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            filter: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let block_meta = self.block_meta.get(block_idx).ok_or(anyhow!(
            "block not found in current SSTable, block_idx: {block_idx}, SSTable id: {}",
            self.id
        ))?;
        // If there is only one block, self.block_meta_offset is the end of the block section
        // indicating the end of the block
        let offset_end = self
            .block_meta
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |b| b.offset);
        // Read checksum and block data section
        let data = self.file.read(
            block_meta.offset as u64,
            (offset_end - block_meta.offset) as u64,
        )?;
        let checksum = (&data[data.len() - SIZE_U32..]).get_u32();
        ensure!(
            crc32fast::hash(&data[..data.len() - SIZE_U32]) == checksum,
            "Block checksum mismatch, block_idx: {block_idx}, SSTable id: {}",
            self.id
        );
        Ok(Arc::new(Block::decode(&data[..data.len() - SIZE_U32])))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(cache) = &self.block_cache {
            // Cache key: (SSTable id, block index)
            let block = cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!("{}", e))?;
            Ok(block)
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        // Consider the case, we seek key: d
        // |         Block Meta         |
        // |----------------------------|
        // |   1: a/d   ;  2: e/g       |
        // So the partition position will point to the block 2
        // the given key may exist in block 1 or block 2
        // so saturating_sub(1) will make pointer to block 1
        // we should first check the block 1, if iterator is invalid
        // we should check the block 2
        self.block_meta
            .partition_point(|meta| meta.first_key.as_key_slice() <= key)
            .saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    #[inline(always)]
    pub fn sst_id(&self) -> usize {
        self.id
    }

    #[inline(always)]
    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
