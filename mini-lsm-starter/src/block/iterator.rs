use bytes::Buf;
use std::cmp::Ordering;
use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::{
    builder::{SIZE_U16, TIMESTAMP_SIZE},
    Block,
};

/// Returns either `true_val` or `false_val` depending on condition `b` with a
/// hint to the compiler that this condition is unlikely to be correctly
/// predicted by a CPU's branch predictor (e.g. a binary search).
/// Inspired by Rust compiler
#[inline]
fn select_unpredictable<T>(b: bool, true_val: T, false_val: T) -> T {
    if b {
        true_val
    } else {
        false_val
    }
}

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            first_key: BlockIterator::decode_first_key(&block),
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        debug_assert!(!self.key.is_empty(), "key should not be empty");
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_idx(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to_idx(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut size = self.block.offsets.len();
        let mut base: usize = 0;
        while size > 1 {
            let half = size / 2;
            let mid = base + half;
            self.seek_to_idx(mid);
            assert!(self.is_valid());
            let cmp = self.key().cmp(&key);
            base = select_unpredictable(cmp == Ordering::Greater, base, mid);
            size -= half;
        }
        self.seek_to_idx(base);
        let cmp = self.key().cmp(&key);
        if cmp == Ordering::Equal {
        } else {
            self.seek_to_idx(base + (cmp == Ordering::Less) as usize);
        }
    }

    fn seek_to_idx(&mut self, idx: usize) {
        // If index is out of bounds, make current iterator invalid
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }
        let offset = self.block.offsets[idx] as usize;
        self.seek_to_offset(offset);
        self.idx = idx;
    }

    fn seek_to_offset(&mut self, offset: usize) {
        let mut entry = &self.block.data[offset..];
        // Get the length of the key
        let overlap_len = entry.get_u16() as usize;
        let rest_len = entry.get_u16() as usize;
        let rest_key = &entry[..rest_len];
        entry.advance(rest_len);
        // Get the timestamp
        let ts = entry.get_u64();

        // Set the current key
        self.key.clear();
        self.key.append(&self.first_key.key_ref()[..overlap_len]);
        self.key.append(rest_key);
        self.key.set_ts(ts);
        // Process the value
        let value_len = entry.get_u16() as usize;
        // offset + sizeof(key overlap length) + sizeof(rest key length) + sizeof(key) + sizeof(timestamp) + sizeof(value length)
        let value_offset_begin = offset + 2 * SIZE_U16 + rest_len + TIMESTAMP_SIZE + SIZE_U16;
        let value_offset_end = value_offset_begin + value_len;
        self.value_range = (value_offset_begin, value_offset_end);
    }

    fn decode_first_key(block: &Arc<Block>) -> KeyVec {
        let mut buf = &block.data[..];
        let overlap_len = buf.get_u16() as usize;
        assert_eq!(overlap_len, 0, "first key overlap length should be 0");
        let key_len = buf.get_u16() as usize;
        let first_key = &buf[..key_len];
        buf.advance(key_len);
        let ts = buf.get_u64();
        KeyVec::from_vec_with_ts(first_key.to_vec(), ts)
    }
}
