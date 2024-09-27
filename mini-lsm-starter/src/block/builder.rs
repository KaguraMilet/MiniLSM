use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

pub const SIZE_U16: usize = std::mem::size_of::<u16>();
pub const TIMESTAMP_SIZE: usize = std::mem::size_of::<u64>();
const OVERLAP_KEY_LEN: usize = 2;
const REST_KEY_LEN: usize = 2;
const VALUE_LENGTH_SIZE: usize = 2;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize, // How many bytes can be stored in this block
    /// The first key in the block, all data are treated as bytes array
    first_key: KeyVec,
}

fn mismatch(lhs: &[u8], rhs: &[u8]) -> usize {
    mismatch_chunks::<128>(lhs, rhs)
}

fn mismatch_chunks<const N: usize>(lhs: &[u8], rhs: &[u8]) -> usize {
    let off = std::iter::zip(lhs.chunks_exact(N), rhs.chunks_exact(N))
        .take_while(|(a, b)| a == b)
        .count()
        * N;
    off + std::iter::zip(&lhs[off..], &rhs[off..])
        .take_while(|(a, b)| a == b)
        .count()
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key should not be empty");
        let overlap_len = mismatch(self.first_key.key_ref(), key.key_ref());
        let rest_key = &key.key_ref()[overlap_len..];

        let entry_size = OVERLAP_KEY_LEN
            + REST_KEY_LEN
            + rest_key.len()
            + TIMESTAMP_SIZE
            + VALUE_LENGTH_SIZE
            + value.len();
        if self.data.len() + self.offsets.len() * SIZE_U16 + entry_size > self.block_size
            && !self.is_empty()
        {
            return false;
        }

        self.offsets.push(self.data.len() as u16);
        // Encode the key
        self.data.put_u16(overlap_len as u16);
        self.data.put_u16((key.key_len() - overlap_len) as u16);
        self.data.put(rest_key);
        // Encode the timestamp
        self.data.put_u64(key.ts());
        // Encode the value
        self.data.put_u16(value.len() as u16);
        self.data.put(value);

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty() && self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
