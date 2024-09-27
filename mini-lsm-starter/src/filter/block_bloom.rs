//! Bloom filter implementation inspired by Parquet, as described
//! in the [spec][parquet-bf-spec]
//!
//! # Bloom filter Size
//! Parquet uses the [Split Block Bloom Filter][sbbf-paper] (SBBF) as its bloom filter
//! implementation. The size of each filter is initialized using a calculation based on
//! the membership of n elements and false positive rate (FPR). The FPR for a SBBF can be
//! approximated as<sup>[1][bf-formulae]</sup>:
//!
//! ```text
//! f = (1 - exp(-k * n / m))^k
//! ```
//!
//! Where, `f` is the FPR, `k` the number of hash functions, `n` the number of elements, and `m` the total number
//! of bits in the bloom filter. This can be re-arranged to determine the total number of bits
//! required to achieve a given FPR and `n`:
//!
//! ```text
//! m = - k * n / ln(1 - (f)^(1/k))
//! ```
//!
//! SBBFs use eight hash functions to cleanly fit in SIMD lanes<sup>[2][sbbf-paper]</sup>, therefore
//! `k` is set to 8. The SBBF will spread those `m` bits accross a set of `b` blocks that
//! are each 256 bits, i.e., 32 bytes, in size. The number of blocks is chosen as:
//!
//! ```text
//! blocks_num = next_power_of_two(m / 8) /32
//! ```
//!
//! SBBF trades off for false positive rate(FPR) to achieve higher probe speed
//!
//! [parquet-bf-spec]: https://parquet.apache.org/docs/file-format/bloomfilter/
//! [sbbf-paper]: https://arxiv.org/pdf/2101.01719
//! [bf-formulae]: http://tfk.mit.edu/pdf/bloom.pdf

use std::ops::{Index, IndexMut};

use anyhow::Result;
use bytes::{Buf, BufMut, BytesMut};

const SALT: [u32; 8] = [
    0x47b6137b_u32,
    0x44974d91_u32,
    0x8824ad5b_u32,
    0xa2b7289d_u32,
    0x705495c7_u32,
    0x2df1424b_u32,
    0x9efc4947_u32,
    0x5c6bfb31_u32,
];

/// Each block is 256 bits,i.e.32 bytes. Block is broken into 8 continuous words
/// Each word is considered an array of bits. Each bit is set to 1 or 0
#[derive(Debug, Copy, Clone)]
pub(crate) struct Block(pub(crate) [u32; 8]);

impl Block {
    const ZERO: Block = Block([0; 8]);

    // Accept an unsigned 32-bit integer and produce a new block that
    // one and only one bit of each word is set to 1 when we call `mask`
    fn mask(x: u32) -> Block {
        let mut result = [0_u32; 8];
        // Following three loops are written separately to be optimized for vectorization.
        // Inspired from Java Parquet implementation:
        // https://github.com/apache/parquet-java/blob/master/parquet-column/src/main/java/org/apache/parquet/column/values/bloomfilter/BlockSplitBloomFilter.java#L225
        for i in 0..8 {
            result[i] = x.wrapping_mul(SALT[i]);
        }
        for i in 0..8 {
            // Value of `val` is in range [0, 31]
            result[i] >>= 27
        }
        for i in 0..8 {
            result[i] = 1 << result[i];
        }
        Block(result)
    }

    fn insert(&mut self, hash: u32) {
        let mask = Block::mask(hash);
        for i in 0..8 {
            self[i] |= mask[i];
        }
    }

    fn check(&self, hash: u32) -> bool {
        let mask = Block::mask(hash);
        for i in 0..8 {
            // One and only one bit of each word is set to 1
            if self[i] & mask[i] == 0 {
                return false;
            }
        }
        true
    }
}

impl Index<usize> for Block {
    type Output = u32;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl IndexMut<usize> for Block {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.0.index_mut(index)
    }
}

#[derive(Debug, Clone)]
pub struct SBBloom(pub(crate) Vec<Block>);

impl SBBloom {
    pub fn encode(&self, buf: &mut Vec<u8>) {
        let filter_start_pos = buf.len();
        self.0.iter().for_each(|block| {
            block.0.iter().for_each(|word| buf.put_u32(*word));
        });
        let checksum = crc32fast::hash(&buf[filter_start_pos..]);
        buf.put_u32(checksum);
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        let filter = buf
            .chunks_exact(32)
            .map(|mut chunk| {
                let mut new_block = Block::ZERO;
                (0..8).for_each(|i| {
                    new_block[i] = chunk.get_u32();
                });
                new_block
            })
            .collect::<Vec<Block>>();
        Ok(SBBloom(filter))
    }

    pub fn might_contain(&self, hash: u64) -> bool {
        let block_index = self.block_index(hash);
        self.0[block_index].check(hash as u32)
    }

    pub fn build_filter(hashes: &[u64], num_of_bits: usize) -> Self {
        // Total number of bytes spent by Filter
        let block_size = optimal_block_size(num_of_bits / 8);
        let mut filter = BytesMut::with_capacity(block_size);
        filter.resize(block_size, 0);
        let filter = filter
            .chunks_exact(32)
            .map(|chunk| {
                let mut new_block = Block::ZERO;
                // Split block into 8 continuous words
                for (i, word) in chunk.chunks_exact(4).enumerate() {
                    new_block[i] = u32::from_le_bytes(word.try_into().unwrap());
                }
                new_block
            })
            .collect::<Vec<Block>>();
        let mut filter = SBBloom(filter);

        hashes.iter().for_each(|hash| {
            filter.insert(*hash);
        });

        filter
    }

    fn insert(&mut self, hash: u64) {
        let block_index = self.block_index(hash);
        self.0[block_index].insert(hash as u32);
    }

    #[inline]
    fn block_index(&self, hash: u64) -> usize {
        (((hash >> 32).saturating_mul(self.0.len() as u64)) >> 32) as usize
    }
}

/// Bloom filter false positive rate: f = (1 - exp(-k * n / m))^k
/// so m = - k * n / ln(1 - (f)^(1/k))
/// m: number of bits in the filter, k: number of hash functions, n: number of entries inserted into filter
/// f: false positive rate
#[inline]
pub fn number_of_bits(entries_num: usize, false_positive_rate: f64) -> usize {
    assert!(
        (0.0..=1.0).contains(&false_positive_rate),
        "false positive rate must between 0.0 and 1.0"
    );
    // For Split Block Bloom Filter, always use 8 hash functions for SIMD acceleration
    let num_bits = (-8.0 * entries_num as f64) / (1.0 - false_positive_rate.powf(1.0 / 8.0)).ln();
    num_bits as usize
}

const BLOCK_MIN_SIZE: usize = 32;
// Follow the original description of the Parquet documentation: https://parquet.apache.org/docs/file-format/bloomfilter/
const BLOCK_MAX_SIZE: usize = 32 * 1024 * 1024 * 1024;

#[inline]
fn optimal_block_size(num_bytes: usize) -> usize {
    let num_bytes = num_bytes.max(BLOCK_MIN_SIZE);
    let num_bytes = num_bytes.min(BLOCK_MAX_SIZE);
    num_bytes.next_power_of_two()
}

#[cfg(test)]
mod tests {
    use super::*;
    use xxhash_rust::xxh64::xxh64;

    #[test]
    fn test_hash() {
        assert_eq!(xxh64("".as_bytes(), 0), 17241709254077376921);
    }

    #[test]
    fn test_mask() {
        for i in 0..1000000 {
            let result = Block::mask(i);
            assert!(result.0.iter().all(|&x| x.count_ones() == 1));
        }
    }

    #[test]
    fn test_block_insert_and_check() {
        for i in 0..1_000_000 {
            let mut block = Block::ZERO;
            block.insert(i);
            assert!(block.check(i));
        }
    }

    #[test]
    fn test_sbbf_insert_and_check() {
        let mut sbbf = SBBloom(vec![Block::ZERO; 1_000]);
        for i in 0..1_000_000 {
            let key = format!("key_{}", i);
            sbbf.insert(xxh64(key.as_bytes(), 0));
            assert!(sbbf.might_contain(xxh64(key.as_bytes(), 0)));
        }
    }

    #[test]
    fn test_encode_and_decode() {
        let mut hashes = Vec::new();
        for i in 0..500000 {
            let key = format!("key_{}", i);
            hashes.push(xxh64(key.as_bytes(), 0));
        }
        let sbbf = SBBloom::build_filter(&hashes, number_of_bits(hashes.len(), 0.01));
        assert!(hashes.iter().all(|hash| sbbf.might_contain(*hash)));
        let mut buf = Vec::new();
        sbbf.encode(&mut buf);
        let decode_sbbf = SBBloom::decode(&buf[..buf.len() - 4]).unwrap();
        sbbf.0.iter().zip(decode_sbbf.0.iter()).for_each(|(a, b)| {
            assert_eq!(a.0, b.0);
        });
        assert!(hashes.iter().all(|hash| decode_sbbf.might_contain(*hash)));
    }

    #[test]
    fn test_optimal_num_of_bytes() {
        for (input, expected) in &[
            (0, 32),
            (9, 32),
            (31, 32),
            (32, 32),
            (33, 64),
            (99, 128),
            (1024, 1024),
            (999_000_000, 999_000_000_usize.next_power_of_two()),
        ] {
            assert_eq!(*expected, optimal_block_size(*input));
        }
    }

    #[test]
    fn test_num_of_bits_from_ndv_fpp() {
        for (fpp, ndv, num_bits) in &[
            (0.1, 10, 57),
            (0.01, 10, 96),
            (0.001, 10, 146),
            (0.1, 100, 577),
            (0.01, 100, 968),
            (0.001, 100, 1460),
            (0.1, 1000, 5772),
            (0.01, 1000, 9681),
            (0.001, 1000, 14607),
            (0.1, 10000, 57725),
            (0.01, 10000, 96815),
            (0.001, 10000, 146076),
            (0.1, 100000, 577254),
            (0.01, 100000, 968152),
            (0.001, 100000, 1460769),
            (0.1, 1000000, 5772541),
            (0.01, 1000000, 9681526),
            (0.001, 1000000, 14607697),
            (1e-50, 1_000_000_000_000, 14226231280773240832),
        ] {
            assert_eq!(*num_bits, number_of_bits(*ndv, *fpp) as u64);
        }
    }
}
