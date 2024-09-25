use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        Self::check_sst_valid(&sstables);
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            });
        }
        let curr_iter = SsTableIterator::create_and_seek_to_first(
            sstables
                .first()
                .expect("Error to get the first sstable")
                .clone(),
        )?;
        let mut iter = Self {
            current: Some(curr_iter),
            next_sst_idx: 1,
            sstables,
        };
        iter.move_until_valid()?;
        Ok(iter)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        Self::check_sst_valid(&sstables);
        let idx = sstables
            .partition_point(|sst| sst.first_key().as_key_slice() <= key)
            .saturating_sub(1);
        if idx >= sstables.len() {
            return Ok(Self {
                current: None,
                next_sst_idx: sstables.len(),
                sstables,
            });
        }
        let curr_iter = SsTableIterator::create_and_seek_to_key(
            sstables
                .get(idx)
                .ok_or_else(|| anyhow::anyhow!("Error to get the {}th sstable", idx))
                .unwrap()
                .clone(),
            key,
        )?;
        let mut iter = Self {
            current: Some(curr_iter),
            next_sst_idx: idx + 1,
            sstables,
        };
        iter.move_until_valid()?;
        Ok(iter)
    }

    // Move the concat iterator until current sstable iterator is valid
    // if already traversed all sstables, current iterator should be None
    fn move_until_valid(&mut self) -> Result<()> {
        while let Some(iter) = &mut self.current {
            if iter.is_valid() {
                break;
            }
            if self.next_sst_idx >= self.sstables.len() {
                self.current = None;
                break;
            } else {
                self.current = Some(SsTableIterator::create_and_seek_to_first(
                    self.sstables
                        .get(self.next_sst_idx)
                        .ok_or_else(|| {
                            anyhow::anyhow!("Error to get the {}th sstable", self.next_sst_idx)
                        })
                        .unwrap()
                        .clone(),
                )?);
                self.next_sst_idx += 1;
            }
        }
        Ok(())
    }

    fn check_sst_valid(sstables: &[Arc<SsTable>]) {
        assert!(sstables.iter().all(|sst| sst.first_key() <= sst.last_key()));
        // Sstables consist of sorted run, there is no redundancy in the sstables
        // so pair[0].last_key() should always less than pair[1].first_key()
        if !sstables.is_empty() {
            assert!(sstables
                .windows(2)
                .all(|pair| pair[0].last_key() < pair[1].first_key()));
        }
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        self.current.as_ref().is_some_and(|iter| iter.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        if let Some(iter) = &mut self.current {
            iter.next()?;
        }
        // After next(), current iterator may be invalid
        self.move_until_valid()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
