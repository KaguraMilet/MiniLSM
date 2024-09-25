use core::panic;
use std::ops::Bound;

use anyhow::{bail, Result};

use crate::{
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    key::{KeyBytes, TimeStamp},
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<KeyBytes>,
    prev_key: Vec<u8>,
    read_ts: TimeStamp,
}

impl LsmIterator {
    pub(crate) fn new(
        iter: LsmIteratorInner,
        end_bound: Bound<KeyBytes>,
        read_ts: TimeStamp,
    ) -> Result<Self> {
        // First get the ownership of LsmIteratorInner to get the mutable reference
        let mut lsm_iter = LsmIterator {
            inner: iter,
            end_bound,
            prev_key: Vec::new(),
            read_ts,
        };
        lsm_iter.next_valid()?;
        Ok(lsm_iter)
    }

    // Only return the latest version of a key if multiple versions of the keys are retrieved from the child iterator
    // and its timestamp should below or equal to `LsmIterator::read_ts`
    fn next_valid(&mut self) -> Result<()> {
        while self.is_valid() {
            let (key, curr_ts) = {
                let key = self.inner.key();
                (key.key_ref(), key.ts())
            };
            if self.prev_key == key || self.read_ts < curr_ts {
                self.inner.next()?;
                continue;
            }
            self.prev_key = key.to_vec();
            if self.inner.value().is_empty() {
                self.inner.next()?;
                continue;
            }
            break;
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    #[inline(always)]
    fn is_valid(&self) -> bool {
        self.inner.is_valid()
            && match &self.end_bound {
                Bound::Included(key) => self.inner.key().key_ref() <= key.key_ref(),
                Bound::Excluded(key) => self.inner.key().key_ref() < key.key_ref(),
                Bound::Unbounded => true,
            }
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        assert!(self.is_valid());
        self.inner.next()?;
        self.next_valid()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if self.has_errored || !self.iter.is_valid() {
            panic!("key() called on invalid iterator");
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if self.has_errored || !self.iter.is_valid() {
            panic!("value() called on invalid iterator");
        }
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("next() called on errored iterator");
        }

        if self.iter.is_valid() {
            if let Err(e) = self.iter.next() {
                self.has_errored = true;
                return Err(e);
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
