use std::{
    ops::Bound,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{bail, Result};
use bytes::Bytes;
use crossbeam_skiplist::{map::Entry, SkipMap};
use foldhash::HashSet;
use log::info;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
};

use super::CommittedTxnData;

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Read and write set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u64>, HashSet<u64>)>>,
}

fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.committed.load(Ordering::Relaxed) {
            return Err(anyhow::anyhow!("Transaction has been committed"));
        }

        if let Some(key_hashes) = &self.key_hashes {
            let mut read_write_sets = key_hashes.lock();
            read_write_sets.0.insert(farmhash::fingerprint64(key));
        }

        if let Some(entry) = self.local_storage.get(key) {
            return Ok(Some(entry.value().clone()).filter(|v| !v.is_empty()));
        }
        self.inner.get_with_ts(key, self.read_ts)
    }

    /// In current implementation, we don't guarantee serializable snapshot isolation for `Scan` operation
    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        if self.committed.load(Ordering::Relaxed) {
            return Err(anyhow::anyhow!("Transaction has been committed"));
        }

        let mut txn_local_iter: TxnLocalIterator = TxnLocalIteratorBuilder {
            map: self.local_storage.clone(),
            iter_builder: |map| map.range((map_bound(lower), map_bound(upper))),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        let entry =
            txn_local_iter.with_iter_mut(|iter| TxnLocalIterator::entry_to_item(iter.next()));
        txn_local_iter.with_item_mut(|item| *item = entry);

        let lsm_iter = self.inner.scan_with_ts(lower, upper, self.read_ts)?;
        let merge_iter = TwoMergeIterator::create(txn_local_iter, lsm_iter)?;
        TxnIterator::create(self.clone(), merge_iter)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.committed.load(Ordering::Relaxed) {
            return Err(anyhow::anyhow!("Transaction has been committed"));
        }

        if let Some(key_hashes) = &self.key_hashes {
            let mut read_write_sets = key_hashes.lock();
            read_write_sets.1.insert(farmhash::fingerprint64(key));
        }

        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        if self.committed.load(Ordering::Relaxed) {
            return Err(anyhow::anyhow!("Transaction has been committed"));
        }

        if let Some(key_hashes) = &self.key_hashes {
            let mut read_write_sets = key_hashes.lock();
            read_write_sets.1.insert(farmhash::fingerprint64(key));
        }

        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::new());
        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
        self.committed
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .map_err(|_| anyhow::anyhow!("Transaction has been committed"))?;

        let _commit_guard = self.inner.mvcc().commit_lock.lock();

        if !self.serializable_validation() {
            bail!("Serializable validation failed, transaction abort");
        }

        let batch = self
            .local_storage
            .iter()
            .map(|entry| {
                if entry.value().is_empty() {
                    WriteBatchRecord::Del(entry.key().clone())
                } else {
                    WriteBatchRecord::Put(entry.key().clone(), entry.value().clone())
                }
            })
            .collect::<Vec<_>>();

        let commit_ts = self.inner.write_batch_inner(&batch)?;

        if let Some(key_hashes) = &self.key_hashes {
            let write_set = std::mem::take(&mut key_hashes.lock().1);
            let write_data = CommittedTxnData {
                key_hashes: write_set,
                read_ts: self.read_ts,
                commit_ts,
            };
            assert!(self
                .inner
                .mvcc()
                .committed_txns
                .lock()
                .insert(commit_ts, write_data)
                .is_none());

            self.inner.mvcc().vacuum();
        }

        Ok(())
    }

    fn serializable_validation(&self) -> bool {
        assert!(
            self.committed.load(Ordering::Relaxed),
            "Transaction is not committed yet"
        );

        let Some(rw_sets) = &self.key_hashes else {
            return true;
        };
        let (ref read_set, ref write_set) = *rw_sets.lock();
        info!("read_set: {:?}, write_set: {:?}", read_set, write_set);

        if write_set.is_empty() {
            return true;
        }

        let committed_txns = self.inner.mvcc().committed_txns.lock();
        for (_, txn) in committed_txns.range((self.read_ts + 1)..) {
            if !txn.key_hashes.is_disjoint(read_set) {
                return false;
            }
        }

        true
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        let mut ts = self.inner.mvcc().ts.lock();
        ts.1.remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl TxnLocalIterator {
    pub fn empty() -> Self {
        TxnLocalIteratorBuilder {
            map: Arc::default(),
            iter_builder: |map| map.range((Bound::Unbounded, Bound::Unbounded)),
            item: (Bytes::new(), Bytes::new()),
        }
        .build()
    }

    fn entry_to_item(entry: Option<Entry<Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry.map_or((Bytes::new(), Bytes::new()), |entry| {
            (entry.key().clone(), entry.value().clone())
        })
    }
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.borrow_item().1.as_ref()
    }

    fn key(&self) -> &[u8] {
        self.borrow_item().0.as_ref()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let next_entry = self.with_iter_mut(|iter| TxnLocalIterator::entry_to_item(iter.next()));
        self.with_item_mut(|item| *item = next_entry);
        Ok(())
    }
}

pub struct TxnIterator {
    txn: Option<Arc<Transaction>>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        // Track read set for scan
        txn.key_hashes.as_ref().map(|key_hashes| {
            key_hashes
                .lock()
                .0
                .insert(farmhash::fingerprint64(iter.key()))
        });

        Ok(Self {
            txn: Some(txn),
            iter,
        })
    }

    pub fn create_from_lsm_iter(iter: FusedIterator<LsmIterator>) -> Result<Self> {
        let iter = TwoMergeIterator::create(TxnLocalIterator::empty(), iter)?;
        Ok(Self { txn: None, iter })
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a> = &'a [u8] where Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        while self.iter.is_valid() {
            if self.iter.value().is_empty() {
                self.iter.next()?;
                continue;
            }
            // Track read set for scan
            self.txn
                .as_ref()
                .and_then(|txn| txn.key_hashes.as_ref())
                .map(|key_hashes| {
                    key_hashes
                        .lock()
                        .0
                        .insert(farmhash::fingerprint64(self.iter.key()))
                });
            break;
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
