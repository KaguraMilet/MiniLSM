use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice, TimeStamp, TS_DEFAULT, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<KeySlice>) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(x) => Bound::Included(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x.key_ref()),
            x.ts(),
        )),
        Bound::Excluded(x) => Bound::Excluded(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x.key_ref()),
            x.ts(),
        )),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(id: usize) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            map: Arc::new(SkipMap::new()),
            wal: Some(Wal::create(path)?),
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let map = SkipMap::new();
        let mut table_size: usize = 0;
        let wal = Wal::recover(path, &map, &mut table_size)?;
        Ok(Self {
            map: Arc::new(map),
            wal: Some(wal),
            id,
            approximate_size: Arc::new(AtomicUsize::new(table_size)),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(KeySlice::from_slice(key, TS_DEFAULT), value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(KeySlice::from_slice(key, TS_DEFAULT))
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        let lower = match lower {
            Bound::Included(key) => Bound::Included(KeySlice::from_slice(key, TS_RANGE_BEGIN)),
            Bound::Excluded(key) => Bound::Excluded(KeySlice::from_slice(key, TS_RANGE_END)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let upper = match upper {
            Bound::Included(key) => Bound::Included(KeySlice::from_slice(key, TS_RANGE_END)),
            Bound::Excluded(key) => Bound::Excluded(KeySlice::from_slice(key, TS_RANGE_BEGIN)),
            Bound::Unbounded => Bound::Unbounded,
        };
        self.scan(lower, upper)
    }

    /// Get a value by key.
    pub fn get(&self, key: KeySlice) -> Option<Bytes> {
        let key = KeyBytes::from_bytes_with_ts(
            // Safety: convert key slice lifetime to static
            Bytes::from_static(unsafe { std::mem::transmute::<&[u8], &[u8]>(key.key_ref()) }),
            key.ts(),
        );
        self.map.get(&key).map(|entry| entry.value().clone())
    }

    pub fn get_with_ts(&self, key: KeySlice) -> Option<Bytes> {
        let iter = self.scan(
            Bound::Included(key),
            Bound::Included(KeySlice::from_slice(key.key_ref(), TS_RANGE_END)),
        );
        // Return newest timestamp entry
        iter.is_valid()
            .then(|| Bytes::copy_from_slice(iter.value()))
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    pub fn put_batch(&self, batch: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut estimated_size = 0;
        batch.iter().for_each(|(key, value)| {
            estimated_size += key.raw_len() + value.len();
            self.map.insert(
                KeyBytes::from_bytes_with_ts(Bytes::copy_from_slice(key.key_ref()), key.ts()),
                Bytes::copy_from_slice(value),
            );
        });
        if let Some(ref wal) = self.wal {
            wal.put_batch(batch)?;
        }
        self.approximate_size
            .fetch_add(estimated_size, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<KeySlice>, upper: Bound<KeySlice>) -> MemTableIterator {
        let (lower, upper) = (map_bound(lower), map_bound(upper));
        let mut mem_iter: MemTableIterator = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range((lower, upper)),
            item: (KeyBytes::new(), Bytes::new()),
        }
        .build();
        let entry = mem_iter.with_iter_mut(|x| MemTableIterator::entry_to_item(x.next()));
        mem_iter.with_item_mut(|item| *item = entry);
        mem_iter
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            builder.add(
                KeySlice::from_slice(entry.key().key_ref(), entry.key().ts()),
                entry.value(),
            );
        }
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn max_ts(&self) -> TimeStamp {
        self.wal
            .as_ref()
            .map(|wal| wal.max_ts())
            .ok_or_else(|| anyhow::anyhow!("WAL is not supported"))
            .unwrap()
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (KeyBytes, Bytes),
}

impl MemTableIterator {
    fn entry_to_item(entry: Option<Entry<KeyBytes, Bytes>>) -> (KeyBytes, Bytes) {
        // If the entry is None, it means the iterator is at the end.
        entry.map_or((KeyBytes::new(), Bytes::new()), |entry| {
            (entry.key().clone(), entry.value().clone())
        })
    }
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.borrow_item().1.as_ref()
    }

    fn key(&self) -> KeySlice {
        self.borrow_item().0.as_key_slice()
    }

    fn is_valid(&self) -> bool {
        // Key should not be empty
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let next_entry = self.with_iter_mut(|iter| MemTableIterator::entry_to_item(iter.next()));
        self.with_item_mut(|item| *item = next_entry);
        Ok(())
    }
}
