#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::BTreeSet;
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Weak};

use anyhow::{anyhow, ensure, Result};
use bytes::Bytes;
use foldhash::HashMap;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::db::filename::{generate_filename_static, FileType};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice, TimeStamp, TS_DEFAULT, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{map_bound, MemTable};
use crate::mvcc::txn::{Transaction, TxnIterator};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: HashMap::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub max_manifest_file_size: usize,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            max_manifest_file_size: 1 << 30,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            max_manifest_file_size: 1 << 30,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            max_manifest_file_size: 1 << 30,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

impl CompactionFilter {
    pub fn matches(&self, key: &[u8]) -> bool {
        match self {
            CompactionFilter::Prefix(prefix) => key.starts_with(prefix.as_ref()),
        }
    }
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    // next_manifest_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.inner.sync_dir()?;
        self.flush_notifier.send(()).ok();
        self.compaction_notifier.send(()).ok();

        let mut compaction_guard = self.compaction_thread.lock();
        if let Some(compaction_thread) = compaction_guard.take() {
            compaction_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }
        let mut flush_guard = self.flush_thread.lock();
        if let Some(flush_thread) = flush_guard.take() {
            flush_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        // If WAL is enabled, no demand to flush memtable to disk
        if self.inner.options.enable_wal {
            self.inner.sync()?;
            self.inner.sync_dir()?;
            return Ok(());
        }

        // Flush current active memtable if there is no WAL
        if !self.inner.state.read().memtable.is_empty() {
            let state_lock = self.inner.state_lock.lock();
            self.inner.force_freeze_memtable(&state_lock)?;
        }

        while {
            let snapshot = self.inner.state.read();
            !snapshot.imm_memtables.is_empty()
        } {
            self.inner.force_flush_next_imm_memtable()?;
        }
        self.inner.sync_dir()?;
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = LsmStorageInner::open(path, options)?;
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<Arc<Transaction>> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    #[inline]
    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let path: &Path = path.as_ref();

        if !path.exists() {
            std::fs::create_dir_all(path)?;
        } else {
            ensure!(path.is_dir(), "path is not a directory: {:?}", path);
        }
        log::debug!("Open database: '{:?}'", path);
        let mut state = LsmStorageState::create(&options);
        let mut next_sst_id = 1;
        let mut initial_ts: Option<TimeStamp> = None;
        let block_cache = Arc::new(BlockCache::new(1024));

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        // Recover from manifest
        let manifest_filenum = 0;
        let manifest_filename =
            generate_filename_static(path, FileType::MANIFEST, manifest_filenum);
        let manifest;
        if !manifest_filename.exists() {
            // Create wal for current memtable
            if options.enable_wal {
                state.memtable = Arc::new(MemTable::create_with_wal(
                    state.memtable.id(),
                    generate_filename_static(path, FileType::WAL, state.memtable.id()),
                )?);
            }
            log::debug!("Create manifest: '{:?}'", manifest_filename);
            manifest = Manifest::create(manifest_filename)?;
            // Record memtable create operation in `LsmStorageState::create(args)`
            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
        } else {
            let (manifest_, records) = Manifest::recover(manifest_filename)?;
            let mut memtable_ids = BTreeSet::new();
            for record in records {
                match record {
                    ManifestRecord::Flush(id) => {
                        // Freeze memtable should happen before flush immutable memtables to sstables
                        // So every flush operation should observe the `ManifestRecord::NewMemtable` record
                        // In other words, if memtable_ids does not contain flush id, it means there is inconsistency
                        assert!(
                            memtable_ids.remove(&id),
                            "memtable {id} not exist, inconsistency"
                        );
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, id);
                        } else {
                            // Tiered compaction
                            state.levels.insert(0, (id, vec![id]));
                        }
                        next_sst_id = next_sst_id.max(id);
                    }
                    ManifestRecord::NewMemtable(id) => {
                        next_sst_id = next_sst_id.max(id);
                        memtable_ids.insert(id);
                    }
                    ManifestRecord::Compaction(task, new_sst_ids) => {
                        // Recover LSM state from oldest to newest(apply compaction result)
                        let (new_state, _) = compaction_controller.apply_compaction_result(
                            &state,
                            &task,
                            &new_sst_ids,
                        );
                        state = new_state;
                        next_sst_id =
                            next_sst_id.max(new_sst_ids.iter().max().copied().unwrap_or_default());
                    }
                }
            }

            // Load sstables from disk
            for id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().flat_map(|(_, ids)| ids))
            {
                let sst = SsTable::open(
                    *id,
                    Some(block_cache.clone()),
                    FileObject::open(&generate_filename_static(path, FileType::SST, *id))
                        .map_err(|e| anyhow::anyhow!("Error while open sstable {id}: {:?}", e))?,
                )?;
                state.sstables.insert(*id, Arc::new(sst));
            }

            initial_ts = initial_ts.max(state.sstables.values().map(|sst| sst.max_ts()).max());

            next_sst_id += 1;
            // If enable WAL, recover immutable memtables from WAL(if exist) and create WAL for current memtable
            if options.enable_wal {
                for id in memtable_ids {
                    let wal_path = generate_filename_static(path, FileType::WAL, id);
                    ensure!(wal_path.exists(), "WAL file for memtable {id} not exist");
                    let memtable = MemTable::recover_from_wal(id, wal_path)?;
                    state.imm_memtables.insert(0, Arc::new(memtable));
                }
                state.memtable = Arc::new(MemTable::create_with_wal(
                    next_sst_id,
                    generate_filename_static(path, FileType::WAL, next_sst_id),
                )?);
                initial_ts = initial_ts.max(
                    std::iter::once(&state.memtable)
                        .chain(state.imm_memtables.iter())
                        .map(|memtable| memtable.max_ts())
                        .max(),
                );
            } else {
                state.memtable = Arc::new(MemTable::create(next_sst_id));
            }
            manifest_.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
            next_sst_id += 1;
            manifest = manifest_;
        }

        let initial_ts = initial_ts.unwrap_or(TS_DEFAULT);
        let storage = Arc::new_cyclic(|storage| {
            let mvcc = LsmMvccInner::new(initial_ts, Weak::clone(storage));

            Self {
                state: Arc::new(RwLock::new(Arc::new(state))),
                state_lock: Mutex::new(()),
                path: path.to_path_buf(),
                block_cache,
                next_sst_id: AtomicUsize::new(next_sst_id),
                compaction_controller,
                manifest: Some(manifest),
                options: options.into(),
                mvcc: Some(mvcc),
                compaction_filters: Arc::new(Mutex::new(Vec::new())),
            }
        });

        storage.sync_dir()?;

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Ok(txn) = self.new_txn() {
            txn.get(key)
        } else {
            self.get_with_ts(key, TS_DEFAULT)
        }
    }

    pub fn get_with_ts(&self, key: &[u8], timestamp: TimeStamp) -> Result<Option<Bytes>> {
        let key = KeySlice::from_slice(key, timestamp);
        let key_hash = xxhash_rust::xxh64::xxh64(key.key_ref(), 0);
        let snapshot = {
            let guard = self.state.read();
            guard.clone()
        };

        if let Some(value) = snapshot.memtable.get_with_ts(key) {
            return Ok(Some(value).filter(|v| !v.is_empty()));
        }

        for memtable in snapshot.imm_memtables.iter() {
            if let Some(value) = memtable.get_with_ts(key) {
                return Ok(Some(value).filter(|v| !v.is_empty()));
            }
        }

        let filter_table = |key: &[u8], table: &SsTable, key_hash: u64| {
            if Self::is_key_exist(key, (table.first_key(), table.last_key()))
                && table
                    .filter
                    .as_ref()
                    .is_some_and(|filter| filter.might_contain(key_hash))
            {
                return true;
            }
            false
        };

        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for id in snapshot.l0_sstables.iter() {
            if let Some(sst) = snapshot.sstables.get(id) {
                if filter_table(key.key_ref(), sst, key_hash) {
                    l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                        sst.clone(),
                        key,
                    )?));
                }
            }
        }

        let l0_merge_iter = MergeIterator::create(l0_iters);
        if l0_merge_iter.is_valid() && l0_merge_iter.key().key_ref() == key.key_ref() {
            return Ok(
                Some(Bytes::copy_from_slice(l0_merge_iter.value())).filter(|v| !v.is_empty())
            );
        }

        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, ids) in snapshot.levels.iter() {
            let tables = ids
                .iter()
                .filter_map(|id| snapshot.sstables.get(id))
                .filter(|sst| filter_table(key.key_ref(), sst, key_hash))
                .cloned()
                .collect::<Vec<Arc<SsTable>>>();

            if !tables.is_empty() {
                level_iters.push(Box::new(SstConcatIterator::create_and_seek_to_key(
                    tables, key,
                )?));
            }
        }

        let level_merge_iter = MergeIterator::create(level_iters);
        if level_merge_iter.is_valid() && level_merge_iter.key().key_ref() == key.key_ref() {
            return Ok(
                Some(Bytes::copy_from_slice(level_merge_iter.value())).filter(|v| !v.is_empty())
            );
        }

        Ok(None)
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        if !self.options.serializable {
            let _ = self.write_batch_inner(batch)?;
            Ok(())
        } else {
            let txn = self.new_txn()?;
            for record in batch {
                match record {
                    WriteBatchRecord::Put(key, value) => txn.put(key.as_ref(), value.as_ref())?,
                    WriteBatchRecord::Del(key) => txn.delete(key.as_ref())?,
                }
            }
            txn.commit()
        }
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch_inner<T: AsRef<[u8]>>(
        &self,
        batch: &[WriteBatchRecord<T>],
    ) -> Result<TimeStamp> {
        let mvcc = self.mvcc();
        let _mvcc_guard = mvcc.write_lock.lock();
        let timestamp = mvcc.latest_commit_ts() + 1;
        for record in batch {
            match record {
                WriteBatchRecord::Del(key) => {
                    let key = key.as_ref();
                    ensure!(!key.is_empty(), "Key should not be empty");
                    let estimated_size = {
                        let guard = self.state.read();
                        guard
                            .memtable
                            .put(KeySlice::from_slice(key, timestamp), &[])?;
                        guard.memtable.approximate_size()
                    };
                    self.try_freeze_memtable(estimated_size)?;
                }
                WriteBatchRecord::Put(key, value) => {
                    let key = key.as_ref();
                    let value = value.as_ref();
                    ensure!(!key.is_empty(), "Key should not be empty");
                    ensure!(!value.is_empty(), "Value should not be empty");
                    let estimated_size = {
                        let guard = self.state.read();
                        guard
                            .memtable
                            .put(KeySlice::from_slice(key, timestamp), value)?;
                        guard.memtable.approximate_size()
                    };
                    self.try_freeze_memtable(estimated_size)?;
                }
            }
        }
        mvcc.update_commit_ts(timestamp);
        Ok(timestamp)
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if !self.options.serializable {
            self.write_batch_inner(&[WriteBatchRecord::Put(key, value)])?;
        } else {
            let new_txn = self.new_txn()?;
            new_txn.put(key, value)?;
            new_txn.commit()?;
        }
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        if !self.options.serializable {
            self.write_batch_inner(&[WriteBatchRecord::Del(key)])?;
        } else {
            let new_txn = self.new_txn()?;
            new_txn.delete(key)?;
            new_txn.commit()?;
        }
        Ok(())
    }

    pub(crate) fn generate_filename(&self, filetype: FileType, seqnum: usize) -> PathBuf {
        generate_filename_static(&self.path, filetype, seqnum)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let old_memtable = {
            let snapshot = self.state.read();
            snapshot.memtable.clone()
        };

        let new_memtable_id = self.next_sst_id();
        let new_memtable = if self.options.enable_wal {
            let wal_path = self.generate_filename(FileType::WAL, new_memtable_id);
            Arc::new(MemTable::create_with_wal(new_memtable_id, wal_path)?)
        } else {
            Arc::new(MemTable::create(new_memtable_id))
        };

        let mut state_write = self.state.write();
        let state = Arc::make_mut(&mut state_write);
        state.imm_memtables.insert(0, old_memtable);
        state.memtable = new_memtable;

        let _ = self
            .manifest
            .as_ref()
            .map(|manifest| {
                manifest.add_record(
                    state_lock_observer,
                    ManifestRecord::NewMemtable(new_memtable_id),
                )
            })
            .ok_or_else(|| {
                anyhow::anyhow!("Err when create new memtable and record in manifest")
            })?;
        self.sync_dir()?;
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let state_guard = self.state_lock.lock();

        // First try to flush the earliest-created memtable
        let flushed_memtable = {
            let snapshot = self.state.read();
            if snapshot.imm_memtables.is_empty() {
                return Ok(());
            }
            snapshot
                .imm_memtables
                .last()
                .expect("no imm memtable")
                .clone()
        };

        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        flushed_memtable.flush(&mut sst_builder)?;
        let sst_id = flushed_memtable.id();
        let sst = Arc::new(sst_builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.generate_filename(FileType::SST, sst_id),
        )?);

        // Need to keep the state guard lock while flushing
        // because we don't know whether the memtable is flushed successfully
        {
            // Modify the state, remove flushed memtable from immutable memtable
            // and insert the sst into l0_sstables
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            let removed_mem = snapshot
                .imm_memtables
                .pop()
                .expect("Pop immutable memtables error, should not happen");
            assert_eq!(removed_mem.id(), sst_id);
            if self.compaction_controller.flush_to_l0() {
                snapshot.l0_sstables.insert(0, sst_id);
            } else {
                // Tiered compaction
                snapshot.levels.insert(0, (sst_id, vec![sst_id]));
            }
            log::info!("Flushed sst {sst_id} with size {}", sst.table_size());
            snapshot.sstables.insert(sst_id, sst);
            *guard = Arc::new(snapshot);
        }

        // Remove old memtable WAL file if it exists
        if self.options.enable_wal {
            std::fs::remove_file(self.generate_filename(FileType::WAL, sst_id))?;
        }

        self.manifest
            .as_ref()
            .ok_or_else(|| anyhow!("manifest not found when trigger flush"))?
            .add_record(&state_guard, ManifestRecord::Flush(sst_id))?;
        self.sync_dir()?;

        Ok(())
    }

    pub fn new_txn(&self) -> Result<Arc<Transaction>> {
        self.mvcc
            .as_ref()
            .map(|mvcc| mvcc.new_txn(self.options.serializable))
            .ok_or(anyhow::anyhow!("MVCC is None, feature is not enabled"))
    }

    /// Create an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        if let Ok(txn) = self.new_txn() {
            txn.scan(lower, upper)
        } else {
            let lsm_iter = self.scan_with_ts(lower, upper, TS_DEFAULT)?;
            TxnIterator::create_from_lsm_iter(lsm_iter)
        }
    }

    pub fn scan_with_ts(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        timestamp: TimeStamp,
    ) -> Result<FusedIterator<LsmIterator>> {
        // Scan range: (user_key_begin, TS_RANGE_BEGIN) -> (user_key_end, TS_RANGE_END)
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
        let snapshot = {
            let guard = self.state.read();
            guard.clone()
        };

        let memtable_iters = std::iter::once(&snapshot.memtable)
            .chain(&snapshot.imm_memtables)
            .map(|memtable| Box::new(memtable.scan(lower, upper)))
            .collect::<Vec<_>>();
        let memtable_merge_iter = MergeIterator::create(memtable_iters);

        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for id in snapshot.l0_sstables.iter() {
            if let Some(sst) = snapshot.sstables.get(id) {
                if Self::is_range_overlap(lower, upper, (sst.first_key(), sst.last_key())) {
                    let iter = match lower {
                        // [key, upper bound)
                        Bound::Included(key) => {
                            Box::new(SsTableIterator::create_and_seek_to_key(sst.clone(), key)?)
                        }
                        // (key, upper bound)   upper bound could be included, excluded or unbounded
                        Bound::Excluded(key) => {
                            let mut iter = Box::new(SsTableIterator::create_and_seek_to_key(
                                sst.clone(),
                                key,
                            )?);
                            while iter.is_valid() && iter.key().key_ref() == key.key_ref() {
                                iter.next()?;
                            }
                            iter
                        }
                        // Lower doesn't have bound
                        Bound::Unbounded => {
                            Box::new(SsTableIterator::create_and_seek_to_first(sst.clone())?)
                        }
                    };
                    l0_iters.push(iter);
                }
            }
        }

        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, ids) in snapshot.levels.iter() {
            let tables = ids
                .iter()
                .filter_map(|id| snapshot.sstables.get(id))
                .filter(|sst| {
                    Self::is_range_overlap(lower, upper, (sst.first_key(), sst.last_key()))
                })
                .cloned()
                .collect::<Vec<Arc<SsTable>>>();

            if !tables.is_empty() {
                let level_iter = match lower {
                    Bound::Included(key) => {
                        Box::new(SstConcatIterator::create_and_seek_to_key(tables, key)?)
                    }
                    Bound::Excluded(key) => {
                        let mut iter =
                            Box::new(SstConcatIterator::create_and_seek_to_key(tables, key)?);
                        while iter.is_valid() && iter.key().key_ref() == key.key_ref() {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => {
                        Box::new(SstConcatIterator::create_and_seek_to_first(tables)?)
                    }
                };
                level_iters.push(level_iter);
            }
        }

        let level_merge_iter = MergeIterator::create(level_iters);
        let l0_merge_iter = MergeIterator::create(l0_iters);
        let lsm_inner = TwoMergeIterator::create(memtable_merge_iter, l0_merge_iter)?;
        let lsm_inner = TwoMergeIterator::create(lsm_inner, level_merge_iter)?;
        Ok(FusedIterator::new(LsmIterator::new(
            lsm_inner,
            map_bound(upper),
            timestamp,
        )?))
    }

    fn try_freeze(&self, estimated_size: usize) -> Result<()> {
        // If the estimated size of the memtable is larger than the target size, freeze the memtable
        if estimated_size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            let guard = self.state.read();
            // Avoid multi threads call freeze memtable multi times
            if guard.memtable.approximate_size() >= self.options.target_sst_size {
                drop(guard);
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    fn is_key_exist(search_key: &[u8], fence_pointer: (&KeyBytes, &KeyBytes)) -> bool {
        fence_pointer.0.key_ref() <= search_key && search_key <= fence_pointer.1.key_ref()
    }

    fn is_range_overlap(
        lower: Bound<KeySlice>,
        upper: Bound<KeySlice>,
        fence_pointer: (&KeyBytes, &KeyBytes),
    ) -> bool {
        match upper {
            // If upper bound smaller than the smallest key in the SSTable
            Bound::Included(key) if key.key_ref() < fence_pointer.0.key_ref() => {
                return false;
            }
            Bound::Excluded(key) if key.key_ref() <= fence_pointer.0.key_ref() => {
                return false;
            }
            _ => {}
        }

        match lower {
            // If lower bound larger than the largest key in the SSTable
            Bound::Included(key) if key.key_ref() > fence_pointer.1.key_ref() => {
                return false;
            }
            Bound::Excluded(key) if key.key_ref() >= fence_pointer.1.key_ref() => {
                return false;
            }
            _ => {}
        }

        true
    }

    fn try_freeze_memtable(&self, estimated_size: usize) -> Result<()> {
        if estimated_size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            let guard = self.state.read();
            // Recheck the estimated size of the memtable
            // Consider the case without recheck: many threads all find estimated size larger than the target sstable size
            // then all threads will call `force_freeze_memtable` at the same time. Recheck to avoid produce multiple empty memtables
            if guard.memtable.approximate_size() >= self.options.target_sst_size {
                drop(guard);
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }
}
