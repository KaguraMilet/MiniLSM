mod leveled;
mod simple_leveled;
mod tiered;

use core::panic;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{ensure, Result};
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::db::filename::FileType;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{KeySlice, KeyVec};
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let iter = Self::create_full_compaction_iter(self, l0_sstables, l1_sstables)?;
                self.compact_generate_sst_from_iter(iter, task.compact_to_bottom_level())
            }
            CompactionTask::Simple(task) => self.simple_compact(task),
            CompactionTask::Tiered(TieredCompactionTask {
                tiers,
                bottom_tier_included: _,
            }) => {
                let snapshot = {
                    let state = self.state.read();
                    state.clone()
                };
                let mut iters = Vec::with_capacity(tiers.len());
                for (_, ssts) in tiers.iter() {
                    let tables = ssts
                        .iter()
                        .filter_map(|id| snapshot.sstables.get(id))
                        .cloned()
                        .collect::<Vec<Arc<SsTable>>>();
                    assert_eq!(tables.len(), ssts.len(), "SSTables not found");
                    iters.push(Box::new(SstConcatIterator::create_and_seek_to_first(
                        tables,
                    )?));
                }
                self.compact_generate_sst_from_iter(
                    MergeIterator::create(iters),
                    task.compact_to_bottom_level(),
                )
            }
            CompactionTask::Leveled(task) => self.leveled_compact(task),
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let l0_sstables;
        let l1_sstables;
        {
            let snapshot = self.state.read();
            l0_sstables = snapshot.l0_sstables.clone();
            l1_sstables = snapshot
                .levels
                .first()
                .map_or(Vec::new(), |level| level.1.clone());
        }

        let new_ssts = self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        })?;

        {
            let _state_guard = self.state_lock.lock();
            let mut state = self.state.read().as_ref().clone();
            for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
                let result = state.sstables.remove(sst);
                assert!(result.is_some());
            }
            let mut new_ids = Vec::with_capacity(new_ssts.len());
            for sst in new_ssts {
                new_ids.push(sst.sst_id());
                let result = state.sstables.insert(sst.sst_id(), sst);
                assert!(result.is_none());
            }
            assert_eq!(l1_sstables, state.levels[0].1);
            state.levels[0].1 = new_ids;
            let mut l0_sstables_set = l0_sstables.iter().copied().collect::<HashSet<_>>();
            state.l0_sstables = state
                .l0_sstables
                .iter()
                .filter(|id| !l0_sstables_set.remove(id))
                .copied()
                .collect();
            assert!(l0_sstables_set.is_empty());
            *self.state.write() = Arc::new(state);
        }

        for sst in l0_sstables.into_iter().chain(l1_sstables.into_iter()) {
            std::fs::remove_file(self.generate_filename(FileType::SST, sst))?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let task = {
            let snapshot = self.state.read();
            match self
                .compaction_controller
                .generate_compaction_task(&snapshot)
            {
                Some(task) => task,
                None => return Ok(()),
            }
        };
        log::info!("trigger compaction: {:?}", task);
        let new_sstables = self.compact(&task)?;
        let new_sstable_ids = new_sstables
            .iter()
            .map(|sst| sst.sst_id())
            .collect::<Vec<usize>>();
        let ssts_to_remove = {
            let state_lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();
            ensure!(
                new_sstables.iter().all(|sst| snapshot
                    .sstables
                    .insert(sst.sst_id(), sst.clone())
                    .is_none()),
                "new sstables already exist, new sstable ids: {:?}",
                new_sstable_ids
            );
            let (mut new_state, ssts_to_remove_ids) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &new_sstable_ids);
            let ssts_to_remove = ssts_to_remove_ids
                .iter()
                .filter_map(|id| new_state.sstables.remove(id))
                .collect::<Vec<Arc<SsTable>>>();
            assert_eq!(
                ssts_to_remove.len(),
                ssts_to_remove_ids.len(),
                "sstables to be removed mismatch"
            );
            {
                let mut state = self.state.write();
                *state = Arc::new(new_state);
            }
            self.sync_dir()?;
            self.manifest
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("manifest not found when trigger compaction"))?
                .add_record(
                    &state_lock,
                    ManifestRecord::Compaction(task, new_sstable_ids),
                )?;
            ssts_to_remove
        };
        log::info!(
            "compaction finished, {} sstables removed, {} sstables added",
            ssts_to_remove.len(),
            new_sstables.len()
        );
        for sst in ssts_to_remove {
            std::fs::remove_file(self.generate_filename(FileType::SST, sst.sst_id()))?;
        }
        self.sync_dir()?;
        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let should_flush = {
            let snapshot = self.state.read();
            snapshot.imm_memtables.len() >= self.options.num_memtable_limit
        };
        if should_flush {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }

    fn create_full_compaction_iter(
        &self,
        l0_ids: &[usize],
        l1_ids: &[usize],
    ) -> Result<TwoMergeIterator<MergeIterator<SsTableIterator>, SstConcatIterator>> {
        let snapshot = {
            let guard = self.state.read();
            guard.clone()
        };
        let mut l0_iters = Vec::with_capacity(l0_ids.len());
        for id in l0_ids {
            l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                snapshot
                    .sstables
                    .get(id)
                    .unwrap_or_else(|| panic!("l0 sstable {} not found", id))
                    .clone(),
            )?));
        }
        let mut l1_tables = Vec::with_capacity(l1_ids.len());
        for id in l1_ids {
            l1_tables.push(
                snapshot
                    .sstables
                    .get(id)
                    .unwrap_or_else(|| panic!("l1 sstable {} not found", id))
                    .clone(),
            );
        }
        TwoMergeIterator::create(
            MergeIterator::create(l0_iters),
            SstConcatIterator::create_and_seek_to_first(l1_tables)?,
        )
    }

    fn simple_compact(&self, task: &SimpleLeveledCompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let guard = self.state.read();
            guard.clone()
        };

        let mut lower_level_tables = Vec::with_capacity(task.lower_level_sst_ids.len());
        for id in &task.lower_level_sst_ids {
            lower_level_tables.push(
                snapshot
                    .sstables
                    .get(id)
                    .unwrap_or_else(|| panic!("sstable {} not found", id))
                    .clone(),
            );
        }

        if task.upper_level.is_some() {
            let mut upper_level_tables = Vec::with_capacity(task.upper_level_sst_ids.len());
            for id in &task.upper_level_sst_ids {
                upper_level_tables.push(
                    snapshot
                        .sstables
                        .get(id)
                        .unwrap_or_else(|| panic!("sstable {} not found", id))
                        .clone(),
                );
            }
            let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_level_tables)?;
            let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_level_tables)?;
            let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
            self.compact_generate_sst_from_iter(iter, task.is_lower_level_bottom_level)
        } else {
            let mut l0_iters = Vec::with_capacity(task.upper_level_sst_ids.len());
            for id in &task.upper_level_sst_ids {
                l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                    snapshot
                        .sstables
                        .get(id)
                        .unwrap_or_else(|| panic!("sstable {} not found", id))
                        .clone(),
                )?));
            }
            let upper_iter = MergeIterator::create(l0_iters);
            let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_level_tables)?;
            let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
            self.compact_generate_sst_from_iter(iter, task.is_lower_level_bottom_level)
        }
    }

    fn leveled_compact(&self, task: &LeveledCompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = { self.state.read().clone() };

        let lower_tables = task
            .lower_level_sst_ids
            .iter()
            .map(|id| {
                snapshot
                    .sstables
                    .get(id)
                    .ok_or_else(|| anyhow::anyhow!("sstable {} not found", id))
                    .unwrap()
            })
            .cloned()
            .collect::<Vec<_>>();
        if task.upper_level.is_some() {
            let upper_tables = task
                .upper_level_sst_ids
                .iter()
                .map(|id| {
                    snapshot
                        .sstables
                        .get(id)
                        .ok_or_else(|| anyhow::anyhow!("sstable {} not found", id))
                        .unwrap()
                })
                .cloned()
                .collect::<Vec<_>>();
            let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_tables)?;
            let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_tables)?;
            let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
            self.compact_generate_sst_from_iter(iter, task.is_lower_level_bottom_level)
        } else {
            let mut l0_iters = Vec::with_capacity(task.upper_level_sst_ids.len());
            for id in &task.upper_level_sst_ids {
                l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                    snapshot
                        .sstables
                        .get(id)
                        .ok_or_else(|| anyhow::anyhow!("sstable {} not found", id))
                        .unwrap()
                        .clone(),
                )?));
            }
            let upper_iter = MergeIterator::create(l0_iters);
            let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_tables)?;
            let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
            self.compact_generate_sst_from_iter(iter, task.is_lower_level_bottom_level)
        }
    }

    fn compact_generate_sst_from_iter<I>(
        &self,
        mut iter: I,
        is_compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>>
    where
        I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
    {
        let watermark = self.mvcc().watermark();
        let mut builder = SsTableBuilder::new(self.options.target_sst_size);
        let mut builders = Vec::new();
        let mut prev_key = KeyVec::new();

        while iter.is_valid() {
            let key = iter.key();
            let value = iter.value();

            let same_as_prev = key.key_ref() == prev_key.key_ref();
            let filter_matched = self
                .compaction_filters
                .lock()
                .iter()
                .any(|f| f.matches(key.key_ref()));
            let second_under_watermark =
                same_as_prev && key.ts() <= watermark && prev_key.ts() <= watermark;
            let first_remove = !second_under_watermark
                && key.ts() <= watermark
                && ((is_compact_to_bottom_level && value.is_empty()) || filter_matched);
            prev_key.set_from_slice(key);

            if first_remove || second_under_watermark {
                iter.next()?;
                continue;
            }

            builder.add(iter.key(), iter.value());
            if builder.estimated_size() >= self.options.target_sst_size && !same_as_prev {
                let builder = std::mem::replace(
                    &mut builder,
                    SsTableBuilder::new(self.options.target_sst_size),
                );
                builders.push(builder);
            }
            iter.next()?;
        }

        // Iterator already closed, we need to flush entries in current builder
        builders.push(builder);
        let mut new_sstables = Vec::with_capacity(builders.len());
        for builder in builders {
            if !builder.is_empty() {
                let sst_id = self.next_sst_id();
                let sst = Arc::new(builder.build(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.generate_filename(FileType::SST, sst_id),
                )?);
                new_sstables.push(sst);
            }
        }
        Ok(new_sstables)
    }
}
