use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    // Equal to `options.level0_file_num_compaction_trigger` in RocksDB
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "L0 sstables should always be empty in tiered compaction."
        );

        // Check precondition is satisfied
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        // Calculate all levels except the last level size
        let size = snapshot
            .levels
            .iter()
            .take(snapshot.levels.len() - 1)
            .fold(0, |acc, (_, ssts)| acc + ssts.len());

        let space_amp_ratio = size as f64
            / snapshot
                .levels
                .last()
                .unwrap_or_else(|| panic!("should not be empty levels"))
                .1
                .len() as f64;
        // Space amplification constraint has been violated, try to schedule a major compaction
        // a major compaction reads all sorted runs as input. If the whole LSM tree estimated
        // size amplification is larger than max_size_amplification_percent / 100, trigger a major compaction
        // and all files will be compacted to one sorted run
        if space_amp_ratio * 100.0 >= self.options.max_size_amplification_percent as f64 {
            log::info!(
                "compaction triggered by space amplification ratio: {:.3}",
                space_amp_ratio
            );
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        // Compaction Triggered by number of sorted runs while respecting size_ratio
        // The compaction picker considers the sorted runs in age order (R1 is the first candidate) to determine
        // whether an adjacent sequence of sorted runs exists that satisfies the size ratio trigger
        // and it indicates that first n sorted runs are too large
        // For more details, please refer to https://github.com/facebook/rocksdb/wiki/Universal-Compaction#:~:text=We%20start%20from,hold%20any%20more.
        if snapshot.levels.len() >= self.options.min_merge_width {
            let size_ratio_trigger = (100.0 + self.options.size_ratio as f64) / 100.0;
            let mut previous_tiers_size = 0;
            for (idx, (_, ssts)) in snapshot.levels.iter().enumerate() {
                let size_ratio = previous_tiers_size as f64 / ssts.len() as f64;
                if size_ratio >= size_ratio_trigger {
                    log::info!("compaction triggered by number of sorted runs while respecting size ratio {:.3}", size_ratio);
                    return Some(TieredCompactionTask {
                        // Consider the simple case: R1 -> R2
                        tiers: snapshot.levels[..idx + 1].to_vec(),
                        bottom_tier_included: idx + 1 == snapshot.levels.len(),
                    });
                } else {
                    previous_tiers_size += ssts.len();
                }
            }
        }
        // Compaction triggered by number of sorted runs without respecting size ratio and the goal is to keep
        // the number of sorted runs try not to exceed options.num_tiers if we don't have memtable flush during
        // compaction
        // Why num_tiers_to_take = snapshot.levels.len() - self.options.num_tiers + 2? Consider a simple case:
        // We have 3 sorted runs, R1, R2 and R3. We reach the limit of num_tiers(3) and we want to keep num of sorted runs
        // under the limit. So snapshot.levels.len() - self.options.num_tiers and the result is 0.
        let num_tiers_to_take = snapshot.levels.len() - self.options.num_tiers + 2;
        log::info!("compaction triggered by number of sorted runs without respecting size ratio");
        Some(TieredCompactionTask {
            tiers: snapshot
                .levels
                .iter()
                .take(num_tiers_to_take)
                .cloned()
                .collect::<Vec<_>>(),
            bottom_tier_included: snapshot.levels.len() >= num_tiers_to_take,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "L0 sstables should always be empty in tiered compaction."
        );
        let mut new_state = snapshot.clone();
        let mut old_sstables = Vec::new();
        let mut old_tiers = HashSet::new();

        if let Some(idx) = snapshot
            .levels
            .iter()
            .enumerate()
            .find(|(_, (id, _))| id == &task.tiers[0].0)
            .map(|(idx, _)| idx)
        {
            new_state.levels.insert(idx, (output[0], output.to_vec()));
        } else {
            new_state.levels.insert(0, (output[0], output.to_vec()));
        }

        for (tier_id, files) in task.tiers.iter() {
            old_tiers.insert(*tier_id);
            old_sstables.extend_from_slice(files);
        }

        new_state.levels.retain(|(id, _)| !old_tiers.contains(id));
        (new_state, old_sstables)
    }
}
