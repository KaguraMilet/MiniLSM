use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

const MB: usize = 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

struct LevelSizes {
    actual_sizes: Vec<usize>,
    target_sizes: Vec<usize>,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn compute_level_sizes(&self, snapshot: &LsmStorageState) -> LevelSizes {
        let levels_len = snapshot.levels.len();
        let level_actual_sizes = snapshot
            .levels
            .iter()
            .map(|(_, ids)| {
                ids.iter()
                    .map(|id| {
                        snapshot
                            .sstables
                            .get(id)
                            .ok_or_else(|| anyhow::anyhow!("sstable {id} not found, inconsistent between levels index and sstables map"))
                            .unwrap()
                            .table_size()
                    })
                    .sum::<u64>() as usize
            })
            .collect::<Vec<_>>();
        let base_level_size_bytes = self.options.base_level_size_mb * MB;
        let mut target_level_sizes: Vec<usize> = vec![0; levels_len];
        // Bottom level size should not exceed `base_level_size_mb``
        if let Some(last_level) = target_level_sizes.last_mut() {
            *last_level = level_actual_sizes[levels_len - 1].max(base_level_size_bytes);
        }
        // When the bottom level does not reach or exceed `base_level_size_mb`, the rest levels target size will be zero
        // and we can skip rest level target size computing
        if level_actual_sizes[levels_len - 1] < base_level_size_bytes {
            return LevelSizes {
                actual_sizes: level_actual_sizes,
                target_sizes: target_level_sizes,
            };
        }
        // The rest levels(except the bottom level) target sizes computing
        for idx in (1..levels_len).rev() {
            // At most one level can be smaller than `base_level_size_mb`
            if target_level_sizes[idx] < base_level_size_bytes {
                break;
            }
            // Compute from the last level to first level, the target level size of upper level computed by
            // current_level_size / self.options.level_size_multiplier
            target_level_sizes[idx - 1] =
                target_level_sizes[idx] / self.options.level_size_multiplier;
        }
        LevelSizes {
            actual_sizes: level_actual_sizes,
            target_sizes: target_level_sizes,
        }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        upper_sst_ids: &[usize],
        in_level_idx: usize,
    ) -> Vec<usize> {
        let min_key = upper_sst_ids
            .iter()
            .map(|id| {
                snapshot
                    .sstables
                    .get(id)
                    .ok_or_else(|| anyhow::anyhow!("sstable {id} not found, inconsistent between levels index and sstables map"))
                    .unwrap()
                    .first_key()
            })
            .min()
            .cloned()
            .unwrap();
        let max_key = upper_sst_ids
            .iter()
            .map(|id| {
                snapshot
                    .sstables
                    .get(id)
                    .ok_or_else(|| anyhow::anyhow!("sstable {id} not found, inconsistent between levels index and sstables map"))
                    .unwrap()
                    .last_key()
            })
            .max()
            .cloned()
            .unwrap();
        snapshot.levels[in_level_idx]
            .1
            .iter()
            .filter_map(|id| {
                let sst = snapshot
                    .sstables
                    .get(id)
                    .ok_or_else(|| anyhow::anyhow!("sstable {id} not found"))
                    .unwrap();
                // SSTable exist key overlaps with upper level sstables
                (!(sst.last_key() < &min_key || sst.first_key() > &max_key)).then_some(*id)
            })
            .collect::<Vec<usize>>()
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let level_sizes = self.compute_level_sizes(snapshot);
        log::info!(
            "Each level(except L0) actual size: {:?}, target size: {:?}",
            level_sizes
                .actual_sizes
                .iter()
                .map(|x| format!("{}MB", x / MB))
                .collect::<Vec<_>>(),
            level_sizes
                .target_sizes
                .iter()
                .map(|x| format!("{}MB", x / MB))
                .collect::<Vec<_>>()
        );
        // Base level will be the first level which target size larger than zero
        let base_level_idx = level_sizes.target_sizes.partition_point(|size| *size == 0);
        // L0 compaction always have highest priority
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            log::info!("Trigger compaction from L0 to L{}", base_level_idx + 1);
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: base_level_idx + 1,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &snapshot.l0_sstables,
                    base_level_idx,
                ),
                is_lower_level_bottom_level: base_level_idx + 1 == self.options.max_levels,
            });
        }

        // Try to search rest level to generate compaction task
        level_sizes
            .actual_sizes
            .iter()
            .zip(level_sizes.target_sizes)
            .enumerate()
            .filter(|(_, (_, target_size))| *target_size > 0)
            .filter_map(|(idx, (actual_size, target_size))| {
                let ratio = *actual_size as f64 / target_size as f64;
                (ratio > 1.0).then_some((idx, ratio))
            })
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
            .map(|(upper_idx, _)| {
                let upper_level_sst_id = snapshot.levels[upper_idx]
                    .1
                    .iter()
                    .min()
                    .copied()
                    .ok_or_else(|| anyhow::anyhow!("no sstables in upper level"))
                    .unwrap();
                log::info!(
                "Trigger compaction from L{} to L{}, select {upper_level_sst_id} for compaction",
                upper_idx + 1,
                upper_idx + 1 + 1
            );
                LeveledCompactionTask {
                    upper_level: Some(upper_idx + 1),
                    upper_level_sst_ids: vec![upper_level_sst_id],
                    lower_level: upper_idx + 1 + 1,
                    lower_level_sst_ids: self.find_overlapping_ssts(
                        snapshot,
                        &[upper_level_sst_id],
                        upper_idx + 1,
                    ),
                    is_lower_level_bottom_level: upper_idx + 1 + 1 == self.options.max_levels,
                }
            })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_snapshot = snapshot.clone();
        if let Some(upper_level) = task.upper_level {
            new_snapshot
                .levels
                .get_mut(upper_level - 1)
                .ok_or_else(|| anyhow::anyhow!("upper level {upper_level} not found"))
                .unwrap()
                .1
                .retain(|id| !task.upper_level_sst_ids.contains(id));
        } else {
            new_snapshot
                .l0_sstables
                .retain(|id| !task.upper_level_sst_ids.contains(id));
        }

        let lower_level = new_snapshot
            .levels
            .get_mut(task.lower_level - 1)
            .ok_or_else(|| anyhow::anyhow!("lower level {} not found", task.lower_level))
            .unwrap();

        lower_level
            .1
            .retain(|id| !task.lower_level_sst_ids.contains(id));
        lower_level.1.extend_from_slice(output);
        // Keep SST ids ordered by first keys in all levels except L0
        lower_level.1.sort_by(|a, b| {
            new_snapshot
                .sstables
                .get(a)
                .ok_or_else(|| anyhow::anyhow!("sstable {a} not found"))
                .unwrap()
                .first_key()
                .cmp(
                    new_snapshot
                        .sstables
                        .get(b)
                        .ok_or_else(|| anyhow::anyhow!("sstable {b} not found"))
                        .unwrap()
                        .first_key(),
                )
        });
        log::debug!(
            "New lower level: {} sstables: {:?}",
            lower_level.0,
            lower_level.1
        );
        (
            new_snapshot,
            task.upper_level_sst_ids
                .iter()
                .chain(task.lower_level_sst_ids.iter())
                .copied()
                .collect::<Vec<_>>(),
        )
    }
}
