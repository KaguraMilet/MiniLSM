use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            log::info!("compaction triggered at level 0 and level 1, number of SSTs reaches the limit of level 0 compaction trigger");
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: self.options.max_levels == 1,
            });
        }

        let level_lens = snapshot
            .levels
            .iter()
            .map(|level| level.1.len())
            .collect::<Vec<_>>();

        for level in 1..self.options.max_levels {
            let lower_level = level + 1;
            let size_ratio = level_lens[lower_level - 1] as f64 / level_lens[level - 1] as f64;
            if size_ratio.is_finite() && size_ratio < self.options.size_ratio_percent as f64 / 100.0
            {
                log::info!(
                    "compaction triggered at level {} and {}, size_ratio = {}",
                    level,
                    lower_level,
                    size_ratio
                );
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(level),
                    upper_level_sst_ids: snapshot.levels[level - 1].1.clone(),
                    lower_level,
                    lower_level_sst_ids: snapshot.levels[lower_level - 1].1.clone(),
                    is_lower_level_bottom_level: lower_level == self.options.max_levels,
                });
            }
        }
        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_snapshot = snapshot.clone();
        if let Some(upper_level) = task.upper_level {
            new_snapshot.levels[upper_level - 1]
                .1
                .retain(|id| !task.upper_level_sst_ids.contains(id));
        } else {
            new_snapshot
                .l0_sstables
                .retain(|id| !task.upper_level_sst_ids.contains(id));
        }
        new_snapshot.levels[task.lower_level - 1].1 = output.to_vec();

        let ssts_to_remove = task
            .upper_level_sst_ids
            .iter()
            .chain(task.lower_level_sst_ids.iter())
            .copied()
            .collect::<Vec<_>>();

        (new_snapshot, ssts_to_remove)
    }
}
