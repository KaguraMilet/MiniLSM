use structured_logger::{json::new_writer, Builder};
use tempfile::tempdir;

use crate::{
    compact::{CompactionOptions, TieredCompactionOptions},
    lsm_storage::{LsmStorageOptions, MiniLsm},
};

use super::harness::{check_compaction_ratio, compaction_bench};

#[test]
fn test_integration() {
    let dir = tempdir().unwrap();
    let log_file = std::fs::File::create("mini-lsm-debug.log").unwrap();
    Builder::with_level("debug")
        .with_target_writer("*", new_writer(log_file))
        .init();

    let storage = MiniLsm::open(
        &dir,
        LsmStorageOptions::default_for_week2_test(CompactionOptions::Tiered(
            TieredCompactionOptions {
                num_tiers: 3,
                max_size_amplification_percent: 200,
                size_ratio: 1,
                min_merge_width: 2,
            },
        )),
    )
    .unwrap();

    compaction_bench(storage.clone());
    check_compaction_ratio(storage.clone());
}
