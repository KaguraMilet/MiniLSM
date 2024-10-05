use criterion::{criterion_group, criterion_main, Criterion};
use mini_lsm_starter::{
    compact::{CompactionOptions, LeveledCompactionOptions},
    lsm_storage::{LsmStorageOptions, MiniLsm},
};
use rand::{rngs::SmallRng, Rng, SeedableRng};

#[inline(always)]
fn generate_key(size: usize, i: usize) -> Vec<u8> {
    let prefix = format!("key{}", i);
    let mut key = Vec::with_capacity(size);
    key.extend_from_slice(prefix.as_bytes());
    while key.len() < size {
        key.push(b'~');
    }
    key.truncate(size);
    key
}

fn put_bench_internal(key_size: usize) {
    let dir = format!("Bytes{key_size}");
    let storage = MiniLsm::open(&dir, LsmStorageOptions::default_for_week1_day6_test()).unwrap();

    const OPS: usize = 500000;
    let mut rng = rand::thread_rng();
    let mut last_op = 0;

    for i in 0..OPS {
        let random_number = rng.gen_range(0..4);
        match random_number {
            0 => storage
                .put(
                    generate_key(key_size, i).as_slice(),
                    format!("value{}", i).as_bytes(),
                )
                .unwrap(),
            1 => storage
                .delete(generate_key(key_size, i).as_slice())
                .unwrap(),
            2 => storage
                .put(
                    generate_key(key_size, last_op).as_slice(),
                    format!("value{}v2", last_op).as_bytes(),
                )
                .unwrap(),
            3 => {
                storage
                    .put(
                        generate_key(key_size, last_op).as_slice(),
                        format!("value{}v3", last_op).as_bytes(),
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        last_op = i;
    }
}

fn put_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("Hash sharing put bench");
    group.bench_function("Put 10 bytes", |b| b.iter(|| put_bench_internal(10)));
    group.bench_function("Put 128 bytes", |b| b.iter(|| put_bench_internal(128)));
    group.bench_function("Put 512 bytes", |b| b.iter(|| put_bench_internal(512)));
}

fn get_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("Get bench");
    for i in &[8, 12, 16, 20] {
        group.bench_with_input(format!("Get {}", i), i, |b, i| {
            let dir = tempfile::tempdir().unwrap();
            let storage = MiniLsm::open(&dir, LsmStorageOptions::default_for_week1_test()).unwrap();
            for key_i in 1..(1 << *i) {
                storage
                    .put(
                        format!("key{}", key_i).as_bytes(),
                        format!("value{}", key_i).as_bytes(),
                    )
                    .unwrap();
            }
            let mut rng = SmallRng::from_seed([0; 32]);
            b.iter(|| {
                storage
                    .get(format!("key{}", rng.gen_range(1..(1 << *i))).as_bytes())
                    .unwrap();
            })
        });
    }
}

criterion_group!(benches, put_bench);
criterion_main!(benches);
