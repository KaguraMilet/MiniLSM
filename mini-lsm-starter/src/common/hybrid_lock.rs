use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use parking_lot_core::SpinWait;
use thiserror::Error;

const LATCH_EXCLUSIVE_BIT: u64 = 1;

#[derive(Debug, Error)]
pub enum HybridLatchError {
    /// Optimistic validation failed, stack must unwind to a safe state.
    #[error("optimistic validation failed")]
    VersionOutdated,
}

pub type Result<T> = std::result::Result<T, HybridLatchError>;

#[repr(align(64))]
pub struct HybridLatch<T: ?Sized> {
    version: AtomicU64,
    lock: RwLock<()>,
    data: UnsafeCell<T>,
}

unsafe impl<T: ?Sized + Send> Send for HybridLatch<T> {}
unsafe impl<T: ?Sized + Send + Sync> Sync for HybridLatch<T> {}

#[inline]
fn has_exclusive_mark(version: u64) -> bool {
    (version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT
}

impl<T> HybridLatch<T> {
    /// Creates a new instance of a `HybridLatch<T>` which is unlocked.
    #[inline(always)]
    pub fn new(data: T) -> HybridLatch<T> {
        HybridLatch {
            version: AtomicU64::new(0),
            lock: RwLock::new(()),
            data: UnsafeCell::new(data),
        }
    }

    /// Locks the `HybridLatch<T>` exclusively, blocking the current thread until it can be acquired
    /// Return the exclusive guard and release the exclusive access permission when it is dropped
    #[inline]
    pub fn exclusive(&self) -> ExclusiveGuard<'_, T> {
        // Check https://github.com/leanstore/leanstore/blob/f9e8dae1c85daf98467f93b521b95d8cf3a1dbeb/backend/leanstore/sync-primitives/Latch.hpp#L170
        // for more details
        let guard = self.lock.write();
        let version = self.version.load(Ordering::Relaxed) + 1;
        self.version.store(version, Ordering::Release);
        ExclusiveGuard {
            latch: self,
            version,
            guard,
        }
    }

    /// Locks the `HybridLatch<T>` shared, blocking the current thread until it can be acquired
    /// Return the shared guard and release the shared access permission when it is dropped
    #[inline]
    pub fn shared(&self) -> ShardGuard<'_, T> {
        let guard = self.lock.read();
        let version = self.version.load(Ordering::Relaxed);
        ShardGuard {
            latch: self,
            version,
            guard,
        }
    }

    /// Acquire optimistic read access from `HybridLatch<T>`, spinning until it can be acquired
    /// Optimistic access must be validated after the read of underlying data and execute any action based on it
    pub fn optimistic_spin(&self) -> OptimisticGuard<'_, T> {
        let mut version = self.version.load(Ordering::Acquire);
        if has_exclusive_mark(version) {
            let mut spinwait = SpinWait::new();
            loop {
                version = self.version.load(Ordering::Acquire);
                if !has_exclusive_mark(version) {
                    break;
                } else {
                    let result = spinwait.spin();
                    if !result {
                        spinwait.reset();
                    }
                    continue;
                }
            }
        }

        OptimisticGuard {
            latch: self,
            version,
        }
    }

    /// Tries to acquire optimistic read access from `HybridLatch, unwinding on contention
    /// Optimistic access must be validated before performing any action based on a read of the
    /// underlying data.
    #[inline]
    pub fn optimistic_or_unwind(&self) -> Result<OptimisticGuard<'_, T>> {
        let version = self.version.load(Ordering::Acquire);
        if has_exclusive_mark(version) {
            Err(HybridLatchError::VersionOutdated)
        } else {
            Ok(OptimisticGuard {
                latch: self,
                version,
            })
        }
    }

    #[inline]
    pub fn optimistic_or_shared(&self) -> GuardState<'_, T> {
        let version = self.version.load(Ordering::Acquire);
        if has_exclusive_mark(version) {
            let guard = self.lock.read();
            GuardState::Shared(ShardGuard {
                latch: self,
                version: self.version.load(Ordering::Relaxed),
                guard,
            })
        } else {
            GuardState::Optimistic(OptimisticGuard {
                latch: self,
                version,
            })
        }
    }

    #[inline]
    pub fn optimistic_or_exclusive(&self) -> GuardState<'_, T> {
        let version = self.version.load(Ordering::Acquire);
        if has_exclusive_mark(version) {
            let guard = self.lock.write();
            GuardState::Exclusive(ExclusiveGuard {
                latch: self,
                version: self.version.load(Ordering::Relaxed),
                guard,
            })
        } else {
            GuardState::Optimistic(OptimisticGuard {
                latch: self,
                version,
            })
        }
    }
}

pub struct ExclusiveGuard<'a, T: ?Sized> {
    latch: &'a HybridLatch<T>,
    version: u64,
    #[allow(dead_code)]
    guard: RwLockWriteGuard<'a, ()>,
}

unsafe impl<'a, T: ?Sized + Sync> Sync for ExclusiveGuard<'a, T> {}

impl<T: ?Sized> Deref for ExclusiveGuard<'_, T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.latch.data.get() }
    }
}

impl<T: ?Sized> DerefMut for ExclusiveGuard<'_, T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.latch.data.get() }
    }
}

impl<T: ?Sized> Drop for ExclusiveGuard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        let new_version = self.version + LATCH_EXCLUSIVE_BIT;
        self.latch.version.store(new_version, Ordering::Release);
    }
}

pub struct ShardGuard<'a, T: ?Sized> {
    latch: &'a HybridLatch<T>,
    version: u64,
    #[allow(dead_code)]
    guard: RwLockReadGuard<'a, ()>,
}

unsafe impl<'a, T: ?Sized + Sync> Sync for ShardGuard<'a, T> {}

impl<T: ?Sized> Deref for ShardGuard<'_, T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.latch.data.get() }
    }
}

pub struct OptimisticGuard<'a, T: ?Sized> {
    latch: &'a HybridLatch<T>,
    version: u64,
}

unsafe impl<'a, T: ?Sized + Sync> Sync for OptimisticGuard<'a, T> {}

impl<'a, T> OptimisticGuard<'a, T> {
    #[inline]
    pub fn recheck(&self) -> Result<()> {
        if self.version != self.latch.version.load(Ordering::Acquire) {
            return Err(HybridLatchError::VersionOutdated);
        }
        Ok(())
    }
}

impl<'a, T> Deref for OptimisticGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.latch.data.get() }
    }
}

pub enum GuardState<'a, T> {
    Optimistic(OptimisticGuard<'a, T>),
    Shared(ShardGuard<'a, T>),
    Exclusive(ExclusiveGuard<'a, T>),
}

#[cfg(test)]
mod tests {
    use super::{HybridLatch, HybridLatchError, Result};
    use serial_test::serial;
    use std::sync::Arc;
    use std::thread;

    #[test]
    #[serial]
    fn single_thread_read_baseline() {
        let data = [1usize; 1000];
        let mut result = 1usize;
        let start = std::time::Instant::now();

        for _ in 0..4000000 {
            for j in 0..1000 {
                result = result.saturating_mul(data[j]);
            }
        }
        println!(
            "Single thread read baseline: {}ms",
            start.elapsed().as_millis()
        );
        assert!(result == 1);
    }

    #[test]
    #[serial]
    fn concurrent_read_write() {
        let latch = Arc::new(HybridLatch::new([1usize; 1000]));

        let readers = 3;
        let writers = 1;

        let mut reader_handles = Vec::new();
        let barrier = Arc::new(std::sync::Barrier::new(readers + writers + 1));

        for _ in 0..readers {
            let latch = latch.clone();
            let barrier = barrier.clone();

            let handle = thread::spawn(move || {
                barrier.wait();
                let mut result = 1usize;

                for _ in 0..4000000 {
                    loop {
                        let res = {
                            let attempt = || {
                                let guard = latch.optimistic_spin();
                                let mut result = 1usize;
                                for i in 0..1000 {
                                    result = result.saturating_mul(guard[i]);
                                }
                                guard.recheck()?;
                                Result::Ok(result)
                            };
                            attempt()
                        };
                        match res {
                            Ok(v) => {
                                result = result * v;
                                break;
                            }
                            Err(_) => {
                                // Retry
                                continue;
                            }
                        }
                    }
                    assert!(result == 1);
                }
                assert!(result == 1);
            });
            reader_handles.push(handle);
        }

        let mut writer_handles = Vec::new();
        for _ in 0..writers {
            let latch = latch.clone();
            let barrier = barrier.clone();

            let handle = thread::spawn(move || {
                barrier.wait();
                let seconds = 10f64;
                let micro_per_sec = 1000000;
                let freq = 100;
                let critical = 1000;
                for _ in 0..(seconds * freq as f64) as usize {
                    thread::sleep(std::time::Duration::from_micros(
                        (micro_per_sec / freq) - critical,
                    ));
                    {
                        let mut guard = latch.exclusive();
                        guard[3] = 2;
                        thread::sleep(std::time::Duration::from_micros(critical));
                        guard[3] = 1;
                    }
                }
            });
            writer_handles.push(handle);
        }
        barrier.wait();
        let start = std::time::Instant::now();

        for handle in reader_handles {
            handle.join().unwrap();
        }
        println!("Reader: {}ms", start.elapsed().as_millis());

        for handle in writer_handles {
            handle.join().unwrap();
        }
        println!("Writer: {}ms", start.elapsed().as_millis());
    }
}
