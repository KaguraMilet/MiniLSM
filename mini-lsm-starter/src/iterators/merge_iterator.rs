use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

// A wrapper struct that stores the index of the iterator and the iterator itself.
struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
/// Generic instead of virtual table
/// Merge iterator manage multi iterators(i.e MemTable iterator, SST iterator, blablabla)
/// e.g Memtable (1,1), (2,2), (3,10), (10,1), (100,1)
/// Level0-1 (3,9), (3,8), (3,7), (11,1)
/// Level0-2 (2,1), (3,6), (3,5), (123,1)
/// Level1 (3,4), (3,3), (3,2), (3,1)
/// current = (1,1)
/// Then we call next()
/// Memtable (1,1), (2,2), (3,10), (10,1), (100,1)
/// Level0-1 (3,9), (3,8), (3,7), (11,1)
/// Level0-2 (2,1), (3,6), (3,5), (123,1)
/// Level1 (3,4), (3,3), (3,2), (3,1)
/// current = (2,2) from memtable
/// Note that Level0-1 point to (3, 9), Level0-2 point to (2, 1), Level1 point to (3, 4)
/// only Memtable point to (2, 2)
pub struct MergeIterator<I: StorageIterator> {
    // For each element in iters, it stores the index of the iterator
    // and the iterator which satisfies theStorageIterator trait
    iters: BinaryHeap<HeapWrapper<I>>,
    // Current point to the iterator which has the smallest key
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap = BinaryHeap::new();
        // If iterators is empty, heap will be empty
        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter));
            }
        }

        // Pop to avoid self reference
        let curr = heap.pop();
        Self {
            iters: heap,
            current: curr,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        // .1 to get the iterator pointer
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.as_ref().is_some_and(|iter| iter.1.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        let curr_iter = self.current.as_mut().unwrap();

        // Try to get the smallest entry from the heap
        while let Some(mut heap_top) = self.iters.peek_mut() {
            // Skip the same key(They represent the same key with old version)
            // Consider the case:
            // Memtable (1,1), (2,2), (3,10), (10,1), (100,1)
            // Level0-1 (1,2), (3,8), (3,7), (11,1)
            // So we should make Level0-1 point to (3, 8) instead of (1, 2)
            if curr_iter.1.key() == heap_top.1.key() {
                if let Err(e) = heap_top.1.next() {
                    // An error occurs, remove the iterator from heap
                    PeekMut::pop(heap_top);
                    return Err(e);
                }
                // If the iterator already reach the end, remove it from heap
                if !heap_top.1.is_valid() {
                    PeekMut::pop(heap_top);
                }
            } else {
                break;
            }
        }

        curr_iter.1.next()?;

        // If current iterator already reach the end, remove it and try to get the minimum one
        if !curr_iter.1.is_valid() {
            if let Some(iter) = self.iters.pop() {
                // curr_iter is the mutable reference of self.current
                *curr_iter = iter;
            }
            return Ok(());
        }

        // Compare the current entry with the top of the heap, make the smallest one to the top of the heap
        if let Some(mut heap_top) = self.iters.peek_mut() {
            // Peek_mut return the reference of the top of the heap
            // Heap wrapper already implement compare operator
            if *curr_iter < *heap_top {
                std::mem::swap(curr_iter, &mut *heap_top);
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        // Recursively calculate the number of active iterators for all sub iterators
        self.iters
            .iter()
            .map(|iter| iter.1.num_active_iterators())
            .sum::<usize>()
            + self
                .current
                .as_ref()
                .map_or(0, |iter| iter.1.num_active_iterators())
    }
}
