use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
/// Both A and B implement StorageIterator trait
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    is_first: bool, // Is it point to the first iterator
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self {
            a,
            b,
            is_first: false,
        };

        iter.skip_b_equal()?;
        iter.is_first = Self::is_first(&iter);
        Ok(iter)
    }

    fn skip_b_equal(&mut self) -> Result<()> {
        while self.a.is_valid() && self.b.is_valid() && self.a.key() == self.b.key() {
            self.b.next()?;
        }
        Ok(())
    }

    fn is_first(&self) -> bool {
        // If the first iterator is invalid, return false
        if !self.a.is_valid() {
            return false;
        }
        if !self.b.is_valid() {
            return true;
        }
        self.a.key() < self.b.key()
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.is_first {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.is_first {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        if self.is_first {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        if self.is_first {
            self.a.next()?;
        } else {
            self.b.next()?;
        }
        self.skip_b_equal()?;
        self.is_first = Self::is_first(self);
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
