use std::collections::BTreeMap;

pub struct Watermark {
    // <timestamp, num_readers>
    readers: BTreeMap<u64, usize>,
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        self.readers.entry(ts).and_modify(|v| *v += 1).or_insert(1);
    }

    pub fn remove_reader(&mut self, ts: u64) {
        self.readers.entry(ts).and_modify(|v| *v -= 1);
        if self.readers.get(&ts) == Some(&0) {
            self.readers.remove(&ts);
        }
    }

    pub fn watermark(&self) -> Option<u64> {
        self.readers.keys().min().copied()
    }

    #[inline]
    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.len()
    }
}

impl Default for Watermark {
    fn default() -> Self {
        Self::new()
    }
}
