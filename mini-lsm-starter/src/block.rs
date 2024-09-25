mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        // In block builder, data and offsets are separate, now need to merge them as a whole object
        let mut buffer = BytesMut::with_capacity(self.data.len() + self.offsets.len() * 2 + 2);
        buffer.put_slice(&self.data);
        // Append offsets after data
        self.offsets
            .iter()
            .for_each(|offset| buffer.put_u16(*offset));
        // Append the number of entries
        buffer.put_u16(self.offsets.len() as u16);
        buffer.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        const SIZE_U16: usize = std::mem::size_of::<u16>();
        // Get the number of entries in the block
        let entries_num = (&data[data.len() - SIZE_U16..]).get_u16() as usize;
        let offsets_start_pos = data.len() - SIZE_U16 * (entries_num + 1);
        let offsets = data[offsets_start_pos..(data.len() - SIZE_U16)]
            .chunks(SIZE_U16)
            .map(|mut offset| offset.get_u16())
            .collect::<Vec<u16>>();
        Self {
            data: data[..offsets_start_pos].to_vec(),
            offsets,
        }
    }
}
