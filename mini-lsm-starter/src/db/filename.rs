use std::path::{Path, PathBuf};

#[derive(Debug, PartialEq, Eq)]
pub enum FileType {
    /// '*.wal' files guarantee crash consistency of MemTable for DB
    WAL,
    /// '*.sst' file
    SST,
    /// 'MANIFEST-*' file
    MANIFEST,
}

#[inline]
pub(crate) fn generate_filename_static(
    path: impl AsRef<Path>,
    filetype: FileType,
    seqnum: usize,
) -> PathBuf {
    match filetype {
        FileType::WAL => path.as_ref().join(format!("{:05}.wal", seqnum)),
        FileType::SST => path.as_ref().join(format!("{:05}.sst", seqnum)),
        FileType::MANIFEST => path.as_ref().join(format!("MANIFEST-{:05}", seqnum)),
    }
}
