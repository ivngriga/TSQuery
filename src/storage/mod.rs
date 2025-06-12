//! Time-series storage implementation

use std::sync::Arc;
use std::io;

mod scan_partitions;
mod table;
mod version;

#[cfg(test)]
mod tests;

pub use table::Table;
// Version and VersionRef are used internally but not needed publicly

/// Common error type for storage operations
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),
    #[error("Invalid Partition: {0}")]
    InvalidPartition(String),
    #[error("Batch concat error: {0}")]
    BatchConcatError(String),
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(String),
    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),
    #[error("Memory limit exceeded: {0}")]
    MemoryLimit(String),
    #[error("Version conflict: {0}")]
    VersionConflict(String),
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),
}

// Remove conflicting From implementation since thiserror handles it

/// Configuration for storage engine
#[derive(Clone, Debug)]
pub enum PartitionGranularity {
    Minute,
    Hour,
    Day,
    Month,
    Year,
}

#[derive(Clone, Debug)]
pub struct StorageConfig {
    pub memory_limit: Option<usize>,
    pub spill_path: Option<std::path::PathBuf>,
    pub spill_compression: Compression,
    pub partition_granularity: PartitionGranularity,
}

#[derive(Clone, Debug)]
pub enum Compression {
    None,
    Lz4,
    Zstd,
}
