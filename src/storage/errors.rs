use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Invalid partition: {0}")]
    InvalidPartition(String),
    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(String),
    #[error("Batch concat error")]
    BatchConcatError(#[from] arrow::error::ArrowError),
    #[error("Memory limit exceeded: {0}")]
    MemoryLimit(String),
    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}
