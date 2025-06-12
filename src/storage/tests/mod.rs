pub mod basic;
pub mod time_queries;
pub mod spilling;
pub mod errors;

use super::*;
use arrow::{
    array::{Float64Array, TimestampNanosecondArray},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use tempfile::tempdir;

pub fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("time", DataType::Timestamp(TimeUnit::Nanosecond, None), false), 
        Field::new("value", DataType::Float64, false),
    ]))
}

pub fn create_test_batch(
    timestamps: Vec<i64>,
    values: Vec<f64>,
    schema: &Schema
) -> RecordBatch {
    let time_array = TimestampNanosecondArray::from(timestamps);
    let value_array = Float64Array::from(values);
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(time_array), Arc::new(value_array)]
    ).unwrap()
}

pub fn test_config() -> StorageConfig {
    StorageConfig {
        memory_limit: Some(10_000), // More realistic test limit
        spill_path: Some(tempdir().unwrap().keep()),
        spill_compression: Compression::None,
        partition_granularity: PartitionGranularity::Day,
    }
}
