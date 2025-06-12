use std::sync::Arc;

use arrow::array::{Float64Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_array::StringArray;
use chrono::{DateTime, Utc};
use sqlparser::ast::BinaryOperator;

pub mod group_by;

use crate::storage::{Compression, PartitionGranularity, StorageConfig, Table};
use crate::query::QueryExecutor;

fn create_test_table() -> Table {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("value", DataType::Float64, false),
        Field::new("tag", DataType::Utf8, false),
    ]));

    let config = StorageConfig {
        memory_limit: None,
        spill_path: None,
        spill_compression: Compression::None,
        partition_granularity: PartitionGranularity::Day,
    };

    Table::new(schema, config)
}

fn create_test_batch(
    timestamps: Vec<i64>,
    values: Vec<f64>,
    tags: Vec<&str>,
    schema: &Schema
) -> RecordBatch {
    let time_array = TimestampNanosecondArray::from(timestamps);
    let value_array = Float64Array::from(values);
    let tag_array = StringArray::from(tags);

    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(time_array), Arc::new(value_array), Arc::new(tag_array)],
    ).unwrap()
}

#[test]
fn test_select_all_columns() {
    let table = create_test_table();
    let batch = create_test_batch(
        vec![1_000_000_000, 2_000_000_000, 3_000_000_000],
        vec![1.0, 2.0, 3.0],
        vec!["a", "b", "c"],
        table.schema()
    );
    table.ingest(batch).unwrap();

    let executor = QueryExecutor;
    let result = executor.execute_query("SELECT * FROM `table`", &table).unwrap();

    
    let batch = result;
    assert_eq!(batch.num_columns(), 3);
    assert_eq!(batch.num_rows(), 3);
}

#[test]
fn test_select_specific_columns() {
    let table = create_test_table();
    let batch = create_test_batch(
        vec![1_000_000_000, 2_000_000_000],
        vec![1.0, 2.0],
        vec!["x", "y"],
        table.schema()
    );
    table.ingest(batch).unwrap();

    let executor = QueryExecutor;
    let result = executor.execute_query("SELECT time, tag FROM `table`", &table).unwrap();
    
    assert_eq!(result.num_columns(), 2);
    assert_eq!(result.schema().field(0).name(), "time");
    assert_eq!(result.schema().field(1).name(), "tag");
}

#[test]
fn test_time_filter_gt() {
    let table = create_test_table();
    let batch = create_test_batch(
        vec![1_000_000_000, 2_000_000_000, 3_000_000_000],
        vec![1.0, 2.0, 3.0],
        vec!["a", "b", "c"],
        table.schema()
    );
    table.ingest(batch).unwrap();

    let executor = QueryExecutor;
    let result = executor.execute_query(
        "SELECT * FROM `table` WHERE time > 1500000000", 
        &table
    ).unwrap();
    
    assert_eq!(result.num_rows(), 2); // Should only include timestamps > 1.5s
}

#[test]
fn test_time_filter_range() {
    let table = create_test_table();
    let batch = create_test_batch(
        vec![1_000_000_000, 2_000_000_000, 3_000_000_000, 4_000_000_000],
        vec![1.0, 2.0, 3.0, 4.0],
        vec!["a", "b", "c", "d"],
        table.schema()
    );
    table.ingest(batch).unwrap();

    let executor = QueryExecutor;
    let result = executor.execute_query(
        "SELECT * FROM `table` WHERE time >= 2000000000 AND time < 4000000000", 
        &table
    ).unwrap();
    
    assert_eq!(result.num_rows(), 2); // Should include timestamps 2s and 3s
}

#[test]
fn test_value_filter() {
    let table = create_test_table();
    let batch = create_test_batch(
        vec![1_000_000_000, 2_000_000_000, 3_000_000_000],
        vec![1.0, 2.0, 3.0],
        vec!["a", "b", "c"],
        table.schema()
    );
    table.ingest(batch).unwrap();

    let executor = QueryExecutor;
    let result = executor.execute_query(
        "SELECT * FROM `table` WHERE value < 2.5", 
        &table
    ).unwrap();
    
    assert_eq!(result.num_rows(), 2); // Should include values 1.0 and 2.0
}
