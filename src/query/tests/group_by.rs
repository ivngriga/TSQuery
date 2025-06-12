use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Float64Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use arrow_array::{Int64Array, StringArray};

use crate::storage::{Compression, PartitionGranularity, StorageConfig, Table};
use crate::query::QueryExecutor;

fn create_group_test_table() -> Table {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("value", DataType::Float64, false),
        Field::new("category", DataType::Utf8, false),
    ]));

    let config = StorageConfig {
        memory_limit: None,
        spill_path: None,
        spill_compression: Compression::None,
        partition_granularity: PartitionGranularity::Day,
    };

    Table::new(schema, config)
}

fn create_group_test_batch(
    timestamps: Vec<i64>,
    values: Vec<f64>,
    categories: Vec<&str>,
    schema: &Schema
) -> RecordBatch {
    let time_array = TimestampNanosecondArray::from(timestamps);
    let value_array = Float64Array::from(values);
    let category_array = StringArray::from(categories);

    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(time_array), Arc::new(value_array), Arc::new(category_array)],
    ).unwrap()
}

#[test]
fn test_group_by_time_hour() {
    let table = create_group_test_table();
    let batch = create_group_test_batch(
        vec![
            1_000_000_000,          // 1s (1970-01-01 00:00:01)
            3_600_000_000_000,      // 1h (1970-01-01 01:00:00)
            3_600_000_000_001,      // 1h + 1ns
            7_200_000_000_000,      // 2h (1970-01-01 02:00:00)
        ],
        vec![1.0, 2.0, 3.0, 4.0],
        vec!["a", "b", "c", "d"],
        table.schema()
    );
    table.ingest(batch).unwrap();

    let executor = QueryExecutor;
    let result = executor.execute_query(
        "SELECT time, SUM(value) FROM `table` GROUP BY timestamp(time, '10 MINUTES')",
        &table
    ).unwrap();

    // println!("time hour res {:?}", result);
    // print_batches(&[result]);

    assert_eq!(result.num_rows(), 3); // Should have 3 time buckets
    assert_eq!(result.num_columns(), 2);
}

#[test]
fn test_group_by_time_day() {
    let table = create_group_test_table();
    let batch = create_group_test_batch(
        vec![
            0,                      // 0s (1970-01-01 00:00:00)
            86_400_000_000_000,     // 1d (1970-01-02 00:00:00)
            86_400_000_000_001,     // 1d + 1ns
            172_800_000_000_000,    // 2d (1970-01-03 00:00:00)
        ],
        vec![1.0, 2.0, 3.0, 4.0],
        vec!["a", "b", "c", "d"],
        table.schema()
    );
    table.ingest(batch).unwrap();

    let executor = QueryExecutor;
    let result = executor.execute_query(
        "SELECT time, AVG(value) FROM `table` GROUP BY timestamp(time, '1 DAY')",
        &table
    ).unwrap();

    assert_eq!(result.num_rows(), 3); // Should have 3 day buckets
}

#[test]
fn test_group_by_with_sum() {
    let table = create_group_test_table();
    let batch = create_group_test_batch(
        vec![0_000_000_000, 0_000_000_001, 86_400_000_000_000],
        vec![1.0, 2.0, 3.0],
        vec!["a", "a", "b"],
        table.schema()
    );
    table.ingest(batch).unwrap();

    let executor = QueryExecutor;
    let result = executor.execute_query(
        "SELECT category, SUM(value) FROM `table` GROUP BY timestamp(time, '1 HOUR')",
        &table
    ).unwrap();

    assert_eq!(result.num_rows(), 2);
    // Verify sums
    let sum_col = result.column(1).as_any().downcast_ref::<Float64Array>().unwrap();

    for i in 0..sum_col.len() {
        let val = sum_col.value(i);
        assert!((val - 3.0).abs() < f64::EPSILON, "Value {} is not approximately 3.0", val);
    }

}

#[test]
fn test_group_by_with_avg() {
    let table = create_group_test_table();
    let batch = create_group_test_batch(
        vec![0_000_000_000, 0_000_000_001, 86_400_000_000_000],
        vec![1.0, 2.0, 3.0],
        vec!["a", "a", "b"],
        table.schema()
    );
    table.ingest(batch).unwrap();

    let executor = QueryExecutor;
    let result = executor.execute_query(
        "SELECT category, AVG(value) FROM `table` GROUP BY timestamp(time, '1 HOUR')",
        &table
    ).unwrap();

    assert_eq!(result.num_rows(), 2);
    // Verify averages  
    let avg_col = result.column(1).as_any().downcast_ref::<Float64Array>().unwrap();

    let mut found_1_5 = false;
    let mut found_3_0 = false;

    for i in 0..avg_col.len() {
        let val = avg_col.value(i);
        if (val - 1.5).abs() < f64::EPSILON {
            found_1_5 = true;
        } else if (val - 3.0).abs() < f64::EPSILON {
            found_3_0 = true;
        }
    }

    assert!(found_1_5, "Expected average value 1.5 not found");
    assert!(found_3_0, "Expected average value 3.0 not found");

}

#[test]
fn test_group_by_with_count() {
    let table = create_group_test_table();
    let batch = create_group_test_batch(
        vec![0_000_000_000, 0_000_000_001, 86_400_000_000_000],
        vec![1.0, 2.0, 3.0],
        vec!["a", "a", "b"],
        table.schema()
    );
    table.ingest(batch).unwrap();

    let executor = QueryExecutor;
    let result = executor.execute_query(
        "SELECT category, COUNT(value) FROM `table` GROUP BY timestamp(time, '1 HOUR')",
        &table
    ).unwrap();

    assert_eq!(result.num_rows(), 2);
    // // Verify counts
    let count_col = result.column(1).as_any().downcast_ref::<Int64Array>().unwrap();

    // Collect all values into a HashSet for unordered membership check
    let values: HashSet<i64> = (0..count_col.len())
        .map(|i| count_col.value(i))
        .collect();

    // Assert that both 1 and 2 are present in the array
    assert!(values.contains(&1), "Expected value 1 not found");
    assert!(values.contains(&2), "Expected value 2 not found");
}

#[test]
fn test_group_by_with_min() {
    let table = create_group_test_table();
    let batch = create_group_test_batch(
        vec![0_000_000_000, 0_000_000_001, 86_400_000_000_000],
        vec![1.0, 2.0, 3.0],
        vec!["a", "a", "b"],
        table.schema()
    );
    table.ingest(batch).unwrap();

    let executor = QueryExecutor;
    let result = executor.execute_query(
        "SELECT category, MIN(value) FROM `table` GROUP BY timestamp(time, '1 HOUR')",
        &table
    ).unwrap();

    assert_eq!(result.num_rows(), 2);
    // Verify mins
    let min_col = result.column(1).as_any().downcast_ref::<Float64Array>().unwrap();

    let mut found_1_0 = false;
    let mut found_3_0 = false;

    for i in 0..min_col.len() {
        let val = min_col.value(i);
        if (val - 1.0).abs() < f64::EPSILON {
            found_1_0 = true;
        } else if (val - 3.0).abs() < f64::EPSILON {
            found_3_0 = true;
        }
    }

    assert!(found_1_0, "Expected minimum value 1.0 not found");
    assert!(found_3_0, "Expected minimum value 3.0 not found");

}

#[test]
fn test_group_by_with_max() {
    let table = create_group_test_table();
    let batch = create_group_test_batch(
        vec![0_000_000_000, 0_000_000_001, 86_400_000_000_000],
        vec![1.0, 2.0, 3.0],
        vec!["a", "a", "b"],
        table.schema()
    );
    table.ingest(batch).unwrap();

    let executor = QueryExecutor;
    let result = executor.execute_query(
        "SELECT category, MAX(value) FROM `table` GROUP BY timestamp(time, '1 HOUR')",
        &table
    ).unwrap();

    assert_eq!(result.num_rows(), 2);
    // Verify maxes
    let max_col = result.column(1).as_any().downcast_ref::<Float64Array>().unwrap();

    let mut found_2_0 = false;
    let mut found_3_0 = false;

    for i in 0..max_col.len() {
        let val = max_col.value(i);
        if (val - 2.0).abs() < f64::EPSILON {
            found_2_0 = true;
        } else if (val - 3.0).abs() < f64::EPSILON {
            found_3_0 = true;
        }
    }

    assert!(found_2_0, "Expected maximum value 2.0 not found");
    assert!(found_3_0, "Expected maximum value 3.0 not found");

}

#[test]
fn test_group_by_with_multiple_aggregates() {
    let table = create_group_test_table();
    let batch = create_group_test_batch(
        vec![0_000_000_000, 0_000_000_001, 86_400_000_000_000],
        vec![1.0, 2.0, 3.0],
        vec!["a", "a", "b"],
        table.schema()
    );
    table.ingest(batch).unwrap();

    let executor = QueryExecutor;
    let result = executor.execute_query(
        "SELECT time, SUM(value), AVG(value), COUNT(value), MIN(value), MAX(value) FROM `table` GROUP BY timestamp(time, '1 HOUR')",
        &table
    ).unwrap();

    assert_eq!(result.num_rows(), 2);
    assert_eq!(result.num_columns(), 6);
    
    let sum_col = result.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
    let avg_col = result.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
    let count_col = result.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
    let min_col = result.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
    let max_col = result.column(5).as_any().downcast_ref::<Float64Array>().unwrap();

    // Helper closure for approximate float equality
    let approx_eq = |a: f64, b: f64| (a - b).abs() < f64::EPSILON;

    // Check sums: all sums are 3.0 and only one distinct value
    {
        for i in 0..sum_col.len() {
            let val = sum_col.value(i);
            assert!(approx_eq(val, 3.0), "Sum value {} is not approximately 3.0", val);
        }
    }

    // Check avgs contains 1.5 and 3.0
    {
        let mut found_1_5 = false;
        let mut found_3_0 = false;
        for i in 0..avg_col.len() {
            let val = avg_col.value(i);
            if approx_eq(val, 1.5) {
                found_1_5 = true;
            } else if approx_eq(val, 3.0) {
                found_3_0 = true;
            }
        }
        assert!(found_1_5, "Expected average value 1.5 not found");
        assert!(found_3_0, "Expected average value 3.0 not found");
    }

    // Check counts contains 1 and 2
    {
        let counts: HashSet<i64> = (0..count_col.len()).map(|i| count_col.value(i)).collect();
        assert!(counts.contains(&1), "Expected count value 1 not found");
        assert!(counts.contains(&2), "Expected count value 2 not found");
    }

    // Check mins contains 1.0 and 3.0
    {
        let mut found_1_0 = false;
        let mut found_3_0 = false;
        for i in 0..min_col.len() {
            let val = min_col.value(i);
            if approx_eq(val, 1.0) {
                found_1_0 = true;
            } else if approx_eq(val, 3.0) {
                found_3_0 = true;
            }
        }
        assert!(found_1_0, "Expected minimum value 1.0 not found");
        assert!(found_3_0, "Expected minimum value 3.0 not found");
    }

    // Check maxes contains 2.0 and 3.0
    {
        let mut found_2_0 = false;
        let mut found_3_0 = false;
        for i in 0..max_col.len() {
            let val = max_col.value(i);
            if approx_eq(val, 2.0) {
                found_2_0 = true;
            } else if approx_eq(val, 3.0) {
                found_3_0 = true;
            }
        }
        assert!(found_2_0, "Expected maximum value 2.0 not found");
        assert!(found_3_0, "Expected maximum value 3.0 not found");
    }

}
