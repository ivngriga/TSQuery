/*
use super::*;
use chrono::{TimeZone, Utc};

#[test]
fn test_time_range_query() {
    let table = Table::new(test_schema(), test_config());
    
    // Add data across different days
    let batches = vec![
        create_test_batch(
            vec![1_000_000_000, 2_000_000_000], // Jan 1, 1970
            vec![1.0, 2.0],
            table.schema()
        ),
        create_test_batch(
            vec![86_400_000_000_000, 86_401_000_000_000], // Jan 2, 1970
            vec![3.0, 4.0],
            table.schema()
        ),
    ];
    
    for batch in batches {
        table.ingest(batch).unwrap();
    }

    // Query just first day
    let start = Utc.timestamp_opt(0, 0).unwrap();
    let end = Utc.timestamp_opt(86_399, 0).unwrap();
    let pred = Box::new(move |batch: &RecordBatch| {
        let times = batch.column(0).as_any()
            .downcast_ref::<TimestampNanosecondArray>().unwrap();
        times.iter().any(|t| {
            let t = t.unwrap();
            let dt = Utc.timestamp_nanos(t);
            dt >= start && dt <= end
        })
    });
    
    let results = table.query(Some(pred));
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 2);
}
 */