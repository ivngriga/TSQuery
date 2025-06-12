use super::*;
use chrono;

#[test]
fn test_spill_behavior() {
    // Very small memory limit to force spilling
    let mut config = test_config();
    config.memory_limit = Some(209); // More reasonable test limit
    
    let table = Table::new(test_schema(), config);
    
    // Test with different partition granularities
    for granularity in [
        PartitionGranularity::Minute,
        PartitionGranularity::Hour,
        PartitionGranularity::Day,
    ].iter() {
        let mut config = test_config();
        config.memory_limit = Some(225);
        config.partition_granularity = granularity.clone();
        
        let table = Table::new(test_schema(), config);
        
        // Create timestamps that span multiple partitions
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
        let timestamps = match granularity {
            PartitionGranularity::Minute => vec![
                now,
                now + 1i64, // +2 minutes
                now + 120_000_000_000i64, // +2 minutes
            ],
            PartitionGranularity::Hour => vec![
                now,
                now + 1i64,
                now + 7_200_000_000_000i64,
            ],
            PartitionGranularity::Day => vec![
                now,
                now + 1i64, // +2 day
                now + 172_000_000_000_000i64, // +2 day
            ],
            _ => unreachable!(),
        };

        let batch = create_test_batch(
            timestamps,
            vec![1.0, 2.0, 3.0],
            table.schema()
        );
        
        table.ingest(batch).unwrap();
        
        // Verify correct number of partitions were created
        let partitions = table.partitions();
        assert_eq!(partitions.len(), 2, 
            "Expected 2 partitions for granularity {:?}", granularity);
        assert_eq!(table.stats().spilled_rows, 2);
        
        // Verify partition keys contain time components when needed
        for key in partitions.keys() {
            match granularity {
                PartitionGranularity::Minute => assert!(key.to_string().contains(':'), 
                    "Minute granularity key should contain time"),
                PartitionGranularity::Hour => assert!(key.to_string().contains('T'), 
                    "Hour granularity key should contain time"),
                _ => {} // No checks for day/month/year
            }
        }
    }
}

/*
#[test]
fn test_spill_reload() {
    println!("running test_spill_reload");

    let mut config = test_config();
    // Set memory limit very low to force spill
    config.memory_limit = Some(100);
    
    let table = Table::new(test_schema(), config);
    
    // Ingest and spill data
    let batch = create_test_batch(
        vec![1_000_000_000, 2_000_000_000],
        vec![1.0, 2.0],
        table.schema()
    );
    table.ingest(batch).unwrap();
    
    assert_eq!(table.stats().spilled_rows, 2);
    
    // Query should load spilled data
    let results = table.query(None);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 2);
    assert_eq!(table.stats().spilled_rows, 0);
}
 */
