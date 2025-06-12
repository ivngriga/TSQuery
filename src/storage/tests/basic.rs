use super::*;

#[test]
fn test_table_creation() {
    let schema = test_schema();
    let config = test_config();
    let table = Table::new(schema.clone(), config);
    
    assert_eq!(table.schema(), &schema);
    assert_eq!(table.memory_usage(), 0);
}

#[test]
fn test_ingest_basic() {
    let table = Table::new(test_schema(), test_config());
    let batch = create_test_batch(
        vec![1_000_000_000, 2_000_000_000], 
        vec![1.0, 2.0],
        table.schema()
    );
    
    table.ingest(batch).unwrap();
    assert_eq!(table.memory_usage() > 0, true);
}

/*
#[test]
fn test_query_with_data() {
    let table = Table::new(test_schema(), test_config());
    let batch = create_test_batch(
        vec![1_000_000_000, 2_000_000_000],
        vec![1.0, 2.0],
        table.schema()
    );
    
    table.ingest(batch).unwrap();
    let results = table.query(None);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 2);
}
*/