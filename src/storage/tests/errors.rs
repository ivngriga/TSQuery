use super::*;
use arrow::array::StringArray;

#[test]
fn test_schema_mismatch() {
    let table = Table::new(test_schema(), test_config());
    
    // Create batch with wrong schema
    let bad_schema = Schema::new(vec![
        Field::new("time", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(bad_schema),
        vec![
            Arc::new(StringArray::from(vec!["not a timestamp"])),
            Arc::new(Float64Array::from(vec![1.0])),
        ]
    ).unwrap();
    
    let result = table.ingest(batch);
    assert!(matches!(result, Err(Error::SchemaMismatch(_))));
}