use std::sync::Arc;
use std::time::Instant;
use arrow::array::{Float64Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use mysql::prelude::*;
use mysql::*;
use rand::Rng;
use crate::storage::{Compression, PartitionGranularity, StorageConfig, Table};
use crate::query::QueryExecutor;

const NUM_ROWS: usize = 1_000_000; // 1 million rows for meaningful benchmarks
const TIME_RANGE_NS: i64 = 30 * 24 * 60 * 60 * 1_000_000_000; // 30 days in nanoseconds

pub fn run_benchmarks() {
    println!("Running benchmarks with {} rows...", NUM_ROWS);
    
    // Setup our engine
    let (table, executor) = setup_our_engine();
    
    // Setup PostgreSQL
    let mut mysql_conn = setup_mysql().expect("Failed to setup MySQL");

    // Generate test data
    let (timestamps, values) = generate_test_data();

    // Ingest data benchmarks
    let our_ingest_time = benchmark_our_ingest(&table, &timestamps, &values);
    let mysql_ingest_time = benchmark_mysql_ingest(&mut mysql_conn, &timestamps, &values);

    // Query benchmarks
    benchmark_queries(&table, &executor, &mut mysql_conn);

    println!("\nBenchmark Results:");
    println!("-----------------");
    println!("\nData Ingestion Results:");
    println!("----------------------");
    println!("Total Time:");
    println!("Our Engine: {:.2}ms", our_ingest_time);
    println!("MySQL: {:.2}ms", mysql_ingest_time);
    println!("Total Speed Ratio (MySQL/Our): {:.2}x", mysql_ingest_time / our_ingest_time);
}

fn setup_our_engine() -> (Table, QueryExecutor) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Timestamp(TimeUnit::Nanosecond, None), false), 
        Field::new("value", DataType::Float64, false),
    ]));

    let config = StorageConfig {
        memory_limit: None, // No spilling for benchmarks
        spill_path: None,
        spill_compression: Compression::None,
        partition_granularity: PartitionGranularity::Hour, // Fine-grained for time queries
    };

    (Table::new(schema, config), QueryExecutor)
}

fn setup_mysql() -> Result<PooledConn, mysql::Error> {
    let url = "mysql://root@localhost:3306/rust_engine_benchmark";
    let pool = Pool::new(url)?;
    let mut conn = pool.get_conn()?;
    
    // Create test table
    conn.query_drop("DROP TABLE IF EXISTS benchmark_ts")?;
    
    conn.query_drop(
        "CREATE TABLE benchmark_ts (
            time DATETIME(6) NOT NULL,
            value DOUBLE NOT NULL
        )"
    )?;

    Ok(conn)
}

fn generate_test_data() -> (Vec<i64>, Vec<f64>) {
    let mut rng = rand::rng();
    let mut timestamps = Vec::with_capacity(NUM_ROWS);
    let mut values = Vec::with_capacity(NUM_ROWS);

    for _ in 0..NUM_ROWS {
        timestamps.push(rng.gen_range(0..TIME_RANGE_NS));
        values.push(rng.random::<f64>() * 100.0); // Random values 0-100
    }

    (timestamps, values)
}

fn benchmark_our_ingest(table: &Table, timestamps: &[i64], values: &[f64]) -> f64 {
    println!("Ingesting data into our engine...");
    let start = Instant::now();
    
    // Test different batch sizes
    let batch_sizes = [100, 1000, 10000];
    for &batch_size in &batch_sizes {
        let batch_start = Instant::now();
        let chunks = timestamps.chunks(batch_size).zip(values.chunks(batch_size));
        for (ts_chunk, val_chunk) in chunks {
            let batch = RecordBatch::try_new(
                table.schema().clone(),
                vec![
                    Arc::new(TimestampNanosecondArray::from(ts_chunk.to_vec())),
                    Arc::new(Float64Array::from(val_chunk.to_vec())),
                ],
            ).unwrap();
            table.ingest(batch).unwrap();
        }
        let duration = batch_start.elapsed().as_secs_f64() * 1000.0;
        println!("Our engine batch size {}: {:.2}ms", batch_size, duration);
    }
    
    let total_duration = start.elapsed().as_secs_f64() * 1000.0;
    println!("Our engine ingest completed in {:.2}ms", total_duration);
    total_duration
}

fn benchmark_mysql_ingest(conn: &mut PooledConn, timestamps: &[i64], values: &[f64]) -> f64 {
    println!("Ingesting data into MySQL...");
    let start = Instant::now();
    
    // Test different batch sizes
    let batch_sizes = [100, 1000, 10000];
    for &batch_size in &batch_sizes {
        let batch_start = Instant::now();
        let mut tx = conn.start_transaction(TxOpts::default()).unwrap();
        
        // Prepare batch insert statement
        let mut query = "INSERT INTO benchmark_ts (time, value) VALUES ".to_string();
        
        for (ts_chunk, val_chunk) in timestamps.chunks(batch_size).zip(values.chunks(batch_size)) {
            let mut params: Vec<Value> = Vec::with_capacity(batch_size * 2);

            for (&ts, &val) in ts_chunk.iter().zip(val_chunk) {
                let dt = chrono::DateTime::from_timestamp_nanos(ts);
                params.push(Value::from(dt.naive_utc().to_string()));
                params.push(Value::from(val));
            }

            let length = params.len() / 2;
            let values_placeholders = (0..length).map(|_| "(?, ?)").collect::<Vec<_>>().join(",");
            let query = format!("INSERT INTO benchmark_ts (time, value) VALUES {}", values_placeholders);

            tx.exec_drop(&query, params).unwrap();
        }
        tx.commit().unwrap();
        
        let duration = batch_start.elapsed().as_secs_f64() * 1000.0;
        println!("MySQL batch size {}: {:.2}ms", batch_size, duration);
    }
    
    let total_duration = start.elapsed().as_secs_f64() * 1000.0;
    println!("MySQL ingest completed in {:.2}ms", total_duration);
    total_duration
}

fn benchmark_queries(table: &Table, executor: &QueryExecutor, mysql_client: &mut PooledConn) {
    println!("\nRunning query benchmarks...");

    // 2. Hourly aggregation
    let our_time = benchmark_our_query(
        executor,
        table,
        "SELECT time, AVG(value), COUNT(value) FROM `table` GROUP BY bucket(time, '1 HOUR')"
    );
    let mysql_time = benchmark_mysql_query(
        mysql_client,
        "SELECT 
            FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(time)/3600)*3600) as bucket,
            AVG(value), COUNT(value) 
         FROM benchmark_ts 
         GROUP BY bucket"
    );
    print_ratio("Hourly aggregation", our_time, mysql_time);

    // 1. Simple time-range query
    let our_time = benchmark_our_query(
        executor, 
        table, 
        "SELECT time, value FROM `table` WHERE time > 86400000000000 AND time < 172800000000000"
    );
    let mysql_time = benchmark_mysql_query(
        mysql_client,
        "SELECT time, value FROM benchmark_ts 
         WHERE time > FROM_UNIXTIME(86400) AND time < FROM_UNIXTIME(172800)"
    );
    print_ratio("Time-range query", our_time, mysql_time);

    

    // 3. Daily aggregation with filter
    let our_time = benchmark_our_query(
        executor,
        table,
        "SELECT time, MAX(value), MIN(value) FROM `table` 
         WHERE value > 50.0 
         GROUP BY bucket(time, '1 DAY')"
    );
    let mysql_time = benchmark_mysql_query(
        mysql_client,
        "SELECT 
            FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(time)/86400)*86400) as bucket,
            MAX(value), MIN(value) 
         FROM benchmark_ts 
         WHERE value > 50.0
         GROUP BY bucket"
    );
    print_ratio("Daily filtered aggregation", our_time, mysql_time);
    
    let our_time = benchmark_our_query(
        executor,
        table,
        "SELECT time, MAX(value), MIN(value) FROM `table` 
         WHERE time > 86400000000000 AND time < 172800000000000
         GROUP BY bucket(time, '1 DAY')"
    );
    let mysql_time = benchmark_mysql_query(
        mysql_client,
        "SELECT 
            FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(time)/86400)*86400) as bucket,
            MAX(value), MIN(value) 
         FROM benchmark_ts 
         WHERE time > FROM_UNIXTIME(86400) AND time < FROM_UNIXTIME(172800)
         GROUP BY bucket"
    );
    print_ratio("Daily filtered aggregation", our_time, mysql_time);
}

fn benchmark_our_query(executor: &QueryExecutor, table: &Table, query: &str) -> f64 {
    let start = Instant::now();
    executor.execute_query(query, table).unwrap();
    start.elapsed().as_secs_f64() * 1000.0
}

fn benchmark_mysql_query(conn: &mut PooledConn, query: &str) -> f64 {
    let start = Instant::now();
    conn.query_iter(query).unwrap().for_each(|_| {});
    start.elapsed().as_secs_f64() * 1000.0
}

fn print_ratio(name: &str, our_time: f64, mysql_time: f64) {
    println!("\n{}:", name);
    println!("Our Engine: {:.2}ms", our_time);
    println!("MySQL: {:.2}ms", mysql_time);
    println!("Speed Ratio (MySQL/Our): {:.2}x", mysql_time / our_time);
}
