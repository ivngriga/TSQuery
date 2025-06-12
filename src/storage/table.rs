use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::ipc::reader::FileReader;
use chrono::{DateTime, Utc, NaiveDate};
use chrono::TimeZone;
use chrono::Datelike;
use chrono::Timelike;

use arrow::{
    datatypes::Schema,
    record_batch::RecordBatch,
    ipc::writer::FileWriter,
};
use parking_lot::Mutex;
use super::{Error, StorageConfig, version::{Version, VersionRef}};

use crate::storage::version::concat_batches;
use crate::storage::PartitionGranularity;

trait TimestampExt {
    fn from_timestamp_nanos(ts: i64) -> DateTime<Utc>;
}

impl TimestampExt for Utc {
    fn from_timestamp_nanos(ts: i64) -> DateTime<Utc> {
        let secs = ts / 1_000_000_000;
        let nanos = (ts % 1_000_000_000) as u32;
        Utc.timestamp_opt(secs, nanos).unwrap()
    }
}

#[derive(Clone)]
pub struct Partition {
    pub current_version: VersionRef,
    min_time: DateTime<Utc>,
    max_time: DateTime<Utc>,
}

pub struct Table {
    schema: Arc<Schema>,
    state: Mutex<TableState>,
    config: StorageConfig,
}

struct TableState {
    partitions: BTreeMap<String, Partition>, // Changed to String key
    memory_usage: usize,
    pub stats: TableStats,
}

impl Table {
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    

    pub fn memory_usage(&self) -> usize {
        self.state.lock().memory_usage
    }

    pub fn stats(&self) -> TableStats {
        self.state.lock().stats.clone()
    }

    pub fn partitions(&self) -> BTreeMap<String, Partition> {
        let state = self.state.lock();
        state.partitions.clone()
    }
}

impl Table {
    /// Returns partition keys for partitions that overlap with the given time range
    pub fn relevant_partitions(
        &self,
        min_time: Option<DateTime<Utc>>,
        max_time: Option<DateTime<Utc>>,
    ) -> Vec<String> {
        let state = self.state.lock();
        state.partitions.iter()
            .filter(|(_, partition)| {
                let matches_min = min_time.map_or(true, |min| partition.max_time >= min);
                let matches_max = max_time.map_or(true, |max| partition.min_time <= max);
                matches_min && matches_max
            })
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Loads all data for a partition as a single RecordBatch by traversing MVCC chain
    pub fn load_partition(&self, partition_key: &str) -> Result<RecordBatch, Error> {
        let partition = {
            let state = self.state.lock();
            state.partitions.get(partition_key)
                .ok_or(Error::InvalidPartition(partition_key.to_string()))?
                .clone()
        };

        // Collect all versions
        let mut batches = Vec::new();
        let mut current_version = Some(partition.current_version);
        
        while let Some(version) = current_version {
            // Add in-memory data
            batches.push(version.data.clone());

            // Add spilled data if any
            if !version.spill_files.is_empty() {
                batches.extend(load_spill_files(&version.spill_files)?);
            }

            // Move to next version
            current_version = version.next.read().as_ref().map(Arc::clone);
        }

        // Combine all batches
        concat_batches(&batches)
            .map_err(|e| Error::BatchConcatError(e.to_string()))
    }
}

#[derive(Debug)]
#[derive(Clone)]
pub struct TableStats {
    pub total_rows: usize, 
    pub spilled_rows: usize,
    pub query_count: usize,
}

impl Table {
    pub fn new(schema: Arc<Schema>, config: StorageConfig) -> Self {
        // Create spill directory if needed
        if let Some(spill_path) = &config.spill_path {
            std::fs::create_dir_all(spill_path).expect("Failed to create spill directory");
        }

        Self {
            schema,
            state: Mutex::new(TableState {
                partitions: BTreeMap::new(),
                memory_usage: 0,
                stats: TableStats {
                    total_rows: 0,
                    spilled_rows: 0,
                    query_count: 0,
                },
            }),
            config,
        }
    }

    fn spill_partition(&self, date: String) -> Result<(), Error> {
        let spill_path = self.config.spill_path.as_ref()
            .ok_or(Error::MemoryLimit("No spill path configured".into()))?;

        // 1. Check if we should spill (brief lock)
        let (should_spill, partition_size) = {
            let state = self.state.lock();
            (
                state.partitions.contains_key(&date),
                state.partitions.get(&date)
                    .map(|p| p.current_version.data.get_array_memory_size())
                    .unwrap_or(0)
            )
        };

        if !should_spill {
            return Ok(());
        }

        // 2. Get data to spill (brief lock)
        let (schema, data) = {
            let state = self.state.lock();
            let partition = state.partitions.get(&date).unwrap();
            (
                partition.current_version.data.schema().clone(),
                concat_batches(&partition.current_version.collect_all_batches()),
            )
        };

        if data.is_err() {
            return Err(Error::BatchConcatError("failed to spill partition".to_string()))
        }

        let data = data.unwrap();

        // 3. Perform spill with atomic write
        let file_path = spill_path.join(format!("{}_{}.arrow", 
            date.replace(":", "-").replace("T", "_"), // Make filesystem-safe
            generate_random_string(10)));
        let temp_path = file_path.with_extension(".tmp");
        
        let file = std::fs::File::create(&temp_path)?;
        let mut writer = FileWriter::try_new(file, &schema)?;
        writer.write(&data)?;
        writer.finish()?;
        std::fs::rename(temp_path, &file_path)?;

        // 4. Update state (brief lock)
        {
            let mut state = self.state.lock();
            if partition_size > state.memory_usage {
                state.memory_usage = 0;
            } else {
                state.memory_usage -= partition_size;
            }
            state.stats.spilled_rows += data.num_rows();
            state.stats.total_rows -= data.num_rows();
            
            
            
            if let Some(partition) = state.partitions.get_mut(&date) {
                let mut spill_files = partition.current_version.spill_files.clone();
                spill_files.push(file_path);

                partition.current_version = Arc::new(Version::new(
                    RecordBatch::new_empty(self.schema.clone()),
                    spill_files,
                ));
            }
        }

        Ok(())
    }

    pub fn ingest(&self, batch: RecordBatch) -> Result<(), Error> {
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(Error::SchemaMismatch(
                "Batch schema does not match table schema".to_string(),
            ));
        }

        // Get partition keys mapped to row indices
        let partition_keys_map = extract_partition_keys(&batch, &self.config.partition_granularity)?;

        let mut total_new_rows = 0;
        let mut total_batch_size = 0;

        for (partition_key, row_indices) in partition_keys_map {
            // Create an Arrow UInt32Array of row indices for 'take'
            let indices_array = arrow::array::UInt32Array::from(
                row_indices.iter().map(|&i| i as u32).collect::<Vec<_>>()
            );

            // Select columns for this partition using 'take' kernel
            let columns: Vec<_> = batch.columns()
                .iter()
                .map(|col| arrow::compute::take(col.as_ref(), &indices_array, None))
                .collect::<Result<_, _>>()?;

            let partition_batch = RecordBatch::try_new(batch.schema(), columns)?;

            let partition_batch_size = partition_batch.get_array_memory_size();

            // Lock state to update or create partition
            {
                let mut state = self.state.lock();

                let partition = state.partitions.entry(partition_key.clone())
                    .or_insert_with(|| Partition {
                        current_version: Arc::new(Version::new(
                            RecordBatch::new_empty(self.schema.clone()),
                            Vec::new(),
                        )),
                        min_time: DateTime::<Utc>::MAX_UTC,
                        max_time: DateTime::<Utc>::MIN_UTC,
                    });

                let new_version = Arc::new(Version::new(
                    partition_batch, 
                    partition.current_version.spill_files.clone(),
                ));

                update_partition_time_bounds(partition, &new_version.data)?;

                let old_current = std::mem::replace(
                    &mut partition.current_version,
                    new_version.clone()
                );
                *new_version.next.write() = Some(old_current);

                total_new_rows += new_version.data.num_rows();
                total_batch_size += partition_batch_size;
            }
        }

        // Update global state memory usage and stats once after all partitions ingested
        {
            let mut state = self.state.lock();
            state.memory_usage += total_batch_size;
            state.stats.total_rows += total_new_rows;
        }

        // Check if spilling is needed (same logic as before)
        let spill_date_opt = {
            let state = self.state.lock();

            if let Some(limit) = self.config.memory_limit {
                println!("Memory check - current: {}, batch: {}, limit: {}", state.memory_usage, total_batch_size, limit);

                if state.memory_usage > limit {
                    println!("Memory limit exceeded, looking for partition to spill");

                    let date_to_spill = state.partitions.iter()
                        .max_by_key(|(_, partition)| {
                            partition.current_version.collect_all_batches()
                                .iter()
                                .map(|batch| batch.get_array_memory_size())
                                .sum::<usize>()
                        })
                        .map(|(date, _)| date.clone());

                    if date_to_spill.is_none() {
                        return Err(Error::MemoryLimit("No partitions available to spill".into()));
                    }

                    date_to_spill
                } else {
                    None
                }
            } else {
                None
            }
        };

        if let Some(date) = spill_date_opt {
            println!("Spilling partition for date {:?}", date);
            let spill_result = self.spill_partition(date);
            if spill_result.is_err() {
                return Err(Error::MemoryLimit("Failed to spill partition".into()));
            }
        }

        Ok(())
    }
}

pub fn load_spill_files(paths: &[PathBuf]) -> Result<Vec<Arc<RecordBatch>>, Error> {
    let mut batches = Vec::new();
    
    for path in paths {
        let file = std::fs::File::open(path)?;
        let reader = FileReader::try_new(file, None)?;
        
        for maybe_batch in reader {
            let batch = maybe_batch?;
            batches.push(Arc::new(batch));
        }
    }

    Ok(batches)
}

use std::collections::HashMap;

fn extract_partition_keys(batch: &RecordBatch, granularity: &PartitionGranularity) -> Result<HashMap<String, Vec<usize>>, Error> {
    // Assume first column is timestamp
    let timestamps = batch.column(0)
        .as_any()
        .downcast_ref::<arrow::array::TimestampNanosecondArray>()
        .ok_or(Error::InvalidTimestamp("First column must be timestamp".to_string()))?;

    let partition_keys: Vec<String> = timestamps.iter()
        .map(|opt_ts| {
            opt_ts.map(|ts| {
                let dt = DateTime::from_timestamp_nanos(ts);
                match granularity {
                    PartitionGranularity::Minute => format!("{}-{:02}-{:02}T{:02}:{:02}", dt.year(), dt.month(), dt.day(), dt.hour(), dt.minute()),
                    PartitionGranularity::Hour => format!("{}-{:02}-{:02}T{:02}", dt.year(), dt.month(), dt.day(), dt.hour()),
                    PartitionGranularity::Day => format!("{}-{:02}-{:02}", dt.year(), dt.month(), dt.day()),
                    PartitionGranularity::Month => format!("{}-{:02}", dt.year(), dt.month()),
                    PartitionGranularity::Year => dt.year().to_string(),
                }
            }).unwrap_or_else(|| "null".to_string())  // or handle null timestamps appropriately
        })
        .collect();

    let mut partitions_map: HashMap<String, Vec<usize>> = HashMap::new();

    for (row_idx, key) in partition_keys.iter().enumerate() {
        partitions_map.entry(key.clone()).or_default().push(row_idx);
    }

    Ok(partitions_map)
}

fn update_partition_time_bounds(partition: &mut Partition, batch: &RecordBatch) -> Result<(), Error> {
    let timestamps = batch.column(0)
        .as_any()
        .downcast_ref::<arrow::array::TimestampNanosecondArray>()
        .ok_or(Error::InvalidTimestamp("First column must be timestamp".to_string()))?;

    for ts in timestamps.iter().flatten() {
        let dt = DateTime::from_timestamp_nanos(ts);
        partition.min_time = partition.min_time.min(dt);
        partition.max_time = partition.max_time.max(dt);
    }

    Ok(())
}

use rand::{rng, Rng};
use rand::distr::Alphanumeric;

fn generate_random_string(len: usize) -> String {
    rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}
