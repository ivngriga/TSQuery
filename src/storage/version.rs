use std::sync::Arc;
use parking_lot::RwLock;
use std::path::PathBuf;
use arrow::record_batch::RecordBatch;
use arrow::array::ArrayRef;
use arrow::compute::concat;

#[derive(Debug)]
pub struct Version {
    pub data: Arc<RecordBatch>,
    pub spill_files: Vec<PathBuf>,
    pub next: RwLock<Option<VersionRef>>,
}

pub type VersionRef = Arc<Version>;

impl Version {
    pub fn new(batch: RecordBatch, spill_files: Vec<PathBuf>) -> Self {
        Self {
            data: Arc::new(batch),
            next: RwLock::new(None),
            spill_files: spill_files,
        }
    }

    pub fn collect_all_batches(&self) -> Vec<Arc<RecordBatch>> {
        let mut batches = Vec::new();
        batches.push(Arc::clone(&self.data));

        if let Some(prev_version) = self.next.read().as_ref() {
            batches.extend(prev_version.collect_all_batches());
        }

        batches
    }
}

pub fn concat_batches(batches: &[Arc<RecordBatch>]) -> arrow::error::Result<RecordBatch> {
    if batches.is_empty() {
        panic!("No batches to concatenate");
    }

    let schema = batches[0].schema();
    let num_columns = schema.fields().len();

    let mut concatenated_columns: Vec<ArrayRef> = Vec::with_capacity(num_columns);

    for col_idx in 0..num_columns {
        // Collect arrays for this column from all batches
        let arrays: Vec<&ArrayRef> = batches.iter()
            .map(|batch| batch.column(col_idx))
            .collect();

        let arrays_ref: Vec<&dyn arrow::array::Array> = arrays.iter()
            .map(|arc_array| arc_array.as_ref() as &dyn arrow::array::Array)
            .collect();

        // Concatenate arrays for this column
        let concatenated = concat(&arrays_ref)?;
        concatenated_columns.push(concatenated);
    }

    RecordBatch::try_new(schema.clone(), concatenated_columns)
}