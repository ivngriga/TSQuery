use std::sync::Arc;
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use arrow_array::BooleanArray;
use crate::storage::{Table};
use crate::storage::version::VersionRef;

use super::table::load_spill_files;

impl Table {
    pub fn scan_partitions(
        &self,
        partition_keys: Vec<String>,
        row_filter: &Option<Box<dyn Fn(&RecordBatch) -> arrow_array::BooleanArray>>,
        selected_columns: Option<&[String]>,
    ) -> Vec<RecordBatch> {
        let partitions = self.partitions();
        let mut results: Vec<RecordBatch> = Vec::new();

        for key in partition_keys {
            if let Some(partition) = partitions.get(&key) {
                let batches_vec = self.scan_version_recursive(&partition.current_version, row_filter, selected_columns);
                results.extend(batches_vec);
            }
        }

        results
    }

    fn scan_version_recursive(
        &self,
        version: &VersionRef,
        row_filter: &Option<Box<dyn Fn(&RecordBatch) -> arrow_array::BooleanArray>>,
        selected_columns: Option<&[String]>,
    ) -> Vec<RecordBatch> {
        let mut results = Vec::new();
        let mut current_version = Some(version.clone());

        if let Some(v) = current_version {
            // Load spill files
            if let Some(batch) = self.scan_version(&v, true, row_filter, selected_columns) {
                results.push(batch);
            }
            current_version = v.next.read().as_ref().map(Arc::clone);
        }

        while let Some(v) = current_version {
            if let Some(batch) = self.scan_version(&v, false, row_filter, selected_columns) {
                results.push(batch);
            }
            current_version = v.next.read().as_ref().map(Arc::clone);
        }

        results
    }


    fn scan_version(
        &self,
        version: &VersionRef,
        should_load_spill_files: bool,
        row_filter: &Option<Box<dyn Fn(&RecordBatch) -> arrow_array::BooleanArray>>,
        selected_columns: Option<&[String]>,
    ) -> Option<RecordBatch> {
        let spill_data = if !version.spill_files.is_empty() && should_load_spill_files {
            load_spill_files( &version.spill_files).ok()
        } else {
            None
        };

        let mut batches = Vec::new();
        
        batches.push(Arc::clone(&version.data));

        if let Some(spilled) = spill_data {
            batches.extend(spilled);
        }

        if batches.is_empty() {
            return None
        }

        let projected_batches: Vec<RecordBatch> = if let Some(cols) = selected_columns {
            batches.iter()
                .filter_map(|batch| project_batch(batch, cols).ok())
                .collect()
        } else {
            batches.iter().map(|b| (**b).clone()).collect()
        };

        let batch_refs: Vec<&RecordBatch> = projected_batches.iter().collect();
        
        let combined_batch = arrow::compute::concat_batches(&batch_refs[0].schema(), batch_refs)
            .map_err(|e| {
                log::error!("Failed to concatenate batches: {}", e);
                e
            })
            .ok()?;

        if let Some(row_filter_func) = row_filter {
            let mask = row_filter_func(&combined_batch);
            filter_record_batch(&combined_batch, &mask).ok()
        } else {
            Some(combined_batch)
        }
    }
}

fn project_batch(
    batch: &RecordBatch,
    selected_columns: &[String],
) -> arrow::error::Result<RecordBatch> {
    let schema = batch.schema();
    let indices: Vec<usize> = selected_columns.iter()
        .filter_map(|col| schema.index_of(col).ok())
        .collect();

    batch.project(&indices)
}
