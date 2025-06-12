
use arrow::{array::{
    ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, StringArray, TimestampNanosecondArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
}, datatypes::DataType};

use super::QueryError;

pub enum PrimitiveArrayRef<'a> {
    Float64(&'a Float64Array),
    Float32(&'a Float32Array),
    Int64(&'a Int64Array),
    Int32(&'a Int32Array),
    Int16(&'a Int16Array),
    Int8(&'a Int8Array),
    UInt64(&'a UInt64Array),
    UInt32(&'a UInt32Array),
    UInt16(&'a UInt16Array),
    UInt8(&'a UInt8Array),
}

impl<'a> PrimitiveArrayRef<'a> {
    pub fn sum(&self) -> Option<f64> {
        match self {
            PrimitiveArrayRef::Float64(arr) => arrow::compute::kernels::aggregate::sum(arr),
            PrimitiveArrayRef::Float32(arr) => arrow::compute::kernels::aggregate::sum(arr).map(|v| v as f64),
            PrimitiveArrayRef::Int64(arr) => arrow::compute::kernels::aggregate::sum(arr).map(|v| v as f64),
            PrimitiveArrayRef::Int32(arr) => arrow::compute::kernels::aggregate::sum(arr).map(|v| v as f64),
            PrimitiveArrayRef::Int16(arr) => arrow::compute::kernels::aggregate::sum(arr).map(|v| v as f64),
            PrimitiveArrayRef::Int8(arr) => arrow::compute::kernels::aggregate::sum(arr).map(|v| v as f64),
            PrimitiveArrayRef::UInt64(arr) => arrow::compute::kernels::aggregate::sum(arr).map(|v| v as f64),
            PrimitiveArrayRef::UInt32(arr) => arrow::compute::kernels::aggregate::sum(arr).map(|v| v as f64),
            PrimitiveArrayRef::UInt16(arr) => arrow::compute::kernels::aggregate::sum(arr).map(|v| v as f64),
            PrimitiveArrayRef::UInt8(arr) => arrow::compute::kernels::aggregate::sum(arr).map(|v| v as f64),
        }
    }

    pub fn mean(&self) -> Option<f64> {
        match self {
            PrimitiveArrayRef::Float64(arr) => arrow::compute::kernels::aggregate::sum(arr).map(|v| v / arr.len() as f64),
            PrimitiveArrayRef::Float32(arr) => arrow::compute::kernels::aggregate::sum(arr).map(|v| (v as f64) / arr.len() as f64),
            PrimitiveArrayRef::Int64(arr) => arrow::compute::kernels::aggregate::sum(arr).map(|v| (v as f64) / arr.len() as f64),
            PrimitiveArrayRef::Int32(arr) => arrow::compute::kernels::aggregate::sum(arr).map(|v| (v as f64) / arr.len() as f64),
            PrimitiveArrayRef::Int16(arr) => arrow::compute::kernels::aggregate::sum(arr).map(|v| (v as f64) / arr.len() as f64),
            PrimitiveArrayRef::Int8(arr) => arrow::compute::kernels::aggregate::sum(arr).map(|v| (v as f64) / arr.len() as f64),
            PrimitiveArrayRef::UInt64(arr) => arrow::compute::kernels::aggregate::sum(arr).map(|v| (v as f64) / arr.len() as f64),
            PrimitiveArrayRef::UInt32(arr) => arrow::compute::kernels::aggregate::sum(arr).map(|v| (v as f64) / arr.len() as f64),
            PrimitiveArrayRef::UInt16(arr) => arrow::compute::kernels::aggregate::sum(arr).map(|v| (v as f64) / arr.len() as f64),
            PrimitiveArrayRef::UInt8(arr) => arrow::compute::kernels::aggregate::sum(arr).map(|v| (v as f64) / arr.len() as f64),

        }
    }

    pub fn min(&self) -> Option<f64> {
        match self {
            PrimitiveArrayRef::Float64(arr) => arrow::compute::kernels::aggregate::min(arr),
            PrimitiveArrayRef::Float32(arr) => arrow::compute::kernels::aggregate::min(arr).map(|v| v as f64),
            PrimitiveArrayRef::Int64(arr) => arrow::compute::kernels::aggregate::min(arr).map(|v| v as f64),
            PrimitiveArrayRef::Int32(arr) => arrow::compute::kernels::aggregate::min(arr).map(|v| v as f64),
            PrimitiveArrayRef::Int16(arr) => arrow::compute::kernels::aggregate::min(arr).map(|v| v as f64),
            PrimitiveArrayRef::Int8(arr) => arrow::compute::kernels::aggregate::min(arr).map(|v| v as f64),
            PrimitiveArrayRef::UInt64(arr) => arrow::compute::kernels::aggregate::min(arr).map(|v| v as f64),
            PrimitiveArrayRef::UInt32(arr) => arrow::compute::kernels::aggregate::min(arr).map(|v| v as f64),
            PrimitiveArrayRef::UInt16(arr) => arrow::compute::kernels::aggregate::min(arr).map(|v| v as f64),
            PrimitiveArrayRef::UInt8(arr) => arrow::compute::kernels::aggregate::min(arr).map(|v| v as f64),
        }
    }

    pub fn max(&self) -> Option<f64> {
        match self {
            PrimitiveArrayRef::Float64(arr) => arrow::compute::kernels::aggregate::max(arr),
            PrimitiveArrayRef::Float32(arr) => arrow::compute::kernels::aggregate::max(arr).map(|v| v as f64),
            PrimitiveArrayRef::Int64(arr) => arrow::compute::kernels::aggregate::max(arr).map(|v| v as f64),
            PrimitiveArrayRef::Int32(arr) => arrow::compute::kernels::aggregate::max(arr).map(|v| v as f64),
            PrimitiveArrayRef::Int16(arr) => arrow::compute::kernels::aggregate::max(arr).map(|v| v as f64),
            PrimitiveArrayRef::Int8(arr) => arrow::compute::kernels::aggregate::max(arr).map(|v| v as f64),
            PrimitiveArrayRef::UInt64(arr) => arrow::compute::kernels::aggregate::max(arr).map(|v| v as f64),
            PrimitiveArrayRef::UInt32(arr) => arrow::compute::kernels::aggregate::max(arr).map(|v| v as f64),
            PrimitiveArrayRef::UInt16(arr) => arrow::compute::kernels::aggregate::max(arr).map(|v| v as f64),
            PrimitiveArrayRef::UInt8(arr) => arrow::compute::kernels::aggregate::max(arr).map(|v| v as f64),
        }
    }
}

pub fn downcast_arr<'a>(
    array_ref: &'a ArrayRef,
    data_type: &DataType,
) -> Result<PrimitiveArrayRef<'a>, QueryError> {
    match data_type {
        DataType::Float64 => {
            let arr = array_ref
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| QueryError::TypeError("Failed to downcast to Float64Array".into()))?;
            Ok(PrimitiveArrayRef::Float64(arr))
        }
        DataType::Float32 => {
            let arr = array_ref
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| QueryError::TypeError("Failed to downcast to Float32Array".into()))?;
            Ok(PrimitiveArrayRef::Float32(arr))
        }
        DataType::Int64 => {
            let arr = array_ref
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| QueryError::TypeError("Failed to downcast to Int64Array".into()))?;
            Ok(PrimitiveArrayRef::Int64(arr))
        }
        DataType::Int32 => {
            let arr = array_ref
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| QueryError::TypeError("Failed to downcast to Int32Array".into()))?;
            Ok(PrimitiveArrayRef::Int32(arr))
        }
        DataType::Int16 => {
            let arr = array_ref
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| QueryError::TypeError("Failed to downcast to Int16Array".into()))?;
            Ok(PrimitiveArrayRef::Int16(arr))
        }
        DataType::Int8 => {
            let arr = array_ref
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| QueryError::TypeError("Failed to downcast to Int8Array".into()))?;
            Ok(PrimitiveArrayRef::Int8(arr))
        }
        DataType::UInt64 => {
            let arr = array_ref
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| QueryError::TypeError("Failed to downcast to UInt64Array".into()))?;
            Ok(PrimitiveArrayRef::UInt64(arr))
        }
        DataType::UInt32 => {
            let arr = array_ref
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| QueryError::TypeError("Failed to downcast to UInt32Array".into()))?;
            Ok(PrimitiveArrayRef::UInt32(arr))
        }
        DataType::UInt16 => {
            let arr = array_ref
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| QueryError::TypeError("Failed to downcast to UInt16Array".into()))?;
            Ok(PrimitiveArrayRef::UInt16(arr))
        }
        DataType::UInt8 => {
            let arr = array_ref
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| QueryError::TypeError("Failed to downcast to UInt8Array".into()))?;
            Ok(PrimitiveArrayRef::UInt8(arr))
        }
        _ => Err(QueryError::TypeError(format!(
            "Unsupported data type for aggregation: {:?}",
            data_type
        ))),
    }
}
