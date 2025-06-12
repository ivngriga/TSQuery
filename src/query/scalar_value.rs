use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, StringArray, TimestampNanosecondArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};
use std::sync::Arc;

#[derive(Debug)]
pub enum ScalarValue {
    TimestampNanosecond(i64),
    Float64(f64),
    Float32(f32),
    Int64(i64),
    Int32(i32),
    Int16(i16),
    Int8(i8),
    UInt64(u64),
    UInt32(u32),
    UInt16(u16),
    UInt8(u8),
    Date32(i32), // days since epoch
    Utf8(String),
    Boolean(bool),
    Null,
}

impl ScalarValue {
    /// Convert scalar to a one-element Arrow array for comparison
    pub fn to_array(&self, len: usize) -> ArrayRef {
        match self {
            ScalarValue::TimestampNanosecond(ts) => {
                let values = vec![*ts; len];
                Arc::new(TimestampNanosecondArray::from(values)) as ArrayRef
            }
            ScalarValue::Float64(f) => {
                let values = vec![*f; len];
                Arc::new(Float64Array::from(values)) as ArrayRef
            }
            ScalarValue::Float32(f) => {
                let values = vec![*f; len];
                Arc::new(Float32Array::from(values)) as ArrayRef
            }
            ScalarValue::Int64(i) => {
                let values = vec![*i; len];
                Arc::new(Int64Array::from(values)) as ArrayRef
            }
            ScalarValue::Int32(i) => {
                let values = vec![*i; len];
                Arc::new(Int32Array::from(values)) as ArrayRef
            }
            ScalarValue::Int16(i) => {
                let values = vec![*i; len];
                Arc::new(Int16Array::from(values)) as ArrayRef
            }
            ScalarValue::Int8(i) => {
                let values = vec![*i; len];
                Arc::new(Int8Array::from(values)) as ArrayRef
            }
            ScalarValue::UInt64(u) => {
                let values = vec![*u; len];
                Arc::new(UInt64Array::from(values)) as ArrayRef
            }
            ScalarValue::UInt32(u) => {
                let values = vec![*u; len];
                Arc::new(UInt32Array::from(values)) as ArrayRef
            }
            ScalarValue::UInt16(u) => {
                let values = vec![*u; len];
                Arc::new(UInt16Array::from(values)) as ArrayRef
            }
            ScalarValue::UInt8(u) => {
                let values = vec![*u; len];
                Arc::new(UInt8Array::from(values)) as ArrayRef
            }
            ScalarValue::Date32(days) => {
                let values = vec![*days; len];
                Arc::new(Date32Array::from(values)) as ArrayRef
            }
            ScalarValue::Utf8(s) => {
                let values = vec![s.as_str(); len];
                Arc::new(StringArray::from(values)) as ArrayRef
            }
            ScalarValue::Boolean(b) => {
                let values = vec![*b; len];
                Arc::new(BooleanArray::from(values)) as ArrayRef
            }
            ScalarValue::Null => {
                // Create an array of nulls with length len, using Float64Array as example
                Arc::new(Float64Array::from(vec![None; len])) as ArrayRef
            }
        }
    }
}
