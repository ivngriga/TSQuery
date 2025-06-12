use std::{collections::{HashMap}, num::ParseIntError, sync::Arc};

use arrow_array::{BooleanArray, StringArray};
use primitive_array_ref::downcast_arr;
use scalar_value::ScalarValue;
use sqlparser::{
    ast::{BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Query, Select, SelectItem, SetExpr, Statement, Value},
    dialect::GenericDialect,
    parser::Parser,
};
use chrono::{DateTime, Utc};
use crate::storage::{Table};
use arrow::{array::{ArrayRef, Float64Array, Float64Builder, Int64Builder, TimestampNanosecondArray}, datatypes::{DataType, Field, Schema}, record_batch::RecordBatch};

#[cfg(test)]
mod tests;

mod scalar_value;
mod primitive_array_ref;

#[derive(Debug)]
pub enum QueryError {
    MissingColumn(String),
    TypeError(String),
    ParseError(String),
    ExecutionError(String),
    UnsupportedFeature(String),
    InvalidAggregateFunction(String),
    InvalidArgument(String),
}

#[derive(Debug)]
enum SupportedGroupByExpr {
    Column(String),
    TimeBucket(i64), // Nanoseconds
}

#[derive(Debug)]
enum AggregateFunction {
    Sum(String),
    Avg(String),
    Count(String),
    Min(String),
    Max(String),
}

#[derive(Debug)]
struct TimePredicate {
    column: String,
    op: TimeOperator,
    value: DateTime<Utc>,
}

#[derive(Debug,PartialEq)]
enum TimeOperator {
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
    Equal,
}
pub struct QueryExecutor;

impl QueryExecutor {
    pub fn execute_query(&self, sql: &str, table: &Table) -> Result<RecordBatch, QueryError> {
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql)
            .map_err(|e| QueryError::ParseError(e.to_string()))?;

        if ast.len() != 1 {
            return Err(QueryError::UnsupportedFeature(
                "Only single-statement queries supported".into(),
            ));
        }

        match &ast[0] {
            Statement::Query(query) => self.execute_select(query, table),
            _ => Err(QueryError::UnsupportedFeature(
                "Only SELECT queries are supported".into(),
            )),
        }
    }

    fn execute_select(&self, query: &Query, table: &Table) -> Result<RecordBatch, QueryError> {
        let select = match &*query.body {
            SetExpr::Select(select) => select,
            _ => return Err(QueryError::UnsupportedFeature("Only SELECT supported".into())),
        };

        // Extract time predicates for partition pruning
        let time_predicates = self.extract_time_range(select)?;
        let partitions = table.relevant_partitions(time_predicates.0, time_predicates.1);

        let row_filter = if let Some(where_clause) = &select.selection {
            self.build_row_filter(where_clause)?
        } else {
            None
        };

        let group_by_expr = extract_group_by_exprs(&select.group_by);
        if let Some(exprs) = group_by_expr {
            if exprs.len() > 0 {
                return self.process_group_by(select, table, partitions, &row_filter, exprs)
            }
        } 
        
        self.execute_simple_select(select, table, partitions, row_filter)
    }

    fn process_group_by(
        &self,
        select: &Select,
        table: &Table,
        partitions: Vec<String>,
        row_filter: &Option<Box<dyn Fn(&RecordBatch) -> BooleanArray>>,
        group_by_exprs: &[Expr],
    ) -> Result<RecordBatch, QueryError> {
        // Parse GROUP BY expressions
        let aggregates = extract_aggregates(select)?; 
        let group_by = self.parse_group_by_exprs(group_by_exprs)?;

        // Execute the scan with partition pruning and row filtering
        let batches = table.scan_partitions(partitions, row_filter, None);
        let combined = concat_batches(&batches)?;

        // Group the data
        let grouped: HashMap<i64, Vec<RecordBatch>> = match group_by {
            SupportedGroupByExpr::Column(_col_name) => return Err(QueryError::UnsupportedFeature("generic group by not supported".to_string())),
            SupportedGroupByExpr::TimeBucket(interval_ns) => self.group_by_time(&combined, interval_ns)?,
        };

        // Compute aggregates for each group
        let results = self.compute_aggregates(&grouped, &aggregates)?;

        Ok(results)
    }

    fn parse_group_by_exprs(&self, exprs: &[Expr]) -> Result<SupportedGroupByExpr, QueryError> {
        if exprs.len() != 1 {
            return Err(QueryError::UnsupportedFeature(
                "Exactly one GROUP BY expression required".into(),
            ));
        }

        match &exprs[0] {
            Expr::Identifier(ident) => Ok(SupportedGroupByExpr::Column(ident.value.clone())),
            Expr::Function(func) if func.name.to_string().eq_ignore_ascii_case("bucket") => {
                let interval = self.parse_time_interval(&func.args)?;
                Ok(SupportedGroupByExpr::TimeBucket(interval))
            }
            _ => Err(QueryError::UnsupportedFeature(
                "Unsupported GROUP BY expression".into(),
            )),
        }
    }

    fn parse_time_interval(&self, args: &FunctionArguments) -> Result<i64, QueryError> {
        // Extract the string literal from args
        let interval_str = match args {
            FunctionArguments::List(list) => {
                if let Some(FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
                    sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::SingleQuotedString(s), .. }),
                ))) = list.args.get(1)
                {
                    s
                } else {
                    return Err(QueryError::InvalidArgument("Expected time interval string".into()));
                }
            }
            _ => return Err(QueryError::InvalidArgument("Invalid function arguments".into())),
        };

        // Parse the interval string, e.g. "1 HOUR 15 MINUTES"
        let mut total_ns: i64 = 0;
        let tokens: Vec<&str> = interval_str.split_whitespace().collect();

        // Expect pairs of (number, unit)
        if tokens.len() % 2 != 0 {
            return Err(QueryError::InvalidArgument("Invalid time interval format".into()));
        }

        for chunk in tokens.chunks(2) {
            let number: i64 = chunk[0].parse().map_err(|e: ParseIntError| {
                QueryError::InvalidArgument(format!("Invalid number in time interval: {}", e))
            })?;
            let unit = chunk[1].to_uppercase();

            let nanos = match unit.as_str() {
                "NANOSECOND" | "NANOSECONDS" => number,
                "MICROSECOND" | "MICROSECONDS" => number * 1_000,
                "MILLISECOND" | "MILLISECONDS" => number * 1_000_000,
                "SECOND" | "SECONDS" => number * 1_000_000_000,
                "MINUTE" | "MINUTES" => number * 60 * 1_000_000_000,
                "HOUR" | "HOURS" => number * 60 * 60 * 1_000_000_000,
                "DAY" | "DAYS" => number * 24 * 60 * 60 * 1_000_000_000,
                "MONTH" | "MONTHS" => number * 30 * 24 * 60 * 60 * 1_000_000_000, // Approximate month = 30 days
                "YEAR" | "YEARS" => number * 365 * 24 * 60 * 60 * 1_000_000_000, // Approximate year = 365 days
                _ => {
                    return Err(QueryError::InvalidArgument(format!(
                        "Unsupported time unit: {}",
                        unit
                    )))
                }
            };
            total_ns = total_ns.checked_add(nanos).ok_or_else(|| {
                QueryError::InvalidArgument("Time interval overflowed i64 nanoseconds".into())
            })?;
        }

        Ok(total_ns)
    }


    fn group_by_time(
        &self,
        batch: &RecordBatch,
        interval_ns: i64,
    ) -> Result<HashMap<i64, Vec<RecordBatch>>, QueryError> {
        let ts_col = batch.column(0);
            
        let ts_array = ts_col
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| QueryError::TypeError("time column is not TimestampNanosecondArray".to_string()))?;

        let mut groups: HashMap<i64, Vec<RecordBatch>> = HashMap::new();
        for i in 0..batch.num_rows() {
            let ts = ts_array.value(i);
            let bucket = (ts / interval_ns) * interval_ns;
            let row = batch.slice(i, 1);
            groups.entry(bucket).or_insert_with(Vec::new).push(row);
        }

        Ok(groups)
    }


    fn compute_aggregates(
        &self,
        groups: &HashMap<i64, Vec<RecordBatch>>,
        aggregates: &Vec<AggregateFunction>,
    ) -> Result<RecordBatch, QueryError> {
        // Prepare schema and arrays for results
        let mut fields = vec![Field::new("group_by_key", DataType::Utf8, false)];
        let mut group_values = Vec::new();
        let mut result_arrays: Vec<Vec<Option<f64>>> = vec![Vec::new(); aggregates.len()];

        for (group, batches) in groups {
            group_values.push(group.to_string());
            
            let combined = concat_batches(&batches)?;
            let schema = combined.schema();
            
            for (i, agg) in aggregates.iter().enumerate() {
                let value = match agg {
                    AggregateFunction::Sum(col) => {
                        let array_ref = combined.column_by_name(col).unwrap();
                        let data_type = schema.field_with_name(col).unwrap().data_type();
                        let prim_arr = downcast_arr(array_ref, data_type)?;
                        prim_arr.sum()
                    },
                    AggregateFunction::Avg(col) => {
                        let array_ref = combined.column_by_name(col).unwrap();
                        let data_type = schema.field_with_name(col).unwrap().data_type();
                        let prim_arr = downcast_arr(array_ref, data_type)?;
                        prim_arr.mean()
                    },
                    AggregateFunction::Count(col) => {
                        let array_ref = combined.column_by_name(col).unwrap();
                        Some(array_ref.len() as f64)
                    },
                    AggregateFunction::Min(col) => {
                        let array_ref = combined.column_by_name(col).unwrap();
                        let data_type = schema.field_with_name(col).unwrap().data_type();
                        let prim_arr = downcast_arr(array_ref, data_type)?;
                        prim_arr.min()
                    },
                    AggregateFunction::Max(col) => {
                        let array_ref = combined.column_by_name(col).unwrap();
                        let data_type = schema.field_with_name(col).unwrap().data_type();
                        let prim_arr = downcast_arr(array_ref, data_type)?;
                        prim_arr.max()
                    },
                    _ => return Err(QueryError::UnsupportedFeature(
                        format!("Unsupported operator: {:?}", agg)
                    )),
                };

                
                result_arrays[i].push(value);
            }
        }

        // Build result schema
        for agg in aggregates {
            fields.push(Field::new(
                match agg {
                    AggregateFunction::Sum(col) => format!("sum_{}", col),
                    AggregateFunction::Avg(col) => format!("avg_{}", col),
                    AggregateFunction::Count(col) => format!("count_{}", col),
                    AggregateFunction::Min(col) => format!("min_{}", col),
                    AggregateFunction::Max(col) => format!("max_{}", col),
                },
                match agg {
                    AggregateFunction::Sum(_) => DataType::Float64,
                    AggregateFunction::Avg(_) => DataType::Float64,
                    AggregateFunction::Count(_) => DataType::Int64,
                    AggregateFunction::Min(_) => DataType::Float64,
                    AggregateFunction::Max(_) => DataType::Float64,
                },
                false,
            ));
        }

        let batch = finalize_aggregate_results(group_values, result_arrays, aggregates)?;
        Ok(batch)
    }

    fn execute_simple_select(
        &self,
        select: &Select,
        table: &Table,
        partitions: Vec<String>,
        row_filter: Option<Box<dyn Fn(&RecordBatch) -> BooleanArray>>
    ) -> Result<RecordBatch, QueryError> {

        // Execute the scan with partition pruning and row filtering
        let batches = table.scan_partitions(
            partitions, 
            &row_filter, 
            extract_column_names(select)?.as_deref(),
        );
        
        // Combine all batches into one
        if batches.is_empty() {
            Ok(RecordBatch::new_empty(table.schema().clone()))
        } else {
            let combined = concat_batches(&batches)?;
            Ok(combined)
        }
    }

    fn extract_time_range(&self, select: &Select) -> Result<(Option<DateTime<Utc>>, Option<DateTime<Utc>>), QueryError> {
        let mut min_time = None;
        let mut max_time = None;
        
        if let Some(where_clause) = &select.selection {
            let mut predicates = Vec::new();
            self.extract_time_from_expr(where_clause, &mut predicates)?;
            
            for pred in predicates {
                match pred.op {
                    TimeOperator::GreaterThan | TimeOperator::GreaterThanOrEqual => {
                        min_time = min_time.map_or(Some(pred.value), |current: DateTime<Utc>| {
                            Some(current.max(pred.value))
                        });
                    }
                    TimeOperator::LessThan | TimeOperator::LessThanOrEqual => {
                        max_time = max_time.map_or(Some(pred.value), |current: DateTime<Utc>| {
                            Some(current.min(pred.value))
                        });
                    }
                    TimeOperator::Equal => {
                        min_time = Some(pred.value);
                        max_time = Some(pred.value);
                        break; // Exact match, no need to check other predicates
                    }
                }
            }
        }
        
        Ok((min_time, max_time))
    }

    fn extract_time_from_expr(&self, expr: &Expr, predicates: &mut Vec<TimePredicate>) -> Result<(), QueryError> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                if let Expr::Identifier(ident) = &**left {
                    if ident.value == "time" {
                        if let Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::Number(n, _), .. }) = &**right {
                            let ts = n.parse::<i64>()
                                .map_err(|_| QueryError::ParseError("Invalid timestamp".into()))?;
                            let dt = DateTime::from_timestamp_nanos(ts);
                            
                            let op = match op {
                                sqlparser::ast::BinaryOperator::Gt => TimeOperator::GreaterThan,
                                sqlparser::ast::BinaryOperator::Lt => TimeOperator::LessThan,
                                sqlparser::ast::BinaryOperator::GtEq => TimeOperator::GreaterThanOrEqual,
                                sqlparser::ast::BinaryOperator::LtEq => TimeOperator::LessThanOrEqual,
                                sqlparser::ast::BinaryOperator::Eq => TimeOperator::Equal,
                                _ => return Ok(()),
                            };
                            
                            predicates.push(TimePredicate {
                                column: "time".into(),
                                op,
                                value: dt,
                            });
                        }
                    }
                }
                self.extract_time_from_expr(left, predicates)?;
                self.extract_time_from_expr(right, predicates)?;
            }
            Expr::Nested(expr) => self.extract_time_from_expr(expr, predicates)?,
            _ => {}
        }
        Ok(())
    }

    fn build_row_filter(
        &self,
        expr: &Expr,
    ) -> Result<Option<Box<dyn Fn(&RecordBatch) -> arrow_array::BooleanArray>>, QueryError> {
        use sqlparser::ast::BinaryOperator::*;

        match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    And | Or => {
                        // Recursively build filters for left and right expressions
                        let left_filter = self.build_row_filter(left)?;
                        let right_filter = self.build_row_filter(right)?;

                        match (left_filter, right_filter) {
                            (Some(left_fn), Some(right_fn)) => {
                                // Combine filters with logical AND or OR
                                let combined_fn: Box<dyn Fn(&RecordBatch) -> arrow_array::BooleanArray> =
                                    match op {
                                        And => Box::new(move |batch: &RecordBatch| {
                                            let left_mask = left_fn(batch);
                                            let right_mask = right_fn(batch);
                                            arrow::compute::and(&left_mask, &right_mask).unwrap()
                                        }),
                                        Or => Box::new(move |batch: &RecordBatch| {
                                            let left_mask = left_fn(batch);
                                            let right_mask = right_fn(batch);
                                            arrow::compute::or(&left_mask, &right_mask).unwrap()
                                        }),
                                        _ => unreachable!(),
                                    };
                                Ok(Some(combined_fn))
                            }
                            _ => Ok(None),
                        }
                    }

                    _ => {
                        let left_col = if let Expr::Identifier(ident) = &**left {
                            ident.value.clone()
                        } else {
                            return Err(QueryError::UnsupportedFeature(
                                format!("Complex left expressions not supported, expr: {:?}", expr).into(),
                            ));
                        };

                        let value = if let Expr::Value(val) = &**right {
                            val.clone()
                        } else {
                            return Err(QueryError::UnsupportedFeature(
                                "Complex right expressions not supported".into(),
                            ));
                        };

                        Ok(Some(self.build_comparison_filter(left_col, op.clone(), value.into())?))
                    }
                }
            }
            Expr::Nested(expr) => self.build_row_filter(expr),
            _ => Err(QueryError::UnsupportedFeature(
                "Unsupported expression type".into(),
            )),
        }
    }

    fn build_comparison_filter(
        &self,
        column: String,
        op: BinaryOperator,
        value: sqlparser::ast::Value,
    ) -> Result<Box<dyn Fn(&RecordBatch) -> arrow_array::BooleanArray>, QueryError> {
        // First convert the SQL value to our internal scalar representation
        let scalar = self.sql_value_to_scalar(&column, value)?;
        
        // Then create the comparison function based on operator
        let comparator = match op {
            BinaryOperator::Gt => arrow::compute::kernels::cmp::gt,
            BinaryOperator::GtEq => arrow::compute::kernels::cmp::gt_eq,
            BinaryOperator::Lt => arrow::compute::kernels::cmp::lt,
            BinaryOperator::LtEq => arrow::compute::kernels::cmp::lt_eq,
            BinaryOperator::Eq => arrow::compute::kernels::cmp::eq,
            BinaryOperator::NotEq => arrow::compute::kernels::cmp::neq,
            _ => return Err(QueryError::UnsupportedFeature(
                format!("Unsupported operator: {}", op)
            )),
        };

        Ok(Box::new(move |batch| {
            let col = batch.column_by_name(&column)
                .expect("Column should exist from schema validation");
            comparator(col, &scalar.to_array(col.len())).unwrap()
        }))
    }

    fn sql_value_to_scalar(
        &self,
        column: &str,
        value: sqlparser::ast::Value,
    ) -> Result<ScalarValue, QueryError> {
        match (column, value) {
            // Handle timestamp columns specially
            ("time" | "timestamp", Value::Number(n, _)) => {
                let ts = n.parse::<i64>()
                    .map_err(|_| QueryError::TypeError("Invalid timestamp".into()))?;
                Ok(ScalarValue::TimestampNanosecond(ts))
            }
            // Numeric columns
            (_, Value::Number(n, _)) => {
                let num = n.parse::<f64>()
                    .map_err(|_| QueryError::TypeError("Invalid number".into()))?;
                Ok(ScalarValue::Float64(num))
            }
            // String columns
            (_, Value::SingleQuotedString(s)) => {
                Ok(ScalarValue::Utf8(s))
            }
            // Boolean columns
            (_, Value::Boolean(b)) => {
                Ok(ScalarValue::Boolean(b))
            }
            // Null values
            (_, Value::Null) => {
                Ok(ScalarValue::Null)
            }
            _ => Err(QueryError::UnsupportedFeature(
                format!("Unsupported value type for column {}", column)
            )),
        }
    }
}

fn extract_column_names(select: &Select) -> Result<Option<Vec<String>>, QueryError> {
    let mut columns = Vec::new();

    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                columns.push(ident.value.clone());
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                // You can choose to push alias or underlying expr name
                if let Expr::Identifier(ident) = expr {
                    columns.push(alias.value.clone()); // or ident.value.clone()
                } else {
                    // Handle complex expressions or skip
                }
            }
            SelectItem::Wildcard(_) => {
                if select.projection.len() > 1 {
                    return Err(QueryError::ParseError(
                        "Combination of named and wildcard in selection unsupported".into(),
                    ));
                }
                
                return Ok(None);
            }
            _ => {
                return Err(QueryError::UnsupportedFeature(
                    "Unsupported select item".into(),
                ));
            }
        }
    }

    Ok(Some(columns))
}

fn is_aggregate_function(name: &str) -> bool {
    matches!(name.to_uppercase().as_str(), "SUM" | "AVG" | "COUNT" | "MIN" | "MAX")
}

fn mean(array: &ArrayRef) -> Result<f64, QueryError> {
    let float_array = array
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or(QueryError::TypeError("Expected Float64Array".to_string()))?;
    
    let sum: f64 = float_array.iter().flatten().sum();
    let count = float_array.len() as f64;
    Ok(sum / count)
}

fn extract_group_by_exprs(group_by: &GroupByExpr) -> Option<&Vec<Expr>> {
    match group_by {
        GroupByExpr::Expressions(exprs, _modifiers) => Some(exprs),
        GroupByExpr::All(_) => None, // "ALL" means group by all non-aggregated select items
    }
}

fn concat_batches(batches: &Vec<RecordBatch>) -> Result<RecordBatch, QueryError> {
    let combined = arrow::compute::concat_batches(&batches[0].schema(), batches)
        .map_err(|e| QueryError::ExecutionError(e.to_string()))?;
    Ok(combined)
}

fn build_aggregate_array(
    values: &[Option<f64>],
    data_type: &DataType,
) -> Result<ArrayRef, QueryError> {
    match data_type {
        DataType::Float64 => {
            let mut builder = Float64Builder::new();
            for v in values {
                if let Some(val) = v {
                    builder.append_value(*val);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            // Convert f64 Option to i64 Option safely (for counts)
            let mut builder = Int64Builder::new();
            for v in values {
                if let Some(val) = v {
                    builder.append_value(*val as i64);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        dt => Err(QueryError::TypeError(format!("Unsupported data type for aggregate array: {:?}", dt))),
    }
}

fn finalize_aggregate_results(
    group_values: Vec<String>,
    result_arrays: Vec<Vec<Option<f64>>>,
    aggregates: &[AggregateFunction],
) -> Result<RecordBatch, QueryError> {
    // Build schema fields
    let mut fields = vec![Field::new("group", DataType::Utf8, false)];
    for agg in aggregates {
        let (name, data_type) = match agg {
            AggregateFunction::Sum(col) => (format!("sum_{}", col), DataType::Float64),
            AggregateFunction::Avg(col) => (format!("avg_{}", col), DataType::Float64),
            AggregateFunction::Count(col) => (format!("count_{}", col), DataType::Int64),
            AggregateFunction::Min(col) => (format!("min_{}", col), DataType::Float64),
            AggregateFunction::Max(col) => (format!("max_{}", col), DataType::Float64),
        };
        fields.push(Field::new(&name, data_type, true));
    }

    let schema = Arc::new(Schema::new(fields));

    // Build arrays starting with group keys
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(aggregates.len() + 1);
    arrays.push(Arc::new(StringArray::from(group_values)));

    // Convert each aggregate result vector into an Arrow array
    for (agg_idx, agg) in aggregates.iter().enumerate() {
        let data_type = match agg {
            AggregateFunction::Sum(_) | AggregateFunction::Avg(_) | AggregateFunction::Min(_) | AggregateFunction::Max(_) => DataType::Float64,
            AggregateFunction::Count(_) => DataType::Int64,
        };
        let arr = build_aggregate_array(&result_arrays[agg_idx], &data_type)?;
        arrays.push(arr);
    }

    RecordBatch::try_new(schema, arrays)
        .map_err(|e| QueryError::ExecutionError(e.to_string()))
}

fn extract_aggregates(select: &sqlparser::ast::Select) -> Result<Vec<AggregateFunction>, QueryError> {
    let mut aggregations = Vec::new();
    for item in &select.projection {
        if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item {
            if let Expr::Function(func) = expr {
                let func_name = func.name.to_string().to_uppercase();
            
                // Match supported aggregate functions
                let variant = match func_name.as_str() {
                    "AVG" => AggregateFunction::Avg as fn(String) -> AggregateFunction,
                    "SUM" => AggregateFunction::Sum as fn(String) -> AggregateFunction,
                    "COUNT" => AggregateFunction::Count as fn(String) -> AggregateFunction,
                    "MIN" => AggregateFunction::Min as fn(String) -> AggregateFunction,
                    "MAX" => AggregateFunction::Max as fn(String) -> AggregateFunction,
                    _ => {
                        return Err(QueryError::InvalidAggregateFunction(format!("unsupported aggregate function {}", func_name).to_string()))
                    },
                };

                if let FunctionArguments::List(list) = &func.args {
                    for arg in &list.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) = arg {
                            aggregations.push(variant(ident.value.clone()));
                        }
                    }       
                } 
            }
        }
    }
    Ok(aggregations)
}