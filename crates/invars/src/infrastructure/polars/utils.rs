use crate::infrastructure::polars::kind::PolarsKind;
use crate::invariant::Invariant;
use crate::violation::Violation;
use crate::violation::value_object::metric_value::MetricValue;
use polars::datatypes::DataType;
use polars::prelude::{Expr, col};

pub fn metric_violation<K>(
    inv: &Invariant<PolarsKind>,
    metric_name: &str,
    value: i64,
    message: String,
) -> Option<Violation> {
    if value == 0 {
        return None;
    }

    Some(
        Violation::new(
            inv.id().clone(),
            inv.severity(),
            inv.scope().clone(),
            message,
        )
        .with_metric(metric_name, MetricValue::Int(value)),
    )
}

pub fn bool_sum_expr(column: &str) -> Expr {
    col(column).cast(DataType::Boolean).sum()
}
