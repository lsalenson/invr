use crate::infrastructure::polars::kind::PolarsKind;
use crate::invariant::Invariant;
use crate::violation::Violation;
use crate::violation::value_object::metric_value::MetricValue;

pub fn metric_violation(
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
