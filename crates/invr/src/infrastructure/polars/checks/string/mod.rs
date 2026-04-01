use crate::invariant::Invariant;
use crate::prelude::PolarsKind;
use crate::violation::Violation;
use crate::violation::value_object::metric_value::MetricValue;
use polars::datatypes::AnyValue;

pub(crate) mod regex_match;
pub(crate) mod string_length_between;
pub(crate) mod string_length_max;
pub(crate) mod string_length_min;

pub fn map(inv: &Invariant<PolarsKind>, value: AnyValue) -> Option<Violation> {
    let violation_count = value.try_extract::<i64>().ok()?;

    if violation_count > 0 {
        Some(
            Violation::new(
                inv.id().clone(),
                inv.severity(),
                inv.scope().clone(),
                format!("{violation_count} strings shorter than min length"),
            )
            .with_metric("violation_count", MetricValue::Int(violation_count)),
        )
    } else {
        None
    }
}
