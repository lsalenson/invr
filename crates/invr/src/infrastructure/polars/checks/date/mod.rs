use crate::invariant::Invariant;
use crate::prelude::PolarsKind;
use crate::violation::Violation;
use crate::violation::value_object::metric_value::MetricValue;
use polars::datatypes::AnyValue;

pub(crate) mod date_between;
pub(crate) mod monotonic_increasing;
pub(crate) mod no_future_dates;
pub(crate) mod no_gaps_in_sequence;

pub fn map(inv: &Invariant<PolarsKind>, value: AnyValue) -> Option<Violation> {
    let count = value.try_extract::<i64>().ok()?;

    if count > 0 {
        Some(
            Violation::new(
                inv.id().clone(),
                inv.severity(),
                inv.scope().clone(),
                format!("{count} dates outside allowed range"),
            )
            .with_metric("violation_count", MetricValue::Int(count)),
        )
    } else {
        None
    }
}
