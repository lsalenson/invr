pub(crate) mod mean_between;
pub(crate) mod stddev_max;
pub(crate) mod sum_between;
pub(crate) mod value_between;
pub(crate) mod value_max;
pub(crate) mod value_min;

use crate::invariant::Invariant;
use crate::prelude::PolarsKind;
use crate::violation::Violation;
use polars::datatypes::AnyValue;

pub fn map_count_violation(
    inv: &Invariant<PolarsKind>,
    value: AnyValue,
    label: &str,
) -> Option<Violation> {
    let count = value.try_extract::<i64>().ok()?;

    if count > 0 {
        Some(Violation::new(
            inv.id().clone(),
            inv.severity(),
            inv.scope().clone(),
            format!("{count} violations of {label}"),
        ))
    } else {
        None
    }
}
