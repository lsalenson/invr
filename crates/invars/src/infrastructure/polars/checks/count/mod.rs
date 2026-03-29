use polars::prelude::*;

use crate::infrastructure::polars::kind::PolarsKind;
use crate::infrastructure::polars::utils::metric_violation;
use crate::invariant::Invariant;
use crate::violation::Violation;
/// Builds the Polars expression computing the total number of rows
/// in the dataset.
///
/// Scope:
/// - Dataset-level only
///
/// Behavior:
/// - Uses `len()` to compute the row count
/// - Casts the result to `Int64`
/// - Returns a single scalar representing the dataset size
///
/// The resulting metric represents the raw `row_count` value.
pub fn plan_row_count() -> Expr {
    len().cast(DataType::Int64)
}
/// Converts the computed row count into a row-count-based violation.
///
/// Supported invariant kinds:
/// - `RowCountMin`
/// - `RowCountMax`
/// - `RowCountBetween`
///
/// Required parameters depend on the invariant kind:
/// - `min` for `RowCountMin`
/// - `max` for `RowCountMax`
/// - `min` and `max` for `RowCountBetween`
///
/// Logic:
/// - Extracts the computed `row_count`
/// - Applies the bound validation depending on `PolarsKind`
/// - Returns a violation if the constraint is not satisfied
///
/// Produced metric:
/// - `row_count` (integer)
pub fn map_row_count(inv: &Invariant<PolarsKind>, v: AnyValue) -> Option<Violation> {
    let count = v.try_extract::<i64>().ok()?;

    match inv.kind() {
        PolarsKind::RowCountMin => {
            let min: i64 = inv.require_param("min").ok()?.parse().ok()?;
            metric_violation(
                inv,
                "row_count",
                if count < min { count } else { 0 },
                format!("row_count {count} < {min}"),
            )
        }
        PolarsKind::RowCountMax => {
            let max: i64 = inv.require_param("max").ok()?.parse().ok()?;
            metric_violation(
                inv,
                "row_count",
                if count > max { count } else { 0 },
                format!("row_count {count} > {max}"),
            )
        }
        PolarsKind::RowCountBetween => {
            let min: i64 = inv.require_param("min").ok()?.parse().ok()?;
            let max: i64 = inv.require_param("max").ok()?.parse().ok()?;
            if count < min || count > max {
                Some(
                    Violation::new(
                        inv.id().clone(),
                        inv.severity(),
                        inv.scope().clone(),
                        format!("row_count {count} not in [{min}, {max}]"),
                    )
                    .with_metric(
                        "row_count",
                        crate::violation::value_object::metric_value::MetricValue::Int(count),
                    ),
                )
            } else {
                None
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::polars::kind::PolarsKind;
    use crate::invariant::Invariant;
    use crate::prelude::Scope;

    fn make_invariant(kind: PolarsKind, params: &[(&str, &str)]) -> Invariant<PolarsKind> {
        let mut inv = Invariant::new("test_id".to_string().parse().unwrap(), kind, Scope::Dataset);
        for (k, v) in params {
            inv = inv.with_param_value(k.to_string(), v.to_string());
        }
        inv
    }

    // -------------------------------------------------------------
    // plan_row_count
    // -------------------------------------------------------------

    #[test]
    fn test_plan_row_count_evaluates_correct_length() {
        let df = df! {
            "a" => &[1, 2, 3, 4, 5]
        }
        .unwrap();

        let result = df.lazy().select([plan_row_count()]).collect().unwrap();

        let value = result.columns()[0].get(0).unwrap();
        let count = value.try_extract::<i64>().unwrap();

        assert_eq!(count, 5);
    }

    // -------------------------------------------------------------
    // RowCountMin
    // -------------------------------------------------------------

    #[test]
    fn test_row_count_min_violation() {
        let inv = make_invariant(PolarsKind::RowCountMin, &[("min", "10")]);

        let v = AnyValue::Int64(5);

        let violation = map_row_count(&inv, v);

        assert!(violation.is_some());
    }

    #[test]
    fn test_row_count_min_no_violation() {
        let inv = make_invariant(PolarsKind::RowCountMin, &[("min", "10")]);

        let v = AnyValue::Int64(15);

        let violation = map_row_count(&inv, v);

        assert!(violation.is_none());
    }

    // -------------------------------------------------------------
    // RowCountMax
    // -------------------------------------------------------------

    #[test]
    fn test_row_count_max_violation() {
        let inv = make_invariant(PolarsKind::RowCountMax, &[("max", "10")]);

        let v = AnyValue::Int64(15);

        let violation = map_row_count(&inv, v);

        assert!(violation.is_some());
    }

    #[test]
    fn test_row_count_max_no_violation() {
        let inv = make_invariant(PolarsKind::RowCountMax, &[("max", "10")]);

        let v = AnyValue::Int64(5);

        let violation = map_row_count(&inv, v);

        assert!(violation.is_none());
    }

    // -------------------------------------------------------------
    // RowCountBetween
    // -------------------------------------------------------------

    #[test]
    fn test_row_count_between_violation_low() {
        let inv = make_invariant(PolarsKind::RowCountBetween, &[("min", "10"), ("max", "20")]);

        let v = AnyValue::Int64(5);

        let violation = map_row_count(&inv, v);

        assert!(violation.is_some());
    }

    #[test]
    fn test_row_count_between_violation_high() {
        let inv = make_invariant(PolarsKind::RowCountBetween, &[("min", "10"), ("max", "20")]);

        let v = AnyValue::Int64(25);

        let violation = map_row_count(&inv, v);

        assert!(violation.is_some());
    }

    #[test]
    fn test_row_count_between_no_violation() {
        let inv = make_invariant(PolarsKind::RowCountBetween, &[("min", "10"), ("max", "20")]);

        let v = AnyValue::Int64(15);

        let violation = map_row_count(&inv, v);

        assert!(violation.is_none());
    }
}
