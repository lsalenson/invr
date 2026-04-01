use crate::infrastructure::polars::kind::PolarsKind;
use crate::invariant::Invariant;
use crate::scope::Scope;
use crate::violation::Violation;
use crate::violation::value_object::metric_value::MetricValue;
use polars::prelude::AnyValue;
use polars::prelude::*;

///
/// Builds the Polars expression computing the number of unique values
/// for the target column.
///
/// Scope:
/// - Requires `Scope::Column`
///
/// The resulting metric represents the count of unique rows for
/// the specified column. The total row count is not computed here
/// and must be provided via parameters during `map()`.
pub fn plan(inv: &Invariant<PolarsKind>) -> Option<Expr> {
    let Scope::Column { name } = inv.scope() else {
        return None;
    };

    Some(col(name).n_unique())
}

///
/// Converts the computed unique count into a duplicate ratio violation.
///
/// Required parameters:
/// - `max_ratio`: maximum allowed duplicate ratio (float)
/// - `row_count_cache`: total number of rows (integer)
///
/// Logic:
/// - Computes `duplicate_count = total_rows - unique_count`
/// - Computes `ratio = duplicate_count / total_rows`
/// - Returns a violation if `ratio > max_ratio`
///
/// Produced metrics:
/// - `duplicate_ratio` (float)
/// - `duplicate_count` (int)
pub fn map(inv: &Invariant<PolarsKind>, value: AnyValue) -> Option<Violation> {
    let unique_count = value.try_extract::<i64>().ok()?;

    let max_ratio: f64 = inv.require_param("max_ratio").ok()?.parse().ok()?;

    let total_rows: i64 = inv.require_param("row_count_cache").ok()?.parse().ok()?;

    if total_rows == 0 {
        return None;
    }

    let duplicate_count = total_rows - unique_count;
    let ratio = duplicate_count as f64 / total_rows as f64;

    if ratio > max_ratio {
        Some(
            Violation::new(
                inv.id().clone(),
                inv.severity(),
                inv.scope().clone(),
                format!("duplicate ratio {ratio:.4} > {max_ratio:.4}"),
            )
            .with_metric("duplicate_ratio", MetricValue::Float(ratio))
            .with_metric("duplicate_count", MetricValue::Int(duplicate_count)),
        )
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::invariant::Invariant;
    use crate::prelude::InvariantId;
    use crate::scope::Scope;
    use polars::df;
    use polars::prelude::{DataFrame, IntoLazy};
    use std::collections::BTreeMap;

    fn make_invariant(max_ratio: f64, row_count: i64) -> Invariant<PolarsKind> {
        let mut params = BTreeMap::new();
        params.insert("max_ratio".to_string(), max_ratio.to_string());
        params.insert("row_count_cache".to_string(), row_count.to_string());

        Invariant::new(
            InvariantId::new("duplicate_ratio_max_test").unwrap(),
            PolarsKind::DuplicateRatioMax,
            Scope::Column {
                name: "a".to_string(),
            },
        )
        .with_params(params)
    }

    fn df(values: Vec<i32>) -> DataFrame {
        df![
            "a" => values
        ]
        .unwrap()
    }

    #[test]
    fn test_plan_returns_expr() {
        let inv = make_invariant(0.5, 4);
        assert!(plan(&inv).is_some());
    }

    #[test]
    fn test_no_violation_when_ratio_below_threshold() {
        // 4 rows, 3 unique -> 1 duplicate -> ratio = 0.25
        let df = df(vec![1, 2, 3, 3]);
        let inv = make_invariant(0.5, 4);

        let unique = df
            .lazy()
            .select([plan(&inv).unwrap()])
            .collect()
            .unwrap()
            .column("a")
            .unwrap()
            .get(0)
            .unwrap()
            .try_extract::<i64>()
            .unwrap();

        let violation = map(&inv, AnyValue::Int64(unique));
        assert!(violation.is_none());
    }

    #[test]
    fn test_violation_when_ratio_above_threshold() {
        // 4 rows, 2 unique -> 2 duplicates -> ratio = 0.5
        let df = df(vec![1, 1, 2, 2]);
        let inv = make_invariant(0.3, 4);

        let unique = df
            .lazy()
            .select([plan(&inv).unwrap()])
            .collect()
            .unwrap()
            .column("a")
            .unwrap()
            .get(0)
            .unwrap()
            .try_extract::<i64>()
            .unwrap();

        let violation = map(&inv, AnyValue::Int64(unique));
        assert!(violation.is_some());
    }

    #[test]
    fn test_wrong_scope_returns_none() {
        let mut params = BTreeMap::new();
        params.insert("max_ratio".to_string(), "0.5".to_string());
        params.insert("row_count_cache".to_string(), "4".to_string());

        let inv = Invariant::new(
            InvariantId::new("wrong_scope").unwrap(),
            PolarsKind::DuplicateRatioMax,
            Scope::Dataset,
        )
        .with_params(params);

        assert!(plan(&inv).is_none());
    }
}
