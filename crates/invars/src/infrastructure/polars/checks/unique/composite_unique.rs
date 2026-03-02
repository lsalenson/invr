use crate::infrastructure::polars::kind::PolarsKind;
use crate::infrastructure::polars::utils::metric_violation;
use crate::invariant::Invariant;
use crate::violation::Violation;
use polars::datatypes::AnyValue;
use polars::prelude::{Expr, as_struct, col};


/// Builds the Polars expression computing the number of unique
/// composite rows across multiple columns.
///
/// Required parameters:
/// - `columns`: comma-separated list of column names (e.g. "a,b,c")
///
/// Behavior:
/// - Creates a struct expression from the provided columns
/// - Computes `n_unique()` on the resulting struct
///
/// The resulting metric represents the count of unique composite keys.
pub fn plan(inv: &Invariant<PolarsKind>) -> Option<Expr> {
    let cols = inv.require_param("columns").ok()?;
    let cols: Vec<_> = cols.split(',').collect();

    Some(as_struct(cols.iter().map(|c| col(*c)).collect::<Vec<_>>()).n_unique())
}

/// Converts the computed unique composite count into a `Violation`.
///
/// Required parameters:
/// - `row_count_cache`: total number of rows in the dataset
///
/// Logic:
/// - Reads the number of unique composite rows
/// - Computes `duplicate_count = total_rows - unique_rows`
/// - Returns a violation if `duplicate_count > 0`
///
/// Metric name: `duplicate_count`
pub fn map(inv: &Invariant<PolarsKind>, v: AnyValue) -> Option<Violation> {
    let unique = v.try_extract::<i64>().ok()?;
    let total: i64 = inv.require_param("row_count_cache").ok()?.parse().ok()?;
    metric_violation::<PolarsKind>(
        inv,
        "duplicate_count",
        total - unique,
        format!("duplicates detected"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::polars::kind::PolarsKind;
    use crate::invariant::Invariant;
    use crate::prelude::InvariantId;
    use crate::scope::Scope;
    use polars::df;
    use polars::prelude::{DataFrame, IntoLazy};
    use std::collections::BTreeMap;

    fn make_invariant(row_count: i64) -> Invariant<PolarsKind> {
        let mut params = BTreeMap::new();
        params.insert("columns".to_string(), "a,b".to_string());
        params.insert("row_count_cache".to_string(), row_count.to_string());

        Invariant::new(
            InvariantId::new("composite_unique_test").unwrap(),
            PolarsKind::CompositeUnique,
            Scope::Dataset,
        )
        .with_params(params)
    }

    fn df() -> DataFrame {
        df![
            "a" => &[1, 1, 2, 2],
            "b" => &[10, 10, 20, 30]
        ]
        .unwrap()
    }

    #[test]
    fn test_plan_returns_expr() {
        let inv = make_invariant(4);
        assert!(plan(&inv).is_some());
    }

    #[test]
    fn test_map_no_violation_when_all_unique() {
        let mut params = BTreeMap::new();
        params.insert("columns".to_string(), "a,b".to_string());
        params.insert("row_count_cache".to_string(), "4".to_string());

        let inv = Invariant::new(
            InvariantId::new("composite_unique_test").unwrap(),
            PolarsKind::CompositeUnique,
            Scope::Dataset,
        )
        .with_params(params);

        let v = AnyValue::Int64(4);
        let result = map(&inv, v);
        assert!(result.is_none());
    }

    #[test]
    fn test_map_violation_when_duplicates_exist() {
        let inv = make_invariant(4);

        let v = AnyValue::Int64(3);
        let result = map(&inv, v);

        assert!(result.is_some());
    }

    #[test]
    fn test_integration_duplicate_count() {
        let df = df();
        let inv = make_invariant(4);

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

        assert_eq!(unique, 3);

        let violation = map(&inv, AnyValue::Int64(unique));
        assert!(violation.is_some());
    }
}
