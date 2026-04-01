use crate::infrastructure::polars::kind::PolarsKind;
use crate::infrastructure::polars::utils::metric_violation;
use crate::invariant::Invariant;
use crate::scope::Scope;
use crate::violation::Violation;
use polars::datatypes::AnyValue;
use polars::prelude::{Expr, col};

///
/// Builds the Polars expression computing the number of unique values
/// for the target column.
///
/// Scope:
/// - Requires `Scope::Column`
///
/// The returned expression evaluates `n_unique()` on the column.
///
/// The metric produced by this expression represents the number
/// of distinct values in the column.
///
/// The total row count is NOT computed here and must be provided
/// separately via the `row_count_cache` parameter for `map()`.
pub fn plan(inv: &Invariant<PolarsKind>) -> Option<Expr> {
    let Scope::Column { name } = inv.scope() else {
        return None;
    };
    Some(col(name).n_unique())
}

///
/// Converts the computed unique value count into a duplication violation.
///
/// Required parameters:
/// - `row_count_cache`: total number of rows in the dataset
///
/// Logic:
/// - Reads `unique_count` from the evaluated expression
/// - Computes `duplicate_count = total_rows - unique_count`
/// - Returns a violation if `duplicate_count > 0`
///
/// Produced metric:
/// - `duplicate_count` (integer)
pub fn map(inv: &Invariant<PolarsKind>, v: AnyValue) -> Option<Violation> {
    let unique = v.try_extract::<i64>().ok()?;
    let total: i64 = inv.require_param("row_count_cache").ok()?.parse().ok()?;
    metric_violation(
        inv,
        "duplicate_count",
        total - unique,
        "duplicates detected".to_string(),
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
        params.insert("row_count_cache".to_string(), row_count.to_string());

        Invariant::new(
            InvariantId::new("unique_test").unwrap(),
            PolarsKind::Unique,
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
        let inv = make_invariant(4);
        assert!(plan(&inv).is_some());
    }

    #[test]
    fn test_no_violation_when_all_unique() {
        let df = df(vec![1, 2, 3, 4]);
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

        let violation = map(&inv, AnyValue::Int64(unique));
        assert!(violation.is_none());
    }

    #[test]
    fn test_violation_when_duplicates_exist() {
        let df = df(vec![1, 1, 2, 3]);
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

        let violation = map(&inv, AnyValue::Int64(unique));
        assert!(violation.is_some());
    }

    #[test]
    fn test_wrong_scope_returns_none() {
        let mut params = BTreeMap::new();
        params.insert("row_count_cache".to_string(), "4".to_string());

        let inv = Invariant::new(
            InvariantId::new("wrong_scope").unwrap(),
            PolarsKind::Unique,
            Scope::Dataset,
        )
        .with_params(params);

        assert!(plan(&inv).is_none());
    }
}
