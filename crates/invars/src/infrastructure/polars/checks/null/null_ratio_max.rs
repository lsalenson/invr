use crate::infrastructure::polars::kind::PolarsKind;
use crate::invariant::Invariant;
use crate::scope::Scope;
use crate::violation::Violation;
use polars::datatypes::AnyValue;
use polars::prelude::{Expr, col};

///
/// Builds the Polars expression counting NULL values in the target column
/// in order to compute a NULL ratio.
///
/// Scope:
/// - Requires `Scope::Column`
///
/// Behavior:
/// - Uses `is_null()` on the column
/// - Aggregates with `sum()` to compute the total number of NULL rows
///
/// The resulting metric represents the raw NULL count.
/// The ratio computation is performed later in `map()` using the
/// `row_count_cache` parameter.
pub fn plan(inv: &Invariant<PolarsKind>) -> Option<Expr> {
    let Scope::Column { name } = inv.scope() else {
        return None;
    };
    Some(col(name).is_null().sum())
}

///
/// Converts the computed NULL count into a ratio-based violation.
///
/// Required parameters:
/// - `row_count_cache`: total number of rows in the dataset
/// - `max_ratio`: maximum allowed NULL ratio (float)
///
/// Logic:
/// - Computes `ratio = null_count / total_rows`
/// - Returns a violation if `ratio > max_ratio`
///
/// Produced metric (implicit in message):
/// - null_ratio (float)
///
/// Note:
/// - If `total_rows == 0`, behavior depends on the caller and should be
///   guarded upstream to avoid division by zero.
pub fn map(inv: &Invariant<PolarsKind>, v: AnyValue) -> Option<Violation> {
    let nulls = v.try_extract::<i64>().ok()?;
    let total: i64 = inv.require_param("row_count_cache").ok()?.parse().ok()?; // or inject

    let ratio = nulls as f64 / total as f64;
    let max: f64 = inv.require_param("max_ratio").ok()?.parse().ok()?;

    if ratio > max {
        Some(Violation::new(
            inv.id().clone(),
            inv.severity(),
            inv.scope().clone(),
            format!("null ratio {ratio} > {max}"),
        ))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::invariant::value_object::id::InvariantId;
    use crate::infrastructure::polars::kind::PolarsKind;
    use polars::prelude::*;
    use std::collections::BTreeMap;

    fn make_invariant(column: &str, row_count: i64, max_ratio: f64) -> Invariant<PolarsKind> {
        let id = InvariantId::new("null_ratio_max_test").unwrap();
        let mut params = BTreeMap::new();
        params.insert("row_count_cache".to_string(), row_count.to_string());
        params.insert("max_ratio".to_string(), max_ratio.to_string());

        Invariant::new(
            id,
            PolarsKind::NullRatioMax,
            Scope::Column {
                name: column.to_string(),
            },
        )
        .with_params(params)
    }

    fn df_with_optional(values: Vec<Option<i32>>) -> DataFrame {
        let series = Series::new(PlSmallStr::from("a"), values);
        DataFrame::new_infer_height(vec![series.into()]).unwrap()
    }

    #[test]
    fn test_plan_returns_expr() {
        let inv = make_invariant("a", 3, 0.5);
        let expr = plan(&inv);
        assert!(expr.is_some());
    }

    #[test]
    fn test_null_ratio_no_violation() {
        let df = df_with_optional(vec![Some(1), None, Some(3)]); // 1/3 = 0.33
        let inv = make_invariant("a", 3, 0.5);

        let result = df.lazy().select([plan(&inv).unwrap()]).collect().unwrap();

        let value = result.columns()[0].get(0).unwrap();
        let violation = map(&inv, value);

        assert!(violation.is_none());
    }

    #[test]
    fn test_null_ratio_violation() {
        let df = df_with_optional(vec![None, None, Some(3)]); // 2/3 = 0.66
        let inv = make_invariant("a", 3, 0.5);

        let result = df.lazy().select([plan(&inv).unwrap()]).collect().unwrap();

        let value = result.columns()[0].get(0).unwrap();
        let violation = map(&inv, value);

        assert!(violation.is_some());
    }

    #[test]
    fn test_wrong_scope_returns_none() {
        let id = InvariantId::new("wrong_scope").unwrap();
        let mut params = BTreeMap::new();
        params.insert("row_count_cache".to_string(), "3".to_string());
        params.insert("max_ratio".to_string(), "0.5".to_string());

        let inv = Invariant::new(id, PolarsKind::NullRatioMax, Scope::Dataset).with_params(params);

        let expr = plan(&inv);
        assert!(expr.is_none());
    }
}
