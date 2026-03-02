use crate::infrastructure::polars::kind::PolarsKind;
use crate::invariant::Invariant;
use crate::scope::Scope;
use crate::violation::Violation;
use crate::violation::value_object::metric_value::MetricValue;
use polars::prelude::AnyValue;
use polars::prelude::*;


/// Builds the Polars expression counting rows whose values are NOT
/// contained in an allowed set (foreign key–like constraint).
///
/// Required parameters:
/// - `allowed_values`: comma-separated list of allowed string values
///   (e.g. "A,B,C")
///
/// Scope:
/// - Requires `Scope::Column`
///
/// Behavior:
/// - Splits `allowed_values` into a list
/// - Builds a temporary Series representing the allowed domain
/// - Uses `is_in()` to check membership
/// - Marks rows NOT present in the allowed set
/// - Returns the total count of invalid rows
///
/// The resulting metric represents the number of values
/// violating the foreign key constraint.
pub fn plan(inv: &Invariant<PolarsKind>) -> Option<Expr> {
    let Scope::Column { name } = inv.scope() else {
        return None;
    };
    let values = inv.require_param("allowed_values").ok()?;

    let allowed: Vec<&str> = values.split(',').collect();
    let series = Series::new(PlSmallStr::from(""), allowed);

    Some(col(name).is_in(lit(series).implode(), false).not().sum())
}

/// Converts the computed invalid count into a `Violation`.
///
/// Logic:
/// - Reads `invalid_count` from the evaluated expression
/// - Returns a violation if `invalid_count > 0`
///
/// Produced metric:
/// - `invalid_count` (integer)
pub fn map(inv: &Invariant<PolarsKind>, value: AnyValue) -> Option<Violation> {
    let invalid_count = value.try_extract::<i64>().ok()?;

    if invalid_count > 0 {
        Some(
            Violation::new(
                inv.id().clone(),
                inv.severity(),
                inv.scope().clone(),
                format!("{invalid_count} rows violate foreign key constraint"),
            )
            .with_metric("invalid_count", MetricValue::Int(invalid_count)),
        )
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::invariant::value_object::id::InvariantId;
    use std::collections::BTreeMap;

    fn make_invariant(column: &str, allowed: &str) -> Invariant<PolarsKind> {
        let id = InvariantId::new("foreign_key_test").unwrap();
        let mut params = BTreeMap::new();
        params.insert("allowed_values".to_string(), allowed.to_string());

        Invariant::new(
            id,
            PolarsKind::ForeignKey,
            Scope::Column {
                name: column.to_string(),
            },
        )
        .with_params(params)
    }

    fn df(values: Vec<&str>) -> DataFrame {
        let series = Series::new("a".into(), values);
        let height = series.len();
        DataFrame::new(height, vec![series.into()]).unwrap()
    }

    #[test]
    fn test_plan_returns_expr() {
        let inv = make_invariant("a", "X,Y,Z");
        assert!(plan(&inv).is_some());
    }

    #[test]
    fn test_foreign_key_no_violation() {
        let df = df(vec!["X", "Y", "Z"]);
        let inv = make_invariant("a", "X,Y,Z");

        let result = df.lazy().select([plan(&inv).unwrap()]).collect().unwrap();

        let count = result.columns()[0]
            .get(0)
            .unwrap()
            .try_extract::<i64>()
            .unwrap();

        assert_eq!(count, 0);
    }

    #[test]
    fn test_foreign_key_violation() {
        let df = df(vec!["X", "INVALID", "Y"]);
        let inv = make_invariant("a", "X,Y,Z");

        let result = df.lazy().select([plan(&inv).unwrap()]).collect().unwrap();

        let count = result.columns()[0]
            .get(0)
            .unwrap()
            .try_extract::<i64>()
            .unwrap();

        assert_eq!(count, 1);
    }

    #[test]
    fn test_wrong_scope_returns_none() {
        let id = InvariantId::new("wrong_scope").unwrap();
        let inv = Invariant::new(id, PolarsKind::ForeignKey, Scope::Dataset);

        assert!(plan(&inv).is_none());
    }
}
