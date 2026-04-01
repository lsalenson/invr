use crate::infrastructure::polars::kind::PolarsKind;
use crate::invariant::Invariant;
use crate::scope::Scope;
use polars::prelude::*;

/// Builds the Polars expression counting rows where numeric values
/// are strictly below a specified minimum threshold.
///
/// Required parameters:
/// - `min`: minimum allowed value (inclusive)
///
/// Scope:
/// - Requires `Scope::Column`
///
/// Behavior:
/// - Casts the column to `Float64`
/// - Marks rows where `value < min`
/// - Returns the total count of rows below the minimum
///
/// The resulting metric represents the number of values
/// violating the lower bound constraint.
///
/// Note:
/// - The minimum bound is inclusive. Values where `value == min`
///   are considered valid.
pub fn plan(inv: &Invariant<PolarsKind>) -> Option<Expr> {
    let Scope::Column { name } = inv.scope() else {
        return None;
    };
    let min: f64 = inv.require_param("min").ok()?.parse().ok()?;

    Some(col(name).cast(DataType::Float64).lt(lit(min)).sum())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::invariant::value_object::id::InvariantId;
    use std::collections::BTreeMap;

    fn make_invariant(column: &str, min: f64) -> Invariant<PolarsKind> {
        let id = InvariantId::new("value_min_test").unwrap();
        let mut params = BTreeMap::new();
        params.insert("min".to_string(), min.to_string());

        Invariant::new(
            id,
            PolarsKind::ValueMin,
            Scope::Column {
                name: column.to_string(),
            },
        )
        .with_params(params)
    }

    fn df(values: Vec<i32>) -> DataFrame {
        let series = Series::new("a".into(), values);
        DataFrame::new(series.len(), vec![series.into()]).unwrap()
    }

    #[test]
    fn test_plan_returns_expr() {
        let inv = make_invariant("a", 0.0);
        assert!(plan(&inv).is_some());
    }

    #[test]
    fn test_value_min_no_violation() {
        let df = df(vec![10, 20, 30]);
        let inv = make_invariant("a", 0.0);

        let result = df.lazy().select([plan(&inv).unwrap()]).collect().unwrap();

        let count = result.columns()[0]
            .get(0)
            .unwrap()
            .try_extract::<i64>()
            .unwrap();

        assert_eq!(count, 0);
    }

    #[test]
    fn test_value_min_violation() {
        let df = df(vec![-10, 5, 6]);
        let inv = make_invariant("a", 0.0);

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
        let inv = Invariant::new(id, PolarsKind::ValueMin, Scope::Dataset);

        assert!(plan(&inv).is_none());
    }
}
