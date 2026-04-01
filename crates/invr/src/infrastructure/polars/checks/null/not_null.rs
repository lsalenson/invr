use crate::infrastructure::polars::kind::PolarsKind;
use crate::infrastructure::polars::utils::metric_violation;
use crate::invariant::Invariant;
use crate::scope::Scope;
use crate::violation::Violation;
use polars::datatypes::AnyValue;
use polars::prelude::{Expr, col};

///
/// Builds the Polars expression counting NULL values in the target column.
///
/// Scope:
/// - Requires `Scope::Column`
///
/// Behavior:
/// - Uses `is_null()` on the column
/// - Sums the boolean mask to obtain the total number of NULL rows
///
/// The resulting metric represents the number of NULL values
/// present in the column.
pub fn plan(inv: &Invariant<PolarsKind>) -> Option<Expr> {
    let Scope::Column { name } = inv.scope() else {
        return None;
    };

    Some(col(name).is_null().sum())
}

///
/// Converts the computed NULL count into a `Violation`.
///
/// Logic:
/// - Reads `null_count` from the evaluated expression
/// - Returns a violation if `null_count > 0`
///
/// Produced metric:
/// - `null_count` (integer)
pub fn map(inv: &Invariant<PolarsKind>, v: AnyValue) -> Option<Violation> {
    let count = v.try_extract::<i64>().ok()?;
    metric_violation(inv, "null_count", count, format!("{count} nulls found"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::invariant::value_object::id::InvariantId;
    use crate::infrastructure::polars::kind::PolarsKind;
    use polars::prelude::*;

    fn make_invariant(column: &str) -> Invariant<PolarsKind> {
        let id = InvariantId::new("not_null_test").unwrap();
        Invariant::new(
            id,
            PolarsKind::NotNull,
            Scope::Column {
                name: column.to_string(),
            },
        )
    }

    fn df_with_optional(values: Vec<Option<i32>>) -> DataFrame {
        let series = Series::new(PlSmallStr::from("a"), values);
        DataFrame::new_infer_height(vec![series.into()]).unwrap()
    }

    #[test]
    fn test_plan_returns_expr() {
        let inv = make_invariant("a");
        let expr = plan(&inv);
        assert!(expr.is_some());
    }

    #[test]
    fn test_not_null_no_violation() {
        let df = df_with_optional(vec![Some(1), Some(2), Some(3)]);
        let inv = make_invariant("a");

        let result = df.lazy().select([plan(&inv).unwrap()]).collect().unwrap();

        let value = result.columns()[0].get(0).unwrap();
        let count = value.try_extract::<i64>().unwrap();

        assert_eq!(count, 0);

        let violation = map(&inv, value);
        assert!(violation.is_none());
    }

    #[test]
    fn test_not_null_violation() {
        let df = df_with_optional(vec![Some(1), None, Some(3)]);
        let inv = make_invariant("a");

        let result = df.lazy().select([plan(&inv).unwrap()]).collect().unwrap();

        let value = result.columns()[0].get(0).unwrap();
        let count = value.try_extract::<i64>().unwrap();

        assert_eq!(count, 1);

        let violation = map(&inv, value);
        assert!(violation.is_some());
    }

    #[test]
    fn test_wrong_scope_returns_none() {
        let id = InvariantId::new("wrong_scope").unwrap();
        let inv = Invariant::new(id, PolarsKind::NotNull, Scope::Dataset);

        let expr = plan(&inv);
        assert!(expr.is_none());
    }
}
