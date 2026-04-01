use crate::infrastructure::polars::kind::PolarsKind;
use crate::invariant::Invariant;
use crate::scope::Scope;
use polars::prelude::*;
use polars::series::ops::NullBehavior;
/// Builds the Polars expression counting rows that break
/// a strictly monotonic increasing order.
///
/// Scope:
/// - Requires `Scope::Column`
///
/// Behavior:
/// - Computes the difference between consecutive rows using `diff(1)`
/// - Detects rows where the difference is negative (`< 0`)
/// - Sums the boolean mask to count order violations
///
/// The resulting metric represents the number of monotonicity
/// violations in the column.
///
/// A result of `0` means the column is monotonic increasing.
pub fn plan(inv: &Invariant<PolarsKind>) -> Option<Expr> {
    let Scope::Column { name } = inv.scope() else {
        return None;
    };

    Some(
        col(name)
            .diff(Expr::from(1), NullBehavior::Ignore)
            .lt(lit(0))
            .sum(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::invariant::value_object::id::InvariantId;
    use crate::infrastructure::polars::kind::PolarsKind;
    use crate::scope::Scope;
    use polars::df;

    fn make_invariant(column: &str) -> Invariant<PolarsKind> {
        let id = InvariantId::new("monotonic_test").unwrap();
        Invariant::new(
            id,
            PolarsKind::MonotonicIncreasing,
            Scope::Column {
                name: column.to_string(),
            },
        )
    }

    #[test]
    fn test_plan_returns_expr() {
        let inv = make_invariant("a");
        let expr = plan(&inv);
        assert!(expr.is_some());
    }

    #[test]
    fn test_monotonic_increasing_no_violation() {
        let df = df! {
            "a" => &[1i64, 2i64, 3i64, 4i64]
        }
        .unwrap();

        let inv = make_invariant("a");

        let result = df.lazy().select([plan(&inv).unwrap()]).collect().unwrap();

        let value = result.columns()[0].get(0).unwrap();
        let count = value.try_extract::<i64>().unwrap();

        assert_eq!(count, 0);
    }

    #[test]
    fn test_monotonic_increasing_violation() {
        let df = df! {
            "a" => &[1i64, 3i64, 2i64, 4i64]
        }
        .unwrap();

        let inv = make_invariant("a");

        let result = df.lazy().select([plan(&inv).unwrap()]).collect().unwrap();

        let value = result.columns()[0].get(0).unwrap();
        let count = value.try_extract::<i64>().unwrap();

        assert!(count > 0);
    }

    #[test]
    fn test_wrong_scope_returns_none() {
        let id = InvariantId::new("wrong_scope").unwrap();
        let inv = Invariant::new(id, PolarsKind::MonotonicIncreasing, Scope::Dataset);

        let expr = plan(&inv);
        assert!(expr.is_none());
    }
}
