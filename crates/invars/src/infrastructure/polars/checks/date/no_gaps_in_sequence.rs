use crate::infrastructure::polars::kind::PolarsKind;
use crate::invariant::Invariant;
use crate::scope::Scope;
use polars::prelude::*;
use polars::series::ops::NullBehavior;
pub fn plan(inv: &Invariant<PolarsKind>) -> Option<Expr> {
    let Scope::Column { name } = inv.scope() else {
        return None;
    };

    Some(
        col(name)
            .diff(Expr::from(1), NullBehavior::Ignore)
            .neq(lit(1))
            .sum(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::invariant::value_object::id::InvariantId;
    use crate::infrastructure::polars::kind::PolarsKind;

    fn make_invariant(column: &str) -> Invariant<PolarsKind> {
        let id = InvariantId::new("no_gaps_test").unwrap();
        Invariant::new(
            id,
            PolarsKind::NoGapsInSequence,
            Scope::Column {
                name: column.to_string(),
            },
        )
    }

    fn df_with_values(values: Vec<i64>) -> DataFrame {
        let series = Series::new("a".into(), values);
        DataFrame::new_infer_height(vec![series.into()]).unwrap()
    }

    #[test]
    fn test_plan_returns_expr() {
        let inv = make_invariant("a");
        let expr = plan(&inv);
        assert!(expr.is_some());
    }

    #[test]
    fn test_no_gaps_no_violation() {
        let df = df_with_values(vec![1, 2, 3, 4]);
        let inv = make_invariant("a");

        let result = df
            .lazy()
            .select([plan(&inv).unwrap()])
            .collect()
            .unwrap();

        let value = result.columns()[0].get(0).unwrap();
        let count = value.try_extract::<i64>().unwrap();

        assert_eq!(count, 0);
    }

    #[test]
    fn test_gap_violation() {
        let df = df_with_values(vec![1, 2, 4, 5]);
        let inv = make_invariant("a");

        let result = df
            .lazy()
            .select([plan(&inv).unwrap()])
            .collect()
            .unwrap();

        let value = result.columns()[0].get(0).unwrap();
        let count = value.try_extract::<i64>().unwrap();

        assert!(count > 0);
    }

    #[test]
    fn test_wrong_scope_returns_none() {
        let id = InvariantId::new("wrong_scope").unwrap();
        let inv = Invariant::new(
            id,
            PolarsKind::NoGapsInSequence,
            Scope::Dataset,
        );

        let expr = plan(&inv);
        assert!(expr.is_none());
    }
}