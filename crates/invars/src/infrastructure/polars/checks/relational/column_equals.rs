use crate::infrastructure::polars::kind::PolarsKind;
use crate::invariant::Invariant;
use crate::scope::Scope;
use crate::violation::Violation;
use crate::violation::value_object::metric_value::MetricValue;
use polars::prelude::AnyValue;
use polars::prelude::*;

pub fn plan(inv: &Invariant<PolarsKind>) -> Option<Expr> {
    let Scope::Column { name } = inv.scope() else {
        return None;
    };
    let other = inv.require_param("other_column").ok()?;

    Some(col(name).neq(col(other)).sum())
}

pub fn map(inv: &Invariant<PolarsKind>, value: AnyValue) -> Option<Violation> {
    let mismatch_count = value.try_extract::<i64>().ok()?;

    if mismatch_count > 0 {
        Some(
            Violation::new(
                inv.id().clone(),
                inv.severity(),
                inv.scope().clone(),
                format!("{mismatch_count} mismatching rows"),
            )
            .with_metric("mismatch_count", MetricValue::Int(mismatch_count)),
        )
    } else {
        None
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::invariant::value_object::id::InvariantId;
    use polars::prelude::*;
    use std::collections::BTreeMap;

    fn make_invariant(col_a: &str, col_b: &str) -> Invariant<PolarsKind> {
        let id = InvariantId::new("column_equals_test").unwrap();
        let mut params = BTreeMap::new();
        params.insert("other_column".to_string(), col_b.to_string());

        Invariant::new(
            id,
            PolarsKind::ColumnEquals,
            Scope::Column {
                name: col_a.to_string(),
            },
        )
        .with_params(params)
    }

    fn df(a: Vec<i32>, b: Vec<i32>) -> DataFrame {
        let s1 = Series::new("a".into(), a);
        let s2 = Series::new("b".into(), b);
        let height = s1.len();
        DataFrame::new(height, vec![s1.into(), s2.into()]).unwrap()
    }

    #[test]
    fn test_plan_returns_expr() {
        let inv = make_invariant("a", "b");
        assert!(plan(&inv).is_some());
    }

    #[test]
    fn test_column_equals_no_violation() {
        let df = df(vec![1, 2, 3], vec![1, 2, 3]);
        let inv = make_invariant("a", "b");

        let result = df
            .lazy()
            .select([plan(&inv).unwrap()])
            .collect()
            .unwrap();

        let count = result.columns()[0]
            .get(0)
            .unwrap()
            .try_extract::<i64>()
            .unwrap();

        assert_eq!(count, 0);
    }

    #[test]
    fn test_column_equals_violation() {
        let df = df(vec![1, 2, 3], vec![1, 999, 3]);
        let inv = make_invariant("a", "b");

        let result = df
            .lazy()
            .select([plan(&inv).unwrap()])
            .collect()
            .unwrap();

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
        let inv = Invariant::new(
            id,
            PolarsKind::ColumnEquals,
            Scope::Dataset,
        );

        assert!(plan(&inv).is_none());
    }
}
