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

    let condition_column = inv.require_param("condition_column").ok()?;
    let condition_value = inv.require_param("condition_value").ok()?;

    Some(
        col(condition_column)
            .eq(lit(condition_value))
            .and(col(name).is_null())
            .sum(),
    )
}

pub fn map(inv: &Invariant<PolarsKind>, value: AnyValue) -> Option<Violation> {
    let violation_count = value.try_extract::<i64>().ok()?;

    if violation_count > 0 {
        Some(
            Violation::new(
                inv.id().clone(),
                inv.severity(),
                inv.scope().clone(),
                format!("{violation_count} rows violate conditional not null rule"),
            )
            .with_metric("violation_count", MetricValue::Int(violation_count)),
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

    fn make_invariant(target: &str, condition_col: &str, condition_val: &str) -> Invariant<PolarsKind> {
        let id = InvariantId::new("conditional_not_null_test").unwrap();
        let mut params = BTreeMap::new();
        params.insert("condition_column".to_string(), condition_col.to_string());
        params.insert("condition_value".to_string(), condition_val.to_string());

        Invariant::new(
            id,
            PolarsKind::ConditionalNotNull,
            Scope::Column {
                name: target.to_string(),
            },
        )
        .with_params(params)
    }

    fn df(cond: Vec<&str>, target: Vec<Option<i32>>) -> DataFrame {
        let s1 = Series::new("cond".into(), cond);
        let s2 = Series::new("a".into(), target);
        let height = s1.len();
        DataFrame::new(height, vec![s1.into(), s2.into()]).unwrap()
    }

    #[test]
    fn test_plan_returns_expr() {
        let inv = make_invariant("a", "cond", "X");
        assert!(plan(&inv).is_some());
    }

    #[test]
    fn test_no_violation_when_condition_not_met() {
        let df = df(vec!["A", "B"], vec![None, None]);
        let inv = make_invariant("a", "cond", "X");

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
    fn test_violation_when_condition_met_and_null() {
        let df = df(vec!["X", "X"], vec![None, Some(1)]);
        let inv = make_invariant("a", "cond", "X");

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
            PolarsKind::ConditionalNotNull,
            Scope::Dataset,
        );

        assert!(plan(&inv).is_none());
    }
}
