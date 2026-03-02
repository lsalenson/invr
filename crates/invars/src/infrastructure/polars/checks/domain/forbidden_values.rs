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
    let values = inv.require_param("values").ok()?;

    let forbidden: Vec<&str> = values.split(',').collect();
    let series = Series::new(PlSmallStr::from(""), forbidden);

    Some(col(name).is_in(lit(series).implode(), false).sum())
}

pub fn map(inv: &Invariant<PolarsKind>, value: AnyValue) -> Option<Violation> {
    let violation_count = value.try_extract::<i64>().ok()?;

    if violation_count > 0 {
        Some(
            Violation::new(
                inv.id().clone(),
                inv.severity(),
                inv.scope().clone(),
                format!("{violation_count} forbidden values detected"),
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
    use crate::infrastructure::polars::kind::PolarsKind;
    use std::collections::BTreeMap;

    fn make_invariant(column: &str, forbidden: &str) -> Invariant<PolarsKind> {
        let id = InvariantId::new("forbidden_values_test").unwrap();
        let mut params = BTreeMap::new();
        params.insert("values".to_string(), forbidden.to_string());

        Invariant::new(
            id,
            PolarsKind::ForbiddenValues,
            Scope::Column {
                name: column.to_string(),
            },
        )
        .with_params(params)
    }

    fn df_with_values(values: Vec<&str>) -> DataFrame {
        let series = Series::new(PlSmallStr::from("a"), values);
        DataFrame::new_infer_height(vec![series.into()]).unwrap()
    }

    #[test]
    fn test_plan_returns_expr() {
        let inv = make_invariant("a", "X,Y");
        let expr = plan(&inv);
        assert!(expr.is_some());
    }

    #[test]
    fn test_forbidden_values_no_violation() {
        let df = df_with_values(vec!["A", "B", "C"]);
        let inv = make_invariant("a", "X,Y");

        let result = df
            .lazy()
            .select([plan(&inv).unwrap()])
            .collect()
            .unwrap();

        let value = result.columns()[0].get(0).unwrap();
        let count = value.try_extract::<i64>().unwrap();

        assert_eq!(count, 0);

        let violation = map(&inv, value);
        assert!(violation.is_none());
    }

    #[test]
    fn test_forbidden_values_violation() {
        let df = df_with_values(vec!["A", "X", "B"]);
        let inv = make_invariant("a", "X,Y");

        let result = df
            .lazy()
            .select([plan(&inv).unwrap()])
            .collect()
            .unwrap();

        let value = result.columns()[0].get(0).unwrap();
        let count = value.try_extract::<i64>().unwrap();

        assert_eq!(count, 1);

        let violation = map(&inv, value);
        assert!(violation.is_some());
    }

    #[test]
    fn test_wrong_scope_returns_none() {
        let id = InvariantId::new("wrong_scope").unwrap();
        let inv = Invariant::new(
            id,
            PolarsKind::ForbiddenValues,
            Scope::Dataset,
        );

        let expr = plan(&inv);
        assert!(expr.is_none());
    }
}
