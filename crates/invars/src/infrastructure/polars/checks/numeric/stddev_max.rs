use crate::infrastructure::polars::kind::PolarsKind;
use crate::invariant::Invariant;
use crate::scope::Scope;
use crate::violation::Violation;
use crate::violation::value_object::metric_value::MetricValue;
use polars::prelude::*;

pub fn plan(inv: &Invariant<PolarsKind>) -> Option<Expr> {
    let Scope::Column { name } = inv.scope() else {
        return None;
    };

    Some(col(name).cast(DataType::Float64).std(1))
}
pub fn map(inv: &Invariant<PolarsKind>, value: AnyValue) -> Option<Violation> {
    let std = value.try_extract::<f64>().ok()?;
    let max: f64 = inv.require_param("max").ok()?.parse().ok()?;

    if std > max {
        Some(
            Violation::new(
                inv.id().clone(),
                inv.severity(),
                inv.scope().clone(),
                format!("std deviation {std} exceeds max {max}"),
            )
            .with_metric("std_dev", MetricValue::Float(std)),
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

    fn make_invariant(column: &str, max: f64) -> Invariant<PolarsKind> {
        let id = InvariantId::new("stddev_max_test").unwrap();
        let mut params = BTreeMap::new();
        params.insert("max".to_string(), max.to_string());

        Invariant::new(
            id,
            PolarsKind::StdDevMax,
            Scope::Column {
                name: column.to_string(),
            },
        )
        .with_params(params)
    }

    fn df(values: Vec<i32>) -> DataFrame {
        let series = Series::new(PlSmallStr::from("a"), values);
        DataFrame::new_infer_height(vec![series.into()]).unwrap()
    }

    #[test]
    fn test_plan_returns_expr() {
        let inv = make_invariant("a", 10.0);
        let expr = plan(&inv);
        assert!(expr.is_some());
    }

    #[test]
    fn test_stddev_no_violation() {
        let df = df(vec![10, 11, 9]);
        let inv = make_invariant("a", 5.0);

        let result = df
            .lazy()
            .select([plan(&inv).unwrap()])
            .collect()
            .unwrap();

        let value = result.columns()[0].get(0).unwrap();
        let violation = map(&inv, value);

        assert!(violation.is_none());
    }

    #[test]
    fn test_stddev_violation() {
        let df = df(vec![0, 100, 200]);
        let inv = make_invariant("a", 10.0);

        let result = df
            .lazy()
            .select([plan(&inv).unwrap()])
            .collect()
            .unwrap();

        let value = result.columns()[0].get(0).unwrap();
        let violation = map(&inv, value);

        assert!(violation.is_some());
    }

    #[test]
    fn test_wrong_scope_returns_none() {
        let id = InvariantId::new("wrong_scope").unwrap();
        let inv = Invariant::new(
            id,
            PolarsKind::StdDevMax,
            Scope::Dataset,
        );

        let expr = plan(&inv);
        assert!(expr.is_none());
    }
}