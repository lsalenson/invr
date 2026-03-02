use crate::infrastructure::polars::kind::PolarsKind;
use crate::invariant::Invariant;
use crate::scope::Scope;
use polars::prelude::*;

pub fn plan(inv: &Invariant<PolarsKind>) -> Option<Expr> {
    let Scope::Column { name } = inv.scope() else {
        return None;
    };
    let max: f64 = inv.require_param("max").ok()?.parse().ok()?;

    Some(col(name).cast(DataType::Float64).gt(lit(max)).sum())
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::invariant::value_object::id::InvariantId;
    use polars::prelude::*;
    use std::collections::BTreeMap;

    fn make_invariant(column: &str, max: f64) -> Invariant<PolarsKind> {
        let id = InvariantId::new("value_max_test").unwrap();
        let mut params = BTreeMap::new();
        params.insert("max".to_string(), max.to_string());

        Invariant::new(
            id,
            PolarsKind::ValueMax,
            Scope::Column {
                name: column.to_string(),
            },
        )
        .with_params(params)
    }

    fn df(values: Vec<i32>) -> DataFrame {
        let series = Series::new("a".into(), values);
        let height = series.len();
        DataFrame::new(height, vec![series.into()]).unwrap()
    }

    #[test]
    fn test_plan_returns_expr() {
        let inv = make_invariant("a", 10.0);
        assert!(plan(&inv).is_some());
    }

    #[test]
    fn test_value_max_no_violation() {
        let df = df(vec![1, 2, 3]);
        let inv = make_invariant("a", 10.0);

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
    fn test_value_max_violation() {
        let df = df(vec![1, 20, 3]);
        let inv = make_invariant("a", 10.0);

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
            PolarsKind::ValueMax,
            Scope::Dataset,
        );

        assert!(plan(&inv).is_none());
    }
}
