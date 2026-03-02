use crate::infrastructure::polars::kind::PolarsKind;
use crate::infrastructure::polars::utils::metric_violation;
use crate::invariant::Invariant;
use crate::scope::Scope;
use crate::violation::Violation;
use polars::datatypes::AnyValue;
use polars::prelude::{Expr, col};

pub fn plan(inv: &Invariant<PolarsKind>) -> Option<Expr> {
    let Scope::Column { name } = inv.scope() else {
        return None;
    };
    Some(col(name).n_unique())
}

pub fn map(inv: &Invariant<PolarsKind>, v: AnyValue) -> Option<Violation> {
    let unique = v.try_extract::<i64>().ok()?;
    let total: i64 = inv.require_param("row_count_cache").ok()?.parse().ok()?;
    metric_violation::<PolarsKind>(
        inv,
        "duplicate_count",
        total - unique,
        format!("duplicates detected"),
    )
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::polars::kind::PolarsKind;
    use crate::invariant::Invariant;
    use crate::scope::Scope;
    use std::collections::BTreeMap;
    use crate::prelude::InvariantId;
    use polars::df;
    use polars::prelude::{DataFrame, IntoLazy};

    fn make_invariant(row_count: i64) -> Invariant<PolarsKind> {
        let mut params = BTreeMap::new();
        params.insert("row_count_cache".to_string(), row_count.to_string());

        Invariant::new(
            InvariantId::new("unique_test").unwrap(),
            PolarsKind::Unique,
            Scope::Column {
                name: "a".to_string(),
            },
        )
        .with_params(params)
    }

    fn df(values: Vec<i32>) -> DataFrame {
        df![
            "a" => values
        ]
        .unwrap()
    }

    #[test]
    fn test_plan_returns_expr() {
        let inv = make_invariant(4);
        assert!(plan(&inv).is_some());
    }

    #[test]
    fn test_no_violation_when_all_unique() {
        let df = df(vec![1, 2, 3, 4]);
        let inv = make_invariant(4);

        let unique = df
            .lazy()
            .select([plan(&inv).unwrap()])
            .collect()
            .unwrap()
            .column("a")
            .unwrap()
            .get(0)
            .unwrap()
            .try_extract::<i64>()
            .unwrap();

        let violation = map(&inv, AnyValue::Int64(unique));
        assert!(violation.is_none());
    }

    #[test]
    fn test_violation_when_duplicates_exist() {
        let df = df(vec![1, 1, 2, 3]);
        let inv = make_invariant(4);

        let unique = df
            .lazy()
            .select([plan(&inv).unwrap()])
            .collect()
            .unwrap()
            .column("a")
            .unwrap()
            .get(0)
            .unwrap()
            .try_extract::<i64>()
            .unwrap();

        let violation = map(&inv, AnyValue::Int64(unique));
        assert!(violation.is_some());
    }

    #[test]
    fn test_wrong_scope_returns_none() {
        let mut params = BTreeMap::new();
        params.insert("row_count_cache".to_string(), "4".to_string());

        let inv = Invariant::new(
            InvariantId::new("wrong_scope").unwrap(),
            PolarsKind::Unique,
            Scope::Dataset,
        )
        .with_params(params);

        assert!(plan(&inv).is_none());
    }
}
