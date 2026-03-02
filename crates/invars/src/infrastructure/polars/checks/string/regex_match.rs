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
    let pattern = inv.require_param("pattern").ok()?;

    Some(
        col(name)
            .cast(DataType::String)
            .str()
            .extract(lit(pattern), 0)
            .is_null()
            .sum(),
    )
}

pub fn map(inv: &Invariant<PolarsKind>, value: AnyValue) -> Option<Violation> {
    let invalid_count = value.try_extract::<i64>().ok()?;

    if invalid_count > 0 {
        Some(
            Violation::new(
                inv.id().clone(),
                inv.severity(),
                inv.scope().clone(),
                format!("{invalid_count} values do not match regex"),
            )
            .with_metric("invalid_count", MetricValue::Int(invalid_count)),
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

    fn make_invariant(pattern: &str) -> Invariant<PolarsKind> {
        let id = InvariantId::new("regex_match_test").unwrap();
        let mut params = BTreeMap::new();
        params.insert("pattern".to_string(), pattern.to_string());

        Invariant::new(
            id,
            PolarsKind::RegexMatch,
            Scope::Column {
                name: "a".to_string(),
            },
        )
        .with_params(params)
    }

    fn df(values: Vec<&str>) -> DataFrame {
        let height = values.len();
        let series = Series::new("a".into(), values);
        DataFrame::new(height, vec![series.into()]).unwrap()
    }

    #[test]
    fn test_plan_returns_expr() {
        let inv = make_invariant("^a.*$");
        assert!(plan(&inv).is_some());
    }

    #[test]
    fn test_regex_match_no_violation() {
        let df = df(vec!["apple", "avocado", "apricot"]);
        let inv = make_invariant("^a.*$");

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

        assert!(map(&inv, AnyValue::Int64(count)).is_none());
    }

    #[test]
    fn test_regex_match_violation() {
        let df = df(vec!["apple", "banana", "apricot"]);
        let inv = make_invariant("^a.*$");

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

        let violation = map(&inv, AnyValue::Int64(count));
        assert!(violation.is_some());
    }

    #[test]
    fn test_wrong_scope_returns_none() {
        let id = InvariantId::new("wrong_scope").unwrap();
        let inv = Invariant::new(
            id,
            PolarsKind::RegexMatch,
            Scope::Dataset,
        );

        assert!(plan(&inv).is_none());
    }
}
