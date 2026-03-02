use crate::infrastructure::polars::kind::PolarsKind;
use crate::invariant::Invariant;
use crate::scope::Scope;
use polars::prelude::*;

pub fn plan(inv: &Invariant<PolarsKind>) -> Option<Expr> {
    let Scope::Column { name } = inv.scope() else {
        return None;
    };

    let min: i64 = inv.require_param("min").ok()?.parse().ok()?;
    let max: i64 = inv.require_param("max").ok()?.parse().ok()?;

    let len = col(name).str().len_chars();

    let too_short = len.clone().lt(lit(min));
    let too_long = len.gt(lit(max));

    Some(too_short.or(too_long).sum())
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::invariant::value_object::id::InvariantId;
    use polars::prelude::*;
    use std::collections::BTreeMap;

    fn make_invariant(min: i64, max: i64) -> Invariant<PolarsKind> {
        let id = InvariantId::new("string_length_between_test").unwrap();
        let mut params = BTreeMap::new();
        params.insert("min".to_string(), min.to_string());
        params.insert("max".to_string(), max.to_string());

        Invariant::new(
            id,
            PolarsKind::StringLengthBetween,
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
        let inv = make_invariant(2, 5);
        assert!(plan(&inv).is_some());
    }

    #[test]
    fn test_no_violation_when_lengths_in_range() {
        let df = df(vec!["ab", "abcd", "abc"]);
        let inv = make_invariant(2, 5);

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
    fn test_violation_when_length_out_of_range() {
        let df = df(vec!["a", "abcdef", "abc"]); // too short + too long
        let inv = make_invariant(2, 5);

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

        assert_eq!(count, 2);
    }

    #[test]
    fn test_wrong_scope_returns_none() {
        let id = InvariantId::new("wrong_scope").unwrap();
        let inv = Invariant::new(
            id,
            PolarsKind::StringLengthBetween,
            Scope::Dataset,
        );

        assert!(plan(&inv).is_none());
    }
}