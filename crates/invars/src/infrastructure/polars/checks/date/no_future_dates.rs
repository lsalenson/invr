use crate::infrastructure::polars::kind::PolarsKind;
use crate::invariant::Invariant;
use crate::scope::Scope;
use chrono::Utc;
use polars::prelude::*;
pub fn plan(inv: &Invariant<PolarsKind>) -> Option<Expr> {
    let Scope::Column { name } = inv.scope() else {
        return None;
    };

    let today = Utc::now().date_naive();

    Some(col(name).gt(lit(today)).sum())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::invariant::value_object::id::InvariantId;
    use crate::infrastructure::polars::kind::PolarsKind;
    use chrono::{Duration, NaiveDate, Utc};
    use polars::prelude::*;

    fn make_invariant(column: &str) -> Invariant<PolarsKind> {
        let id = InvariantId::new("no_future_test").unwrap();
        Invariant::new(
            id,
            PolarsKind::NoFutureDates,
            Scope::Column {
                name: column.to_string(),
            },
        )
    }

    fn df_with_dates(dates: Vec<NaiveDate>) -> DataFrame {
        let series = Series::new("d".into(), dates);
        DataFrame::new_infer_height(vec![series.into()]).unwrap()
    }

    #[test]
    fn test_plan_returns_expr() {
        let inv = make_invariant("d");
        let expr = plan(&inv);
        assert!(expr.is_some());
    }

    #[test]
    fn test_no_future_dates_no_violation() {
        let today = Utc::now().date_naive();
        let dates = vec![today - Duration::days(2), today];
        let df = df_with_dates(dates);
        let inv = make_invariant("d");

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
    fn test_no_future_dates_violation() {
        let today = Utc::now().date_naive();
        let dates = vec![today, today + Duration::days(3)];
        let df = df_with_dates(dates);
        let inv = make_invariant("d");

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
            PolarsKind::NoFutureDates,
            Scope::Dataset,
        );

        let expr = plan(&inv);
        assert!(expr.is_none());
    }
}
