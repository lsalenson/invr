use crate::infrastructure::polars::kind::PolarsKind;
use crate::invariant::Invariant;
use crate::scope::Scope;
use chrono::NaiveDate;
use polars::prelude::*;

/// Builds the Polars expression counting rows where date values
/// fall outside a specified inclusive range.
///
/// Required parameters:
/// - `start`: lower bound date (format: "YYYY-MM-DD")
/// - `end`: upper bound date (format: "YYYY-MM-DD")
///
/// Scope:
/// - Requires `Scope::Column`
///
/// Behavior:
/// - Parses `start` and `end` as `NaiveDate`
/// - Compares the column against the bounds
/// - Marks rows where `date < start` OR `date > end`
/// - Returns the total count of out-of-range dates
///
/// The resulting metric represents the number of rows
/// violating the date range constraint.
///
/// Note:
/// - Bounds are inclusive. Dates equal to `start` or `end`
///   are considered valid.

pub fn plan(inv: &Invariant<PolarsKind>) -> Option<Expr> {
    let Scope::Column { name } = inv.scope() else {
        return None;
    };

    let start = inv.require_param("start").ok()?;
    let end = inv.require_param("end").ok()?;

    let start_date = NaiveDate::parse_from_str(start, "%Y-%m-%d").ok()?;
    let end_date = NaiveDate::parse_from_str(end, "%Y-%m-%d").ok()?;

    Some(
        col(name)
            .lt(lit(start_date))
            .or(col(name).gt(lit(end_date)))
            .sum(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::invariant::value_object::id::InvariantId;
    use crate::scope::Scope;
    use chrono::NaiveDate;

    fn make_invariant(start: &str, end: &str) -> Invariant<PolarsKind> {
        let id = InvariantId::new("date_between_test").unwrap();
        Invariant::new(
            id,
            PolarsKind::DateBetween,
            Scope::Column {
                name: "d".to_string(),
            },
        )
        .with_param_value("start".to_string(), start.to_string())
        .with_param_value("end".to_string(), end.to_string())
    }

    fn make_df() -> DataFrame {
        let dates = vec![
            NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2023, 1, 5).unwrap(),
            NaiveDate::from_ymd_opt(2023, 1, 10).unwrap(),
        ];
        DataFrame::new_infer_height(vec![Series::new(PlSmallStr::from("d"), dates).into()]).unwrap()
    }

    #[test]
    fn test_plan_returns_expr() {
        let inv = make_invariant("2023-01-01", "2023-01-31");
        let expr = plan(&inv);
        assert!(expr.is_some());
    }

    #[test]
    fn test_date_between_no_violation() {
        let df = make_df();
        let inv = make_invariant("2023-01-01", "2023-01-31");

        let result = df.lazy().select([plan(&inv).unwrap()]).collect().unwrap();

        let value = result.columns()[0].get(0).unwrap();
        let count = value.try_extract::<i64>().unwrap();

        assert_eq!(count, 0);
    }

    #[test]
    fn test_date_between_violation() {
        let df = make_df();
        let inv = make_invariant("2023-01-03", "2023-01-08");

        let result = df.lazy().select([plan(&inv).unwrap()]).collect().unwrap();

        let value = result.columns()[0].get(0).unwrap();
        let count = value.try_extract::<i64>().unwrap();

        assert!(count > 0);
    }
}
