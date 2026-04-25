/// Integration tests for the full RunSpec pipeline.
///
/// These tests exercise the end-to-end path:
/// Invariant → Spec → RunSpec::run → Report
#[cfg(feature = "polars")]
mod tests {
    use invr::prelude::*;
    use polars::prelude::*;

    fn make_df() -> DataFrame {
        df![
            "id"    => [1i64, 2, 3, 4, 5],
            "name"  => ["alice", "bob", "carol", "dave", "eve"],
            "score" => [85.0f64, 92.0, 78.0, 95.0, 88.0],
        ]
        .unwrap()
    }

    // ----------------------------------------------------------------
    // NotNull
    // ----------------------------------------------------------------

    #[test]
    fn not_null_passes_on_clean_column() {
        let df = make_df();
        let spec = Spec::from_invariants(vec![Invariant::new(
            InvariantId::new("id_not_null").unwrap(),
            PolarsKind::NotNull,
            Scope::column("id"),
        )]);
        let report = RunSpec::new(EnginePolarsDataFrame).run(&df, &spec).unwrap();
        assert!(!report.failed());
        assert_eq!(report.error_count(), 0);
    }

    #[test]
    fn not_null_fires_when_column_has_nulls() {
        let df = df![
            "id" => [Some(1i64), None, Some(3)]
        ]
        .unwrap();
        let spec = Spec::from_invariants(vec![Invariant::new(
            InvariantId::new("id_not_null").unwrap(),
            PolarsKind::NotNull,
            Scope::column("id"),
        )]);
        let report = RunSpec::new(EnginePolarsDataFrame).run(&df, &spec).unwrap();
        assert!(report.failed());
        assert_eq!(report.error_count(), 1);
    }

    // ----------------------------------------------------------------
    // Unique
    // ----------------------------------------------------------------

    #[test]
    fn unique_passes_on_unique_column() {
        let df = make_df();
        let spec = Spec::from_invariants(vec![Invariant::new(
            InvariantId::new("id_unique").unwrap(),
            PolarsKind::Unique,
            Scope::column("id"),
        )]);
        let report = RunSpec::new(EnginePolarsDataFrame).run(&df, &spec).unwrap();
        assert!(!report.failed());
    }

    #[test]
    fn unique_fires_on_duplicate_values() {
        let df = df!["id" => [1i64, 2, 2, 4]].unwrap();
        let spec = Spec::from_invariants(vec![Invariant::new(
            InvariantId::new("id_unique").unwrap(),
            PolarsKind::Unique,
            Scope::column("id"),
        )]);
        let report = RunSpec::new(EnginePolarsDataFrame).run(&df, &spec).unwrap();
        assert!(report.failed());
    }

    // ----------------------------------------------------------------
    // RowCountMin
    // ----------------------------------------------------------------

    #[test]
    fn row_count_min_passes() {
        let df = make_df();
        let spec = Spec::from_invariants(vec![
            Invariant::new(
                InvariantId::new("min_rows").unwrap(),
                PolarsKind::RowCountMin,
                Scope::Dataset,
            )
            .with_param_value("min", "3"),
        ]);
        let report = RunSpec::new(EnginePolarsDataFrame).run(&df, &spec).unwrap();
        assert!(!report.failed());
    }

    #[test]
    fn row_count_min_fires_when_too_few_rows() {
        let df = df!["x" => [1i32, 2]].unwrap();
        let spec = Spec::from_invariants(vec![
            Invariant::new(
                InvariantId::new("min_rows").unwrap(),
                PolarsKind::RowCountMin,
                Scope::Dataset,
            )
            .with_param_value("min", "5"),
        ]);
        let report = RunSpec::new(EnginePolarsDataFrame).run(&df, &spec).unwrap();
        assert!(report.failed());
    }

    // ----------------------------------------------------------------
    // Severity: warn does not make report.failed() == true
    // ----------------------------------------------------------------

    #[test]
    fn warn_severity_does_not_fail_report() {
        let df = df!["x" => [Some(1i64), None]].unwrap();
        let spec = Spec::from_invariants(vec![
            Invariant::new(
                InvariantId::new("x_not_null").unwrap(),
                PolarsKind::NotNull,
                Scope::column("x"),
            )
            .with_severity(Severity::Warn),
        ]);
        let report = RunSpec::new(EnginePolarsDataFrame).run(&df, &spec).unwrap();
        assert!(
            !report.failed(),
            "warn violation should not fail the report"
        );
        assert!(report.has_warnings());
        assert_eq!(report.warn_count(), 1);
    }

    // ----------------------------------------------------------------
    // Report serialization
    // ----------------------------------------------------------------

    #[test]
    fn report_serializes_to_json() {
        let df = df!["id" => [Some(1i64), None]].unwrap();
        let spec = Spec::from_invariants(vec![Invariant::new(
            InvariantId::new("id_not_null").unwrap(),
            PolarsKind::NotNull,
            Scope::column("id"),
        )]);
        let report = RunSpec::new(EnginePolarsDataFrame).run(&df, &spec).unwrap();
        let json = serde_json::to_string(&report).expect("report must be serializable");
        assert!(json.contains("id_not_null"));
    }

    // ----------------------------------------------------------------
    // Multiple invariants in one spec
    // ----------------------------------------------------------------

    #[test]
    fn multiple_invariants_accumulate_violations() {
        let df = df![
            "id"   => [Some(1i64), None],
            "code" => [Some("a"), Some("b")]
        ]
        .unwrap();

        let spec = Spec::from_invariants(vec![
            Invariant::new(
                InvariantId::new("id_not_null").unwrap(),
                PolarsKind::NotNull,
                Scope::column("id"),
            ),
            Invariant::new(
                InvariantId::new("row_count").unwrap(),
                PolarsKind::RowCountMin,
                Scope::Dataset,
            )
            .with_param_value("min", "10"),
        ]);
        let report = RunSpec::new(EnginePolarsDataFrame).run(&df, &spec).unwrap();
        assert_eq!(report.error_count(), 2);
    }

    // ----------------------------------------------------------------
    // Report metrics
    // ----------------------------------------------------------------

    #[test]
    fn report_metrics_are_populated() {
        let df = make_df();
        let spec = Spec::from_invariants(vec![Invariant::new(
            InvariantId::new("id_not_null").unwrap(),
            PolarsKind::NotNull,
            Scope::column("id"),
        )]);
        let report = RunSpec::new(EnginePolarsDataFrame).run(&df, &spec).unwrap();
        assert_eq!(report.metrics().total_invariants, 1);
        assert_eq!(report.metrics().violations, 0);
        assert!(report.metrics().execution_time_ms < 60_000);
    }
}
