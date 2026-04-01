/// Integration tests for YAML spec loading.
#[cfg(all(feature = "yaml", feature = "polars"))]
mod tests {
    use invars::prelude::*;
    use polars::prelude::*;

    const SPEC_YAML: &str = r#"
invariants:
  - id: age_not_null
    kind: not_null
    scope:
      type: column
      name: age

  - id: age_min
    kind: value_min
    scope:
      type: column
      name: age
    params:
      min: "0"
      column: age

  - id: row_count_check
    kind: row_count_min
    scope:
      type: dataset
    params:
      min: "1"

  - id: warn_null_score
    kind: not_null
    scope:
      type: column
      name: score
    severity: warn
"#;

    #[test]
    fn spec_loads_from_yaml_string() {
        let spec = spec_from_str(SPEC_YAML).expect("valid YAML should parse");
        assert_eq!(spec.invariants().len(), 4);
    }

    #[test]
    fn loaded_spec_preserves_severity() {
        let spec = spec_from_str(SPEC_YAML).unwrap();
        let warn_inv = spec
            .find_by_str("warn_null_score")
            .expect("invariant must exist");
        assert_eq!(warn_inv.severity(), Severity::Warn);
    }

    #[test]
    fn loaded_spec_preserves_params() {
        let spec = spec_from_str(SPEC_YAML).unwrap();
        let inv = spec
            .find_by_str("row_count_check")
            .expect("invariant must exist");
        assert_eq!(inv.param("min"), Some("1"));
    }

    #[test]
    fn loaded_spec_runs_against_dataframe() {
        let spec = spec_from_str(SPEC_YAML).unwrap();
        let df = df![
            "age"   => [25i64, 30, 45],
            "score" => [Some(85.0f64), None, Some(92.0)],
        ]
        .unwrap();
        let report = RunSpec::new(EnginePolarsDataFrame).run(&df, &spec).unwrap();
        // age has no nulls and score has one null → warn violation but no error
        assert!(!report.failed());
        assert!(report.has_warnings());
    }

    #[test]
    fn invalid_yaml_returns_parse_error() {
        let bad = "invariants: [[[";
        let err = spec_from_str(bad).unwrap_err();
        assert!(matches!(err, YamlError::Parse(_)));
    }

    #[test]
    fn duplicate_ids_return_spec_error() {
        let bad = r#"
invariants:
  - id: duplicate
    kind: not_null
    scope:
      type: column
      name: x
  - id: duplicate
    kind: not_null
    scope:
      type: column
      name: y
"#;
        let err = spec_from_str(bad).unwrap_err();
        assert!(matches!(err, YamlError::Spec(_)));
    }
}
