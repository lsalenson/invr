pub mod dto;
pub mod error;
pub mod import;
pub mod mapper;

#[cfg(all(feature = "yaml", feature = "polars"))]
pub use load::spec_from_str;

#[cfg(all(feature = "yaml", feature = "polars"))]
mod load {
    use super::error::YamlError;
    use crate::infrastructure::polars::kind::PolarsKind;
    use crate::interface::yaml::dto::kind::polars::PolarsKindYaml;
    use crate::interface::yaml::dto::spec::SpecYaml;
    use crate::spec::Spec;

    /// Deserialize a [`Spec<PolarsKind>`] from a YAML string.
    ///
    /// # Example
    ///
    /// ```yaml
    /// invariants:
    ///   - id: age_not_null
    ///     kind: not_null
    ///     scope:
    ///       type: column
    ///       name: age
    ///   - id: row_count_check
    ///     kind: row_count_min
    ///     scope:
    ///       type: dataset
    ///     params:
    ///       min: "100"
    /// ```
    pub fn spec_from_str(s: &str) -> Result<Spec<PolarsKind>, YamlError> {
        let yaml: SpecYaml<PolarsKindYaml> = serde_yaml::from_str(s).map_err(YamlError::Parse)?;
        Spec::try_from(yaml).map_err(YamlError::Spec)
    }
}
