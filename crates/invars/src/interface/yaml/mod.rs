pub mod dto;
pub mod mapper;

#[cfg(feature = "yaml")]
mod error;
#[cfg(feature = "yaml")]
pub use error::YamlLoadError;

#[cfg(all(feature = "yaml", feature = "polars"))]
pub use load::spec_from_str;

#[cfg(all(feature = "yaml", feature = "polars"))]
mod load {
    use super::error::YamlLoadError;
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
    pub fn spec_from_str(s: &str) -> Result<Spec<PolarsKind>, YamlLoadError> {
        let yaml: SpecYaml<PolarsKindYaml> =
            serde_yml::from_str(s).map_err(YamlLoadError::Parse)?;
        Spec::try_from(yaml).map_err(YamlLoadError::Spec)
    }
}
