#[cfg(feature = "polars")]
use std::path::Path;

#[cfg(feature = "polars")]
use crate::infrastructure::polars::kind::PolarsKind;
#[cfg(feature = "polars")]
use crate::interface::yaml::dto::kind::polars::PolarsKindYaml;
#[cfg(feature = "polars")]
use crate::interface::yaml::dto::spec::SpecYaml;
#[cfg(feature = "polars")]
use crate::spec::Spec;

#[cfg(feature = "polars")]
use super::error::YamlError;

#[cfg(feature = "polars")]
pub fn load_spec_from_path(path: &Path) -> Result<Spec<PolarsKind>, YamlError> {
    let content = std::fs::read_to_string(path).map_err(YamlError::Io)?;
    load_spec_from_str(&content)
}

#[cfg(feature = "polars")]
pub fn load_spec_from_str(content: &str) -> Result<Spec<PolarsKind>, YamlError> {
    let dto: SpecYaml<PolarsKindYaml> = serde_yaml::from_str(content).map_err(YamlError::Parse)?;
    Spec::try_from(dto).map_err(YamlError::Spec)
}
