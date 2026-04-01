use crate::interface::yaml::dto::scope::ScopeYaml;
use crate::scope::Scope;
use crate::scope::error::ScopeError;

impl TryFrom<ScopeYaml> for Scope {
    type Error = ScopeError;

    fn try_from(value: ScopeYaml) -> Result<Self, Self::Error> {
        match value {
            ScopeYaml::Dataset => Ok(Scope::Dataset),
            ScopeYaml::Column { name } => Ok(Scope::Column { name }),
        }
    }
}
