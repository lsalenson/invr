use crate::interface::yaml::dto::severity::SeverityYaml;
use crate::severity::Severity;
impl From<SeverityYaml> for Severity {
    fn from(value: SeverityYaml) -> Self {
        match value {
            SeverityYaml::Info => Severity::Info,
            SeverityYaml::Warn => Severity::Warn,
            SeverityYaml::Error => Severity::Error,
        }
    }
}
