mod display;
pub mod error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Info,
    Warn,
    Error,
}

impl Severity {
    pub fn is_error(self) -> bool {
        matches!(self, Severity::Error)
    }

    pub fn is_info(self) -> bool {
        matches!(self, Severity::Info)
    }

    pub fn is_warn(self) -> bool {
        matches!(self, Severity::Warn)
    }

    pub fn level(self) -> u8 {
        match self {
            Severity::Info => 0,
            Severity::Warn => 1,
            Severity::Error => 2,
        }
    }
}
