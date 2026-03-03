mod display;
pub mod error;
mod from_str;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Scope {
    Dataset,
    Column { name: String },
}

impl Scope {
    pub fn dataset() -> Self {
        Scope::Dataset
    }

    pub fn column(name: impl Into<String>) -> Self {
        Scope::Column { name: name.into() }
    }

    pub fn is_dataset(&self) -> bool {
        matches!(self, Scope::Dataset)
    }

    pub fn column_name(&self) -> Option<&str> {
        match self {
            Scope::Column { name } => Some(name),
            _ => None,
        }
    }
}
