pub mod error;
mod from_str;

pub(crate) use crate::invariant::value_object::id::error::InvariantIdError;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize)]
#[serde(transparent)]
pub struct InvariantId(String);

impl InvariantId {
    pub fn new(value: impl Into<String>) -> Result<Self, InvariantIdError> {
        let value = value.into().trim().to_string();

        if value.is_empty() {
            return Err(InvariantIdError::Empty);
        }

        if !value
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        {
            return Err(InvariantIdError::InvalidFormat { value });
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn equals(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
