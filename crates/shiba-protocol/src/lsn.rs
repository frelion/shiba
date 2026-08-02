use core::fmt;
use core::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

/// A `PostgreSQL` WAL location represented as an unsigned 64-bit coordinate.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PostgresLsn(u64);

impl PostgresLsn {
    pub const ZERO: Self = Self(0);

    #[must_use]
    pub const fn from_u64(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    #[must_use]
    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }
}

impl fmt::Display for PostgresLsn {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{:X}/{:X}",
            self.0 >> 32,
            self.0 & u64::from(u32::MAX)
        )
    }
}

/// The supplied LSN is not the canonical `PostgreSQL` `X/Y` representation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParsePostgresLsnError;

impl fmt::Display for ParsePostgresLsnError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid canonical PostgreSQL LSN")
    }
}

impl std::error::Error for ParsePostgresLsnError {}

impl FromStr for PostgresLsn {
    type Err = ParsePostgresLsnError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let (high, low) = input.split_once('/').ok_or(ParsePostgresLsnError)?;
        if high.is_empty() || low.is_empty() || high.len() > 8 || low.len() > 8 || low.contains('/')
        {
            return Err(ParsePostgresLsnError);
        }
        let high = u32::from_str_radix(high, 16).map_err(|_| ParsePostgresLsnError)?;
        let low = u32::from_str_radix(low, 16).map_err(|_| ParsePostgresLsnError)?;
        let value = Self((u64::from(high) << 32) | u64::from(low));
        if value.to_string() != input {
            return Err(ParsePostgresLsnError);
        }
        Ok(value)
    }
}

impl Serialize for PostgresLsn {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for PostgresLsn {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = <&str>::deserialize(deserializer)?;
        encoded.parse().map_err(de::Error::custom)
    }
}
