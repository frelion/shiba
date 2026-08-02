use serde::{Deserialize, Deserializer, Serialize, de};

use crate::WireDigest;
use crate::{CatalogVersion, CauseId, CommitFrontier, ProtocolVersion, SourceTransactionId};

/// Phase-1 messages only. Operator, runtime, and effect messages are absent by
/// design until their contracts are proven.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "body",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum WireMessage {
    SourceTransaction(SourceTransactionId),
    Cause(CauseId),
    CommitFrontier(CommitFrontier),
}

/// The only accepted clean-room Phase-1 wire envelope.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct WireEnvelope {
    protocol_version: ProtocolVersion,
    catalog_version: CatalogVersion,
    message: WireMessage,
}

impl WireEnvelope {
    #[must_use]
    pub const fn new(message: WireMessage) -> Self {
        Self {
            protocol_version: ProtocolVersion::INITIAL,
            catalog_version: CatalogVersion::INITIAL,
            message,
        }
    }

    #[must_use]
    pub const fn protocol_version(&self) -> ProtocolVersion {
        self.protocol_version
    }

    #[must_use]
    pub const fn catalog_version(&self) -> CatalogVersion {
        self.catalog_version
    }

    #[must_use]
    pub const fn message(&self) -> WireMessage {
        self.message
    }

    /// Produces the unique compact JSON representation for this protocol
    /// version. Phase-1 messages contain no maps or floating-point values.
    ///
    /// # Errors
    ///
    /// Returns the serialization error if the envelope cannot be encoded.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Hashes the exact canonical JSON behind the version-1 domain separator.
    ///
    /// # Errors
    ///
    /// Returns the serialization error if the envelope cannot be encoded.
    pub fn digest(&self) -> Result<WireDigest, serde_json::Error> {
        Ok(WireDigest::for_canonical_json(&self.to_canonical_json()?))
    }

    /// Decodes exactly one envelope and rejects trailing data.
    ///
    /// # Errors
    ///
    /// Returns a deserialization error for malformed, unsupported, or trailing
    /// input.
    pub fn from_json(input: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(input)
    }
}

impl<'de> Deserialize<'de> for WireEnvelope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            protocol_version: ProtocolVersion,
            catalog_version: CatalogVersion,
            message: WireMessage,
        }

        let raw = Raw::deserialize(deserializer)?;
        if raw.protocol_version != ProtocolVersion::INITIAL {
            return Err(de::Error::custom(format_args!(
                "unsupported protocol version {}",
                raw.protocol_version
            )));
        }
        if raw.catalog_version != CatalogVersion::INITIAL {
            return Err(de::Error::custom(format_args!(
                "unsupported catalog version {}",
                raw.catalog_version
            )));
        }
        Ok(Self {
            protocol_version: raw.protocol_version,
            catalog_version: raw.catalog_version,
            message: raw.message,
        })
    }
}
