use core::fmt;
use fmt::Write;

use sha2::{Digest, Sha256};

/// Domain prefix for the only Phase-1 wire digest.
pub const WIRE_DIGEST_DOMAIN: &[u8] = b"shiba.protocol.wire.v1\0";
/// Domain prefix for deterministic bootstrap-batch digests.
pub const BOOTSTRAP_BATCH_DIGEST_DOMAIN: &[u8] = b"shiba.bootstrap.batch.v1\0";

/// A SHA-256 digest of a version-1 canonical wire envelope.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct WireDigest([u8; 32]);

impl WireDigest {
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    #[must_use]
    pub fn to_hex(self) -> String {
        let mut encoded = String::with_capacity(64);
        for byte in self.0 {
            write!(encoded, "{byte:02x}").expect("writing to a String cannot fail");
        }
        encoded
    }

    pub(crate) fn for_canonical_json(canonical_json: &[u8]) -> Self {
        Self::with_domain(WIRE_DIGEST_DOMAIN, canonical_json)
    }

    fn with_domain(domain: &[u8], canonical_json: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(domain);
        hasher.update(canonical_json);
        Self(hasher.finalize().into())
    }
}

impl fmt::Debug for WireDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "WireDigest({self})")
    }
}

impl fmt::Display for WireDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "sha256:{}", self.to_hex())
    }
}

/// SHA-256 digest of one canonical bootstrap batch.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct BootstrapBatchDigest([u8; 32]);

impl BootstrapBatchDigest {
    #[must_use]
    pub fn for_canonical_bytes(bytes: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(BOOTSTRAP_BATCH_DIGEST_DOMAIN);
        hasher.update(bytes);
        Self(hasher.finalize().into())
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    #[must_use]
    pub fn to_hex(self) -> String {
        let mut encoded = String::with_capacity(64);
        for byte in self.0 {
            write!(encoded, "{byte:02x}").expect("writing to a String cannot fail");
        }
        encoded
    }
}

impl fmt::Debug for BootstrapBatchDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "BootstrapBatchDigest({self})")
    }
}

impl fmt::Display for BootstrapBatchDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "sha256:{}", self.to_hex())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_known_vector_and_domains_are_distinct() {
        // Adopted evidence from:
        // /Users/zzhang/Documents/Shiba/crates/shiba-protocol/src/physical.rs
        // old test: sha256_implementation_matches_known_vector_and_domains_are_separate
        // reproduce: cargo test -p shiba-protocol sha256_implementation_matches_known_vector_and_domains_are_separate
        assert_eq!(
            WireDigest::with_domain(b"", b"abc").to_hex(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_ne!(
            WireDigest::with_domain(WIRE_DIGEST_DOMAIN, b"same"),
            WireDigest::with_domain(b"shiba.protocol.wire.v2\0", b"same")
        );
        assert_ne!(
            BootstrapBatchDigest::for_canonical_bytes(b"same").to_hex(),
            WireDigest::with_domain(WIRE_DIGEST_DOMAIN, b"same").to_hex()
        );
        assert_ne!(
            BootstrapBatchDigest::for_canonical_bytes(b"first"),
            BootstrapBatchDigest::for_canonical_bytes(b"second")
        );
    }
}
