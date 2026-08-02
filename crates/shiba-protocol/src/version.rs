use core::fmt;
use core::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use crate::ProtocolError;

macro_rules! version_type {
    ($name:ident, $label:literal) => {
        #[doc = concat!("A non-zero ", $label, ".")]
        #[derive(
            Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(NonZeroU32);

        impl $name {
            /// The first clean-room version. The product name "V2" is not a
            /// wire or catalog version number.
            pub const INITIAL: Self = Self(NonZeroU32::MIN);

            #[doc = concat!(
                "Creates a non-zero ",
                $label,
                ".\n\n# Errors\n\nReturns [`ProtocolError::ZeroValue`] when `value` is zero."
            )]
            pub const fn new(value: u32) -> Result<Self, ProtocolError> {
                match NonZeroU32::new(value) {
                    Some(value) => Ok(Self(value)),
                    None => Err(ProtocolError::ZeroValue($label)),
                }
            }

            pub const fn get(self) -> u32 {
                self.0.get()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.get().fmt(formatter)
            }
        }
    };
}

version_type!(ProtocolVersion, "protocol version");
version_type!(CatalogVersion, "catalog version");
