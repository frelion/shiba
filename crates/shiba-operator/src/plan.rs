use core::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use shiba_protocol::SourceId;

use crate::{ObjectAddress, OperatorId};

pub const PLAN_FORMAT_VERSION: u32 = 1;
pub const STATE_CODEC_VERSION: u32 = 1;
pub const MAX_KEYED_MUTATIONS: usize = 20_000;
const PLAN_DIGEST_DOMAIN: &[u8] = b"shiba.operator.plan.v1\0";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputRole {
    Key,
    Payload,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputBinding {
    pub role: InputRole,
    pub address: ObjectAddress,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueType {
    Int8,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "shape", rename_all = "snake_case", deny_unknown_fields)]
pub enum OutputContract {
    Scalar {
        value_type: ValueType,
    },
    KeyedRows {
        key_type: ValueType,
        value_type: ValueType,
        nullable: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateContract {
    pub codec_version: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PlanImplementation {
    CountRows,
    SumInt8 {
        input: ObjectAddress,
    },
    ProjectRows {
        key: ObjectAddress,
        value: ObjectAddress,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalPlan {
    format_version: u32,
    operator_id: OperatorId,
    source_id: SourceId,
    inputs: Vec<InputBinding>,
    state_contract: StateContract,
    output_contract: OutputContract,
    implementation: PlanImplementation,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompiledPlan {
    pub format_version: u32,
    pub operator_id: OperatorId,
    pub source_id: SourceId,
    pub inputs: Vec<InputBinding>,
    pub state_contract: StateContract,
    pub output_contract: OutputContract,
    pub implementation: PlanImplementation,
    pub canonical_payload: Vec<u8>,
    pub digest: [u8; 32],
}

impl CompiledPlan {
    /// Constructs canonical plan bytes and their domain-separated digest.
    ///
    /// # Errors
    ///
    /// Returns an error if canonical serialization fails.
    pub fn build(
        operator_id: OperatorId,
        source_id: SourceId,
        inputs: Vec<InputBinding>,
        output_contract: OutputContract,
        implementation: PlanImplementation,
    ) -> Result<Self, PlanError> {
        let state_contract = StateContract {
            codec_version: STATE_CODEC_VERSION,
        };
        let canonical = CanonicalPlan {
            format_version: PLAN_FORMAT_VERSION,
            operator_id,
            source_id,
            inputs,
            state_contract,
            output_contract,
            implementation,
        };
        validate_contract(&canonical)?;
        let canonical_payload = serde_json::to_vec(&canonical).map_err(|_| PlanError::Codec)?;
        let digest = digest(&canonical_payload);
        Ok(Self {
            format_version: canonical.format_version,
            operator_id: canonical.operator_id,
            source_id: canonical.source_id,
            inputs: canonical.inputs,
            state_contract: canonical.state_contract,
            output_contract: canonical.output_contract,
            implementation: canonical.implementation,
            canonical_payload,
            digest,
        })
    }

    /// Decodes one exact canonical payload and verifies its supplied digest.
    ///
    /// # Errors
    ///
    /// Rejects malformed/trailing data, unknown fields or versions, structural
    /// contract mismatch, non-canonical bytes, and digest mismatch.
    pub fn from_canonical_payload(
        payload: &[u8],
        expected_digest: [u8; 32],
    ) -> Result<Self, PlanError> {
        let canonical: CanonicalPlan =
            serde_json::from_slice(payload).map_err(|_| PlanError::Codec)?;
        let rebuilt = Self::build(
            canonical.operator_id,
            canonical.source_id,
            canonical.inputs,
            canonical.output_contract,
            canonical.implementation,
        )?;
        if rebuilt.format_version != canonical.format_version
            || rebuilt.state_contract != canonical.state_contract
            || rebuilt.canonical_payload != payload
            || rebuilt.digest != expected_digest
        {
            return Err(PlanError::DigestMismatch);
        }
        Ok(rebuilt)
    }

    /// Recomputes and verifies the complete canonical plan representation.
    ///
    /// # Errors
    ///
    /// Rejects unknown versions and any payload or digest mismatch.
    pub fn validate(&self) -> Result<(), PlanError> {
        if self.format_version != PLAN_FORMAT_VERSION
            || self.state_contract.codec_version != STATE_CODEC_VERSION
        {
            return Err(PlanError::UnsupportedVersion);
        }
        let rebuilt = Self::build(
            self.operator_id,
            self.source_id,
            self.inputs.clone(),
            self.output_contract.clone(),
            self.implementation.clone(),
        )?;
        if rebuilt.canonical_payload != self.canonical_payload || rebuilt.digest != self.digest {
            return Err(PlanError::DigestMismatch);
        }
        Ok(())
    }
}

fn digest(payload: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(PLAN_DIGEST_DOMAIN);
    hasher.update(payload);
    hasher.finalize().into()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlanError {
    Codec,
    UnsupportedVersion,
    DigestMismatch,
    ContractMismatch,
}

impl fmt::Display for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid compiled plan: {self:?}")
    }
}

impl std::error::Error for PlanError {}

fn validate_contract(plan: &CanonicalPlan) -> Result<(), PlanError> {
    let scalar = OutputContract::Scalar {
        value_type: ValueType::Int8,
    };
    let keyed = OutputContract::KeyedRows {
        key_type: ValueType::Int8,
        value_type: ValueType::Int8,
        nullable: true,
    };
    let valid = match plan.implementation {
        PlanImplementation::CountRows => plan.inputs.is_empty() && plan.output_contract == scalar,
        PlanImplementation::SumInt8 { input } => {
            plan.inputs
                == [InputBinding {
                    role: InputRole::Payload,
                    address: input,
                }]
                && plan.output_contract == scalar
        }
        PlanImplementation::ProjectRows { key, value } => {
            plan.inputs
                == [
                    InputBinding {
                        role: InputRole::Key,
                        address: key,
                    },
                    InputBinding {
                        role: InputRole::Payload,
                        address: value,
                    },
                ]
                && plan.output_contract == keyed
        }
    };
    if valid {
        Ok(())
    } else {
        Err(PlanError::ContractMismatch)
    }
}
