use shiba_protocol::{GraphId, SlotGeneration, SourceId};

use crate::PgoutputError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgoutputSource {
    pub(crate) source_id: SourceId,
    pub(crate) slot_generation: SlotGeneration,
    pub(crate) relation_id: u32,
    pub(crate) shape: SourceShape,
    pub(crate) relation_identity: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PgoutputGraph {
    pub(crate) graph_id: GraphId,
    pub(crate) slot_generation: SlotGeneration,
    pub(crate) sources: Vec<PgoutputSource>,
}

impl PgoutputGraph {
    /// Builds one singleton graph descriptor without exposing transport fields.
    ///
    /// # Errors
    /// Rejects a zero relation OID.
    pub fn single(graph_id: GraphId, source: PgoutputSource) -> Result<Self, PgoutputError> {
        Self::new(graph_id, source.slot_generation, vec![source])
    }

    /// Builds the exact one- or two-relation descriptor set for one graph slot.
    ///
    /// # Errors
    /// Rejects empty, oversized, duplicate, or mixed-generation descriptors.
    pub fn new(
        graph_id: GraphId,
        slot_generation: SlotGeneration,
        mut sources: Vec<PgoutputSource>,
    ) -> Result<Self, PgoutputError> {
        if sources.is_empty()
            || sources.len() > 2
            || sources.iter().any(|source| source.relation_id == 0)
            || sources
                .iter()
                .any(|source| source.slot_generation != slot_generation)
        {
            return Err(PgoutputError::RelationShape);
        }
        sources.sort_by_key(|source| source.source_id);
        if sources.windows(2).any(|pair| {
            pair[0].source_id == pair[1].source_id || pair[0].relation_id == pair[1].relation_id
        }) {
            return Err(PgoutputError::RelationMismatch);
        }
        Ok(Self {
            graph_id,
            slot_generation,
            sources,
        })
    }

    pub(crate) fn source_for_relation(
        &self,
        relation_id: u32,
    ) -> Result<PgoutputSource, PgoutputError> {
        self.sources
            .iter()
            .copied()
            .find(|source| source.relation_id == relation_id)
            .ok_or(PgoutputError::RelationMismatch)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SourceShape {
    Empty,
    KeyOnly,
    NullableInt8Payload,
    CompositeInt8,
    TextPayload,
}

impl SourceShape {
    pub(crate) const fn columns(self) -> u16 {
        match self {
            Self::Empty => 0,
            Self::KeyOnly => 1,
            Self::NullableInt8Payload | Self::CompositeInt8 | Self::TextPayload => 2,
        }
    }
}

impl PgoutputSource {
    #[must_use]
    pub const fn empty(
        source_id: SourceId,
        slot_generation: SlotGeneration,
        relation_id: u32,
    ) -> Self {
        Self::with_shape(source_id, slot_generation, relation_id, SourceShape::Empty)
    }
    #[must_use]
    pub const fn new(
        source_id: SourceId,
        slot_generation: SlotGeneration,
        relation_id: u32,
    ) -> Self {
        Self::with_shape(
            source_id,
            slot_generation,
            relation_id,
            SourceShape::KeyOnly,
        )
    }
    #[must_use]
    pub const fn with_replica_index(
        source_id: SourceId,
        slot_generation: SlotGeneration,
        relation_id: u32,
    ) -> Self {
        Self {
            source_id,
            slot_generation,
            relation_id,
            shape: SourceShape::KeyOnly,
            relation_identity: b'i',
        }
    }
    #[must_use]
    pub const fn with_nullable_int8_payload(
        source_id: SourceId,
        slot_generation: SlotGeneration,
        relation_id: u32,
    ) -> Self {
        Self::with_shape(
            source_id,
            slot_generation,
            relation_id,
            SourceShape::NullableInt8Payload,
        )
    }
    #[must_use]
    pub const fn with_nullable_int8_payload_replica_index(
        source_id: SourceId,
        slot_generation: SlotGeneration,
        relation_id: u32,
    ) -> Self {
        Self {
            source_id,
            slot_generation,
            relation_id,
            shape: SourceShape::NullableInt8Payload,
            relation_identity: b'i',
        }
    }
    #[must_use]
    pub const fn composite_int8(
        source_id: SourceId,
        slot_generation: SlotGeneration,
        relation_id: u32,
    ) -> Self {
        Self::with_shape(
            source_id,
            slot_generation,
            relation_id,
            SourceShape::CompositeInt8,
        )
    }
    #[must_use]
    pub const fn with_text_payload(
        source_id: SourceId,
        slot_generation: SlotGeneration,
        relation_id: u32,
    ) -> Self {
        Self::with_shape(
            source_id,
            slot_generation,
            relation_id,
            SourceShape::TextPayload,
        )
    }
    const fn with_shape(
        source_id: SourceId,
        slot_generation: SlotGeneration,
        relation_id: u32,
        shape: SourceShape,
    ) -> Self {
        Self {
            source_id,
            slot_generation,
            relation_id,
            shape,
            relation_identity: b'd',
        }
    }
}
