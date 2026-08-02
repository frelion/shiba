use shiba_protocol::{SlotGeneration, SourceId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgoutputSource {
    pub(crate) source_id: SourceId,
    pub(crate) slot_generation: SlotGeneration,
    pub(crate) relation_id: u32,
    pub(crate) shape: SourceShape,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SourceShape {
    Empty,
    KeyOnly,
    NullableInt8Payload,
    CompositeInt8,
}

impl SourceShape {
    pub(crate) const fn columns(self) -> u16 {
        match self {
            Self::Empty => 0,
            Self::KeyOnly => 1,
            Self::NullableInt8Payload | Self::CompositeInt8 => 2,
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
        }
    }
}
