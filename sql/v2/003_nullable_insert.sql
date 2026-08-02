-- Nullable int8 payload extends the sole current-row state. Runtime and the
-- bootstrap processor remain serialized by the same source ownership fence.

ALTER TABLE shiba_internal.source_row_state
    ADD COLUMN payload_present boolean NOT NULL DEFAULT false,
    ADD COLUMN payload_int8 bigint,
    ADD CONSTRAINT source_row_state_payload_shape CHECK (
        payload_present OR payload_int8 IS NULL
    );

COMMENT ON COLUMN shiba_internal.source_row_state.payload_present IS
    'Distinguishes an absent source payload column from a present SQL NULL';
COMMENT ON COLUMN shiba_internal.source_row_state.payload_int8 IS
    'M4.1 nullable int8 payload decoded from the admitted source relation';
