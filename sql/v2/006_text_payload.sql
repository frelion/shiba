-- Text payload extends the existing current source-row state authority without
-- adding a payload cache or a second row-state writer.

ALTER TABLE shiba_internal.source_row_state
    ADD COLUMN payload_text text,
    ADD CONSTRAINT source_row_state_payload_type CHECK (
        (payload_present OR payload_text IS NULL)
        AND (payload_int8 IS NULL OR payload_text IS NULL)
    );

COMMENT ON COLUMN shiba_internal.source_row_state.payload_text IS
    'M5.1 exact text payload retained when pgoutput emits unchanged TOAST';
