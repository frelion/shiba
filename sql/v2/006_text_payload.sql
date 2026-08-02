-- M5.1 adds text payload to the existing current source-row state authority.
-- The runtime remains its sole writer inside the source transaction commit.

ALTER TABLE shiba_internal.applied_insert
    ADD COLUMN payload_text text,
    ADD CONSTRAINT applied_insert_payload_type CHECK (
        (payload_present OR payload_text IS NULL)
        AND (payload_int8 IS NULL OR payload_text IS NULL)
    );

COMMENT ON COLUMN shiba_internal.applied_insert.payload_text IS
    'M5.1 exact text payload retained when pgoutput emits unchanged TOAST';
