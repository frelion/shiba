-- M4.1 extends the existing Apply fact without changing its identity.
-- The runtime remains the sole writer inside the source transaction commit.

ALTER TABLE shiba_internal.applied_insert
    ADD COLUMN payload_present boolean NOT NULL DEFAULT false,
    ADD COLUMN payload_int8 bigint,
    ADD CONSTRAINT applied_insert_payload_shape CHECK (
        payload_present OR payload_int8 IS NULL
    );

COMMENT ON COLUMN shiba_internal.applied_insert.payload_present IS
    'Distinguishes an absent source payload column from a present SQL NULL';
COMMENT ON COLUMN shiba_internal.applied_insert.payload_int8 IS
    'M4.1 nullable int8 payload decoded from the admitted source relation';
