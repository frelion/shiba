-- M4.3 adds the second component of the admitted composite int8 row identity.
-- Empty tuples remain excluded from source-row uniqueness.

ALTER TABLE shiba_internal.applied_insert
    DROP CONSTRAINT applied_insert_source_row_unique,
    ADD COLUMN source_row_sub_id bigint,
    ADD CONSTRAINT applied_insert_key_shape CHECK (
        source_row_id IS NOT NULL OR source_row_sub_id IS NULL
    );

CREATE UNIQUE INDEX applied_insert_source_row_unique
    ON shiba_internal.applied_insert (
        source_id, source_row_id, source_row_sub_id
    ) NULLS NOT DISTINCT
    WHERE source_row_id IS NOT NULL;

COMMENT ON COLUMN shiba_internal.applied_insert.source_row_sub_id IS
    'Second int8 key component for an admitted composite row identity';
