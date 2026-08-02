-- The second component extends admitted composite int8 row identity. Keyless
-- rows remain excluded from source-key uniqueness.

ALTER TABLE shiba_internal.source_row_state
    DROP CONSTRAINT source_row_state_source_row_unique,
    ADD COLUMN source_row_sub_id bigint,
    ADD CONSTRAINT source_row_state_key_shape CHECK (
        source_row_id IS NOT NULL OR source_row_sub_id IS NULL
    );

CREATE UNIQUE INDEX source_row_state_source_row_unique
    ON shiba_internal.source_row_state (
        source_id, source_row_id, source_row_sub_id
    ) NULLS NOT DISTINCT
    WHERE source_row_id IS NOT NULL;

COMMENT ON COLUMN shiba_internal.source_row_state.source_row_sub_id IS
    'Second int8 key component for an admitted composite row identity';
