-- Keyless admitted rows use only the table's generated internal state identity;
-- no WAL or bootstrap transaction identity is fabricated as a source key.

ALTER TABLE shiba_internal.source_row_state
    ALTER COLUMN source_row_id DROP NOT NULL;

COMMENT ON COLUMN shiba_internal.source_row_state.source_row_id IS
    'Stable int8 row key when present; NULL only for an admitted keyless row';
