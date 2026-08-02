-- M4.2 admits INSERT causes from zero-column relations. Such rows have no
-- source-level key; transaction identity plus input sequence remains durable.

ALTER TABLE shiba_internal.applied_insert
    ALTER COLUMN source_row_id DROP NOT NULL;

COMMENT ON COLUMN shiba_internal.applied_insert.source_row_id IS
    'Stable int8 row key when present; NULL only for an admitted empty tuple';
