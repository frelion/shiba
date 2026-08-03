-- Pure target-side admission.  It acquires a transaction-scoped relation lock
-- and returns the exact publication snapshot consumed by the prepare writer.

CREATE FUNCTION shiba_internal.validate_source_rebuild_target(
    target_relation regclass,
    target_identity_index regclass,
    target_publication oid,
    target_slot name
)
RETURNS TABLE (
    database_oid oid, publication_name name,
    publication_insert boolean, publication_update boolean,
    publication_delete boolean, publication_truncate boolean,
    publication_via_root boolean, publication_attnums smallint[]
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
BEGIN
    -- relation_size opens the exact regclass under AccessShareLock for this tx.
    PERFORM pg_catalog.pg_relation_size(target_relation);
    PERFORM pg_catalog.pg_relation_size(target_identity_index);
    IF NOT pg_catalog.has_table_privilege(
        session_user, target_relation, 'SELECT'
    ) THEN
        RAISE EXCEPTION 'rebuild caller lacks SELECT on target relation';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class AS relation
        WHERE relation.oid = target_relation AND relation.relkind = 'r'
          AND relation.relreplident = 'd'
    ) OR 2 <> (
        SELECT count(*) FROM pg_catalog.pg_attribute AS attribute
        WHERE attribute.attrelid = target_relation
          AND attribute.attnum > 0 AND NOT attribute.attisdropped
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute AS key
        JOIN pg_catalog.pg_attribute AS payload
          ON payload.attrelid = key.attrelid AND payload.attnum = 2
        WHERE key.attrelid = target_relation AND key.attnum = 1
          AND key.atttypid = 20 AND key.attnotnull
          AND payload.atttypid = 20 AND NOT payload.attnotnull
          AND key.attgenerated = '' AND payload.attgenerated = ''
    ) THEN
        RAISE EXCEPTION 'target must be ordinary (int8 NOT NULL, int8 NULL)';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_index AS identity
        WHERE identity.indexrelid = target_identity_index
          AND identity.indrelid = target_relation
          AND identity.indisprimary AND identity.indisunique
          AND identity.indisvalid AND identity.indisready
          AND identity.indnkeyatts = 1 AND identity.indnatts = 1
          AND (identity.indkey::smallint[])[0] = 1
          AND identity.indexprs IS NULL AND identity.indpred IS NULL
    ) THEN
        RAISE EXCEPTION 'target requires exact default primary-key identity';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_replication_slots
        WHERE slot_name = target_slot
    ) THEN
        RAISE EXCEPTION 'target rebuild slot must be absent';
    END IF;

    SELECT database.oid, publication.pubname,
           publication.pubinsert, publication.pubupdate,
           publication.pubdelete, publication.pubtruncate,
           publication.pubviaroot,
           CASE WHEN member.prattrs IS NULL THEN ARRAY[1::smallint, 2::smallint]
                ELSE member.prattrs::smallint[] END
    INTO STRICT database_oid, publication_name,
                publication_insert, publication_update,
                publication_delete, publication_truncate,
                publication_via_root, publication_attnums
    FROM pg_catalog.pg_database AS database
    CROSS JOIN pg_catalog.pg_publication AS publication
    JOIN pg_catalog.pg_publication_rel AS member
      ON member.prpubid = publication.oid
    WHERE database.datname = pg_catalog.current_database()
      AND publication.oid = target_publication
      AND NOT publication.puballtables
      AND publication.pubinsert AND publication.pubupdate
      AND publication.pubdelete AND NOT publication.pubtruncate
      AND NOT publication.pubviaroot
      AND member.prrelid = target_relation AND member.prqual IS NULL
      AND (member.prattrs IS NULL
           OR member.prattrs::smallint[] = ARRAY[1::smallint, 2::smallint])
      AND 1 = (SELECT count(*) FROM pg_catalog.pg_publication_rel
               WHERE prpubid = target_publication);
    RETURN NEXT;
END
$function$;

REVOKE ALL ON FUNCTION shiba_internal.validate_source_rebuild_target(
    regclass, regclass, oid, name
) FROM PUBLIC;
