-- Side-effect-free graph-wide target preflight. Arrays are ordered by the
-- durable graph input ordinal and contain one or exactly two members.

CREATE FUNCTION shiba_internal.validate_graph_rebuild_target(
    requested_graph_id bigint, target_source_ids bigint[],
    target_relations oid[], target_identity_indexes oid[],
    target_publication oid, target_slot name
) RETURNS TABLE (
    database_oid oid, publication_name name,
    publication_insert boolean, publication_update boolean,
    publication_delete boolean, publication_truncate boolean,
    publication_via_root boolean
) LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $function$
DECLARE member_count integer; position integer; key_subid integer;
BEGIN
    member_count := COALESCE(pg_catalog.array_length(target_source_ids, 1), 0);
    IF member_count NOT IN (1, 2)
       OR pg_catalog.array_length(target_relations, 1) <> member_count
       OR pg_catalog.array_length(target_identity_indexes, 1) <> member_count
       OR target_source_ids <> ARRAY(
           SELECT source_id FROM shiba_internal.graph_source_member
           WHERE graph_id = requested_graph_id ORDER BY input_ordinal
       ) OR (SELECT count(DISTINCT relation_oid)
             FROM pg_catalog.unnest(target_relations) AS relation_oid) <> member_count
    THEN RAISE EXCEPTION 'target graph member coordinates are invalid'; END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots
               WHERE slot_name = target_slot) THEN
        RAISE EXCEPTION 'target graph rebuild slot must be absent';
    END IF;
    FOR position IN 1..member_count LOOP
        PERFORM pg_catalog.pg_relation_size(target_relations[position]);
        PERFORM pg_catalog.pg_relation_size(target_identity_indexes[position]);
        IF NOT pg_catalog.has_table_privilege(
            session_user, target_relations[position], 'SELECT'
        ) THEN RAISE EXCEPTION 'rebuild caller lacks SELECT on target relation'; END IF;
        IF NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_class AS relation
            WHERE relation.oid = target_relations[position]
              AND relation.relkind = 'r' AND relation.relreplident IN ('d', 'i')
        ) OR 2 <> (
            SELECT count(*) FROM pg_catalog.pg_attribute AS attribute
            WHERE attribute.attrelid = target_relations[position]
              AND attribute.attnum > 0 AND NOT attribute.attisdropped
        ) THEN RAISE EXCEPTION 'target member must be ordinary two-int8 relation'; END IF;
        SELECT (identity.indkey::smallint[])[0]::integer INTO STRICT key_subid
          FROM pg_catalog.pg_index AS identity
          WHERE identity.indexrelid = target_identity_indexes[position]
            AND identity.indrelid = target_relations[position]
            AND identity.indisunique AND identity.indisvalid AND identity.indisready
            AND (identity.indisprimary OR identity.indisreplident)
            AND identity.indnkeyatts = 1 AND identity.indnatts = 1
            AND identity.indexprs IS NULL AND identity.indpred IS NULL;
        IF NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_attribute AS key
            WHERE key.attrelid = target_relations[position] AND key.attnum = key_subid
              AND key.atttypid = 20 AND key.attnotnull AND key.attgenerated = ''
        ) OR NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_attribute AS payload
            WHERE payload.attrelid = target_relations[position]
              AND payload.attnum > 0 AND payload.attnum <> key_subid
              AND payload.atttypid = 20 AND NOT payload.attnotnull
              AND payload.attgenerated = ''
        ) THEN RAISE EXCEPTION 'target requires bigint key and nullable bigint payload'; END IF;
        IF EXISTS (
            SELECT 1 FROM shiba_internal.source_binding AS binding
            WHERE binding.source_id <> target_source_ids[position]
              AND binding.address_classid = 'pg_class'::regclass
              AND binding.address_objid IN (
                  target_relations[position], target_identity_indexes[position]
              )
        ) THEN RAISE EXCEPTION 'target object is bound by another source'; END IF;
    END LOOP;
    SELECT database.oid, publication.pubname, publication.pubinsert,
           publication.pubupdate, publication.pubdelete,
           publication.pubtruncate, publication.pubviaroot
      INTO STRICT database_oid, publication_name, publication_insert,
          publication_update, publication_delete,
          publication_truncate, publication_via_root
      FROM pg_catalog.pg_database AS database
      CROSS JOIN pg_catalog.pg_publication AS publication
      WHERE database.datname = pg_catalog.current_database()
        AND publication.oid = target_publication AND NOT publication.puballtables
        AND publication.pubinsert AND publication.pubupdate
        AND publication.pubdelete AND NOT publication.pubtruncate
        AND NOT publication.pubviaroot;
    IF member_count <> (SELECT count(*) FROM pg_catalog.pg_publication_rel
                        WHERE prpubid = target_publication)
       OR EXISTS (
        SELECT relation_oid FROM pg_catalog.unnest(target_relations) AS relation_oid
        EXCEPT SELECT prrelid FROM pg_catalog.pg_publication_rel
               WHERE prpubid = target_publication AND prqual IS NULL
    ) THEN RAISE EXCEPTION 'target publication member set does not match graph'; END IF;
    RETURN NEXT;
END
$function$;

REVOKE ALL ON FUNCTION shiba_internal.validate_graph_rebuild_target(
    bigint, bigint[], oid[], oid[], oid, name
) FROM PUBLIC;
