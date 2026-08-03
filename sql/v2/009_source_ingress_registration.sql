-- Sole graph ingress writer. It accepts only an exact publication member set
-- matching the already-installed canonical graph membership.

CREATE FUNCTION shiba_internal.configure_graph_ingress(
    requested_graph_id bigint, requested_publication oid,
    requested_slot name, requested_generation bigint
) RETURNS void LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $function$
DECLARE current_database_oid oid; configured record; expected_members bigint;
BEGIN
    SELECT database.oid INTO STRICT current_database_oid
      FROM pg_catalog.pg_database AS database
      WHERE database.datname = pg_catalog.current_database();
    PERFORM definition.graph_id FROM shiba_internal.graph_definition AS definition
      WHERE definition.graph_id = requested_graph_id FOR UPDATE;
    SELECT count(*) INTO expected_members
      FROM shiba_internal.graph_source_member WHERE graph_id = requested_graph_id;
    IF expected_members NOT IN (1, 2)
       OR EXISTS (
           SELECT 1 FROM shiba_internal.graph_source_member AS member
           JOIN shiba_internal.source_invalidation AS invalid USING (source_id)
           WHERE member.graph_id = requested_graph_id
       ) THEN RAISE EXCEPTION 'graph membership is incomplete or invalidated'; END IF;
    SELECT publication.pubname, publication.pubinsert, publication.pubupdate,
           publication.pubdelete, publication.pubtruncate, publication.pubviaroot
      INTO STRICT configured
      FROM pg_catalog.pg_publication AS publication
      WHERE publication.oid = requested_publication
        AND NOT publication.puballtables AND publication.pubinsert
        AND publication.pubupdate AND publication.pubdelete
        AND NOT publication.pubviaroot;
    IF expected_members <> (
        SELECT count(*) FROM pg_catalog.pg_publication_rel
        WHERE prpubid = requested_publication
    ) OR EXISTS (
        SELECT 1 FROM shiba_internal.graph_source_member AS member
        JOIN shiba_internal.source_binding AS binding
          ON binding.source_id = member.source_id
         AND binding.binding_kind = 'relation' AND binding.address_objsubid = 0
        LEFT JOIN pg_catalog.pg_publication_rel AS publication_member
          ON publication_member.prpubid = requested_publication
         AND publication_member.prrelid = binding.address_objid
        WHERE member.graph_id = requested_graph_id
          AND (publication_member.prrelid IS NULL OR publication_member.prqual IS NOT NULL)
    ) THEN RAISE EXCEPTION 'publication member set does not match graph'; END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots AS slot
        WHERE slot.slot_name = requested_slot AND slot.slot_type = 'logical'
          AND slot.plugin = 'pgoutput' AND slot.datoid = current_database_oid
          AND NOT slot.temporary AND NOT slot.active
          AND NOT slot.two_phase AND NOT slot.failover AND NOT slot.synced)
    THEN RAISE EXCEPTION 'slot must be exact inactive pgoutput in this database'; END IF;

    INSERT INTO shiba_internal.graph_ingress_config (
        graph_id, graph_digest, database_oid, publication_objid,
        publication_name, publication_insert, publication_update,
        publication_delete, publication_truncate, publication_via_root,
        slot_name, slot_generation
    ) SELECT definition.graph_id, definition.graph_digest, current_database_oid,
             requested_publication, configured.pubname, configured.pubinsert,
             configured.pubupdate, configured.pubdelete, configured.pubtruncate,
             configured.pubviaroot, requested_slot, requested_generation
      FROM shiba_internal.graph_definition AS definition
      WHERE definition.graph_id = requested_graph_id;
    INSERT INTO shiba_internal.graph_ingress_source (
        graph_id, source_id, publication_attnums
    ) SELECT member.graph_id, member.source_id,
        CASE WHEN publication_member.prattrs IS NULL THEN ARRAY(
            SELECT attribute.attnum::smallint FROM pg_catalog.pg_attribute AS attribute
            WHERE attribute.attrelid = binding.address_objid
              AND attribute.attnum > 0 AND NOT attribute.attisdropped
            ORDER BY attribute.attnum
        ) ELSE ARRAY(
            SELECT listed.attnum FROM pg_catalog.unnest(
                publication_member.prattrs::smallint[]) AS listed(attnum)
            ORDER BY listed.attnum
        ) END
      FROM shiba_internal.graph_source_member AS member
      JOIN shiba_internal.source_binding AS binding
        ON binding.source_id = member.source_id
       AND binding.binding_kind = 'relation' AND binding.address_objsubid = 0
      JOIN pg_catalog.pg_publication_rel AS publication_member
        ON publication_member.prpubid = requested_publication
       AND publication_member.prrelid = binding.address_objid
      WHERE member.graph_id = requested_graph_id;
    IF (SELECT count(*) FROM shiba_internal.graph_ingress_source
        WHERE graph_id = requested_graph_id) <> expected_members THEN
        RAISE EXCEPTION 'graph ingress member snapshot is incomplete';
    END IF;
END
$function$;

REVOKE ALL ON FUNCTION shiba_internal.configure_graph_ingress(
    bigint, oid, name, bigint
) FROM PUBLIC;
