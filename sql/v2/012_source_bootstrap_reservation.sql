-- Sole pristine graph-bootstrap reservation writer. Slot creation remains a
-- nontransactional replication-protocol step after this durable reservation.

CREATE FUNCTION shiba_internal.reserve_graph_bootstrap(
    requested_bootstrap_id bigint, requested_graph_id bigint,
    requested_publication oid, requested_slot name, requested_generation bigint
) RETURNS void LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $function$
DECLARE current_database_oid oid; configured record; expected_members bigint;
BEGIN
    PERFORM definition.graph_id FROM shiba_internal.graph_definition AS definition
      WHERE definition.graph_id = requested_graph_id FOR UPDATE;
    SELECT count(*) INTO expected_members FROM shiba_internal.graph_source_member
      WHERE graph_id = requested_graph_id;
    IF expected_members NOT IN (1, 2)
       OR EXISTS (SELECT 1 FROM shiba_internal.source_row_state AS row_state
                  JOIN shiba_internal.graph_source_member AS member USING (source_id)
                  WHERE member.graph_id = requested_graph_id)
       OR EXISTS (SELECT 1 FROM shiba_internal.graph_continuation
                  WHERE graph_id = requested_graph_id)
       OR EXISTS (SELECT 1 FROM shiba_internal.source_invalidation AS invalid
                  JOIN shiba_internal.graph_source_member AS member USING (source_id)
                  WHERE member.graph_id = requested_graph_id)
    THEN RAISE EXCEPTION 'bootstrap requires pristine valid graph state'; END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots
               WHERE slot_name = requested_slot) THEN
        RAISE EXCEPTION 'bootstrap slot must not exist before reservation';
    END IF;
    SELECT database.oid INTO STRICT current_database_oid FROM pg_catalog.pg_database AS database
      WHERE database.datname = pg_catalog.current_database();
    SELECT publication.pubname, publication.pubinsert, publication.pubupdate,
           publication.pubdelete, publication.pubtruncate, publication.pubviaroot
      INTO STRICT configured FROM pg_catalog.pg_publication AS publication
      WHERE publication.oid = requested_publication AND NOT publication.puballtables
        AND publication.pubinsert AND publication.pubupdate
        AND publication.pubdelete AND NOT publication.pubviaroot;
    IF expected_members <> (SELECT count(*) FROM pg_catalog.pg_publication_rel
                            WHERE prpubid = requested_publication)
       OR EXISTS (
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

    INSERT INTO shiba_internal.graph_ingress_config (
        graph_id, graph_digest, database_oid, publication_objid, publication_name,
        publication_insert, publication_update, publication_delete,
        publication_truncate, publication_via_root, slot_name, slot_generation
    ) SELECT graph_id, graph_digest, current_database_oid, requested_publication,
             configured.pubname, configured.pubinsert, configured.pubupdate,
             configured.pubdelete, configured.pubtruncate, configured.pubviaroot,
             requested_slot, requested_generation
      FROM shiba_internal.graph_definition WHERE graph_id = requested_graph_id;
    INSERT INTO shiba_internal.graph_ingress_source
        (graph_id, source_id, publication_attnums)
    SELECT member.graph_id, member.source_id,
        CASE WHEN publication_member.prattrs IS NULL THEN ARRAY(
            SELECT attribute.attnum::smallint FROM pg_catalog.pg_attribute AS attribute
            WHERE attribute.attrelid = binding.address_objid
              AND attribute.attnum > 0 AND NOT attribute.attisdropped
            ORDER BY attribute.attnum
        ) ELSE ARRAY(SELECT listed.attnum FROM pg_catalog.unnest(
            publication_member.prattrs::smallint[]) AS listed(attnum) ORDER BY listed.attnum) END
      FROM shiba_internal.graph_source_member AS member
      JOIN shiba_internal.source_binding AS binding ON binding.source_id = member.source_id
       AND binding.binding_kind = 'relation' AND binding.address_objsubid = 0
      JOIN pg_catalog.pg_publication_rel AS publication_member
        ON publication_member.prpubid = requested_publication
       AND publication_member.prrelid = binding.address_objid
      WHERE member.graph_id = requested_graph_id;
    INSERT INTO shiba_internal.graph_bootstrap (
        graph_id, graph_digest, bootstrap_id, slot_name, slot_generation, phase
    ) SELECT graph_id, graph_digest, requested_bootstrap_id, requested_slot,
             requested_generation, 'creating'
      FROM shiba_internal.graph_definition WHERE graph_id = requested_graph_id;
    INSERT INTO shiba_internal.graph_bootstrap_checkpoint (graph_id, source_id)
      SELECT graph_id, source_id FROM shiba_internal.graph_source_member
      WHERE graph_id = requested_graph_id;
    DELETE FROM shiba_internal.graph_result_row
      WHERE graph_id = requested_graph_id;
    UPDATE shiba.graph_result SET result_status = 'building'
      WHERE graph_id = requested_graph_id;
END
$function$;

REVOKE ALL ON FUNCTION shiba_internal.reserve_graph_bootstrap(
    bigint, bigint, oid, name, bigint
) FROM PUBLIC;
