-- Sole destructive graph rebuild writer. Validation locks old and target
-- identities before this transaction installs one forward-only building graph.

CREATE FUNCTION shiba_internal.prepare_graph_rebuild(
    requested_graph_id bigint, expected_old_digest bytea,
    expected_old_bootstrap_id bigint, expected_old_relations oid[],
    expected_old_identity_indexes oid[], expected_old_publication oid,
    expected_old_slot name, expected_old_generation bigint,
    new_bootstrap_id bigint, target_source_ids bigint[], target_relations oid[],
    target_identity_indexes oid[], target_publication oid, target_slot name,
    target_generation bigint, target_spec_payload bytea,
    target_graph_payload bytea, target_graph_digest bytea,
    target_result_ids bigint[], target_result_shapes text[],
    target_key_nullable boolean[], target_value_nullable boolean[]
) RETURNS void LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $function$
DECLARE target record; position integer; key_subid integer; payload_subid integer;
BEGIN
    IF pg_catalog.octet_length(target_spec_payload) = 0
       OR pg_catalog.octet_length(target_graph_payload) = 0
       OR pg_catalog.array_length(target_result_ids, 1) IS NULL
       OR pg_catalog.array_length(target_result_shapes, 1)
          <> pg_catalog.array_length(target_result_ids, 1)
       OR pg_catalog.array_length(target_key_nullable, 1)
          <> pg_catalog.array_length(target_result_ids, 1)
       OR pg_catalog.array_length(target_value_nullable, 1)
          <> pg_catalog.array_length(target_result_ids, 1)
       OR (SELECT count(DISTINCT result_id)
           FROM pg_catalog.unnest(target_result_ids) AS result_id)
          <> pg_catalog.array_length(target_result_ids, 1)
    THEN RAISE EXCEPTION 'target graph result contract is invalid'; END IF;
    SELECT * INTO STRICT target FROM shiba_internal.validate_graph_rebuild_current(
        requested_graph_id, expected_old_digest, expected_old_bootstrap_id,
        expected_old_relations, expected_old_identity_indexes,
        expected_old_publication, expected_old_slot, expected_old_generation,
        new_bootstrap_id, target_source_ids, target_relations,
        target_identity_indexes, target_publication, target_slot,
        target_generation, target_graph_digest
    );
    SET CONSTRAINTS ALL DEFERRED;
    DELETE FROM shiba_internal.source_invalidation WHERE source_id = ANY(target_source_ids);
    DELETE FROM shiba_internal.graph_ingress_invalidation
      WHERE graph_id = requested_graph_id;
    DELETE FROM shiba_internal.graph_continuation WHERE graph_id = requested_graph_id;
    DELETE FROM shiba_internal.source_row_state WHERE source_id = ANY(target_source_ids);
    DELETE FROM shiba_internal.graph_node_state WHERE graph_id = requested_graph_id;
    DELETE FROM shiba_internal.graph_result_row WHERE graph_id = requested_graph_id;
    DELETE FROM shiba.graph_result WHERE graph_id = requested_graph_id;
    DELETE FROM shiba_internal.source_binding WHERE source_id = ANY(target_source_ids);
    FOR position IN 1..pg_catalog.array_length(target_source_ids, 1) LOOP
        SELECT (identity.indkey::smallint[])[0]::integer INTO STRICT key_subid
          FROM pg_catalog.pg_index AS identity
          WHERE identity.indexrelid = target_identity_indexes[position]
            AND identity.indrelid = target_relations[position];
        SELECT attribute.attnum::integer INTO STRICT payload_subid
          FROM pg_catalog.pg_attribute AS attribute
          WHERE attribute.attrelid = target_relations[position]
            AND attribute.attnum > 0 AND NOT attribute.attisdropped
            AND attribute.attnum <> key_subid;
        INSERT INTO shiba_internal.source_binding
            (source_id, binding_kind, address_classid, address_objid, address_objsubid)
        VALUES (target_source_ids[position], 'relation', 'pg_class'::regclass,
                target_relations[position], 0),
               (target_source_ids[position], 'column', 'pg_class'::regclass,
                target_relations[position], key_subid),
               (target_source_ids[position], 'column', 'pg_class'::regclass,
                target_relations[position], payload_subid),
               (target_source_ids[position], 'identity_index', 'pg_class'::regclass,
                target_identity_indexes[position], 0);
    END LOOP;
    UPDATE shiba_internal.graph_definition SET spec_payload = target_spec_payload,
        graph_payload = target_graph_payload, graph_digest = target_graph_digest
      WHERE graph_id = requested_graph_id;
    UPDATE shiba_internal.graph_source_member SET graph_digest = target_graph_digest
      WHERE graph_id = requested_graph_id;
    DELETE FROM shiba_internal.graph_ingress_source WHERE graph_id = requested_graph_id;
    UPDATE shiba_internal.graph_ingress_config SET graph_digest = target_graph_digest,
        database_oid = target.database_oid, publication_objid = target_publication,
        publication_name = target.publication_name,
        publication_insert = target.publication_insert,
        publication_update = target.publication_update,
        publication_delete = target.publication_delete,
        publication_truncate = target.publication_truncate,
        publication_via_root = target.publication_via_root,
        slot_name = target_slot, slot_generation = target_generation
      WHERE graph_id = requested_graph_id;
    INSERT INTO shiba_internal.graph_ingress_source
      SELECT requested_graph_id, target_source_ids[numbered.position], CASE
        WHEN member.prattrs IS NULL THEN ARRAY(SELECT attnum::smallint
          FROM pg_catalog.pg_attribute WHERE attrelid = target_relations[numbered.position]
            AND attnum > 0 AND NOT attisdropped ORDER BY attnum)
        ELSE ARRAY(SELECT attnum FROM pg_catalog.unnest(member.prattrs::smallint[])
          AS listed(attnum) ORDER BY attnum) END
      FROM pg_catalog.generate_series(1, pg_catalog.array_length(target_source_ids, 1))
           AS numbered(position)
      JOIN pg_catalog.pg_publication_rel AS member
        ON member.prpubid = target_publication
       AND member.prrelid = target_relations[numbered.position];
    INSERT INTO shiba.graph_result (
        graph_id, result_id, output_shape, output_key_type,
        output_key_nullable, output_value_type, output_value_nullable,
        result_status, value_payload, value_bigint
    ) SELECT requested_graph_id, target_result_ids[numbered.position],
        target_result_shapes[numbered.position],
        CASE target_result_shapes[numbered.position] WHEN 'keyed' THEN 'int8' END,
        target_key_nullable[numbered.position], 'int8',
        target_value_nullable[numbered.position],
        'building', NULL, NULL
      FROM pg_catalog.generate_series(1, pg_catalog.array_length(target_result_ids, 1))
           AS numbered(position);
    UPDATE shiba_internal.graph_bootstrap SET graph_digest = target_graph_digest,
        bootstrap_id = new_bootstrap_id, slot_name = target_slot,
        slot_generation = target_generation, phase = 'rebuild_prepared',
        consistent_point = NULL, fence_token = pg_catalog.gen_random_uuid(),
        catchup_fence_lsn = NULL, activation_end_lsn = NULL,
        retired_bootstrap_id = expected_old_bootstrap_id,
        retired_slot_name = expected_old_slot,
        retired_slot_generation = expected_old_generation
      WHERE graph_id = requested_graph_id;
    UPDATE shiba_internal.graph_bootstrap_checkpoint SET last_batch_ordinal = 0,
        last_source_row_id = NULL, last_batch_digest = NULL
      WHERE graph_id = requested_graph_id;
END
$function$;

REVOKE ALL ON FUNCTION shiba_internal.prepare_graph_rebuild(
    bigint, bytea, bigint, oid[], oid[], oid, name, bigint,
    bigint, bigint[], oid[], oid[], oid, name, bigint, bytea, bytea, bytea,
    bigint[], text[], boolean[], boolean[]
) FROM PUBLIC;
