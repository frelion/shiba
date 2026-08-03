-- Exact active graph/generation/digest/member CAS. Locks remain transaction
-- scoped when the sole destructive prepare writer invokes this function.

CREATE FUNCTION shiba_internal.validate_graph_rebuild_current(
    requested_graph_id bigint, expected_old_digest bytea,
    expected_old_bootstrap_id bigint, expected_old_relations oid[],
    expected_old_identity_indexes oid[], expected_old_publication oid,
    expected_old_slot name, expected_old_generation bigint,
    new_bootstrap_id bigint, target_source_ids bigint[],
    target_relations oid[], target_identity_indexes oid[],
    target_publication oid, target_slot name, target_generation bigint,
    target_graph_digest bytea
) RETURNS TABLE (
    database_oid oid, publication_name name,
    publication_insert boolean, publication_update boolean,
    publication_delete boolean, publication_truncate boolean,
    publication_via_root boolean
) LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $function$
DECLARE target record; actual_relations oid[]; actual_indexes oid[];
BEGIN
    IF requested_graph_id <= 0 OR new_bootstrap_id <= 0
       OR new_bootstrap_id = expected_old_bootstrap_id
       OR target_slot = expected_old_slot
       OR target_generation <> expected_old_generation + 1
       OR pg_catalog.octet_length(expected_old_digest) <> 32
       OR pg_catalog.octet_length(target_graph_digest) <> 32
    THEN RAISE EXCEPTION 'rebuild identity must advance exactly once'; END IF;
    SELECT * INTO STRICT target FROM shiba_internal.validate_graph_rebuild_target(
        requested_graph_id, target_source_ids, target_relations,
        target_identity_indexes, target_publication, target_slot
    );
    PERFORM pg_catalog.pg_advisory_xact_lock(
        '-4611686018427387904'::bigint + requested_graph_id
    );
    PERFORM definition.graph_id FROM shiba_internal.graph_definition AS definition
      WHERE definition.graph_id = requested_graph_id
        AND definition.graph_digest = expected_old_digest FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'stale graph definition digest'; END IF;
    PERFORM bootstrap.graph_id FROM shiba_internal.graph_bootstrap AS bootstrap
      JOIN shiba_internal.graph_ingress_config AS config USING (graph_id)
      WHERE bootstrap.graph_id = requested_graph_id
        AND bootstrap.phase = 'active'
        AND bootstrap.bootstrap_id = expected_old_bootstrap_id
        AND bootstrap.slot_name = expected_old_slot
        AND bootstrap.slot_generation = expected_old_generation
        AND bootstrap.graph_digest = expected_old_digest
        AND config.graph_digest = expected_old_digest
        AND config.database_oid = target.database_oid
        AND config.publication_objid = expected_old_publication
        AND config.slot_name = expected_old_slot
        AND config.slot_generation = expected_old_generation
      FOR UPDATE OF bootstrap, config;
    IF NOT FOUND THEN RAISE EXCEPTION 'stale graph lifecycle identity'; END IF;
    IF NOT shiba_internal.graph_rebuild_slot_is_exact(
        expected_old_slot, target.database_oid
    ) THEN RAISE EXCEPTION 'old slot is not exact inactive pgoutput authority'; END IF;
    -- A durable old-generation invalidation is an admission reason, not a
    -- target identity. Exact old digest/lifecycle/binding/slot CAS above still
    -- owns retirement; prepare clears those old invalidations atomically only
    -- after the side-effect-free target preflight has succeeded.
    SELECT array_agg(relation.address_objid ORDER BY member.input_ordinal),
           array_agg(identity_index.address_objid ORDER BY member.input_ordinal)
      INTO actual_relations, actual_indexes
      FROM shiba_internal.graph_source_member AS member
      JOIN shiba_internal.source_binding AS relation
        ON relation.source_id = member.source_id
       AND relation.binding_kind = 'relation' AND relation.address_objsubid = 0
      JOIN shiba_internal.source_binding AS identity_index
        ON identity_index.source_id = member.source_id
       AND identity_index.binding_kind = 'identity_index'
       AND identity_index.address_objsubid = 0
      WHERE member.graph_id = requested_graph_id;
    IF actual_relations IS DISTINCT FROM expected_old_relations
       OR actual_indexes IS DISTINCT FROM expected_old_identity_indexes
    THEN RAISE EXCEPTION 'stale graph member binding identity'; END IF;
    IF NOT EXISTS (SELECT 1 FROM shiba.graph_result
                   WHERE graph_id = requested_graph_id)
       OR EXISTS (SELECT 1 FROM shiba.graph_result
                  WHERE graph_id = requested_graph_id AND result_status <> 'active')
    THEN RAISE EXCEPTION 'stale graph result identity'; END IF;
    database_oid := target.database_oid;
    publication_name := target.publication_name;
    publication_insert := target.publication_insert;
    publication_update := target.publication_update;
    publication_delete := target.publication_delete;
    publication_truncate := target.publication_truncate;
    publication_via_root := target.publication_via_root;
    RETURN NEXT;
END
$function$;

REVOKE ALL ON FUNCTION shiba_internal.validate_graph_rebuild_current(
    bigint, bytea, bigint, oid[], oid[], oid, name, bigint,
    bigint, bigint[], oid[], oid[], oid, name, bigint, bytea
) FROM PUBLIC;
