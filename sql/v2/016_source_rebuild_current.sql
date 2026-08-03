-- Exact active-generation CAS and operator-plan preflight.  Locks acquired here
-- remain transaction scoped when the sole destructive writer invokes it.

CREATE FUNCTION shiba_internal.validate_source_rebuild_current(
    requested_source_id bigint, expected_old_bootstrap_id bigint,
    expected_old_relation oid, expected_old_identity_index oid,
    expected_old_publication oid, expected_old_slot name,
    expected_old_generation bigint, expected_count_operator_id bigint,
    expected_sum_operator_id bigint, expected_sum_input_subid integer,
    new_bootstrap_id bigint, target_relation regclass,
    target_identity_index regclass, target_publication oid,
    target_slot name, target_generation bigint
)
RETURNS TABLE (
    database_oid oid, publication_name name,
    publication_insert boolean, publication_update boolean,
    publication_delete boolean, publication_truncate boolean,
    publication_via_root boolean, publication_attnums smallint[]
)
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE target record;
BEGIN
    IF requested_source_id <= 0 OR expected_sum_input_subid <= 0
       OR new_bootstrap_id <= 0 OR new_bootstrap_id = expected_old_bootstrap_id
       OR target_slot = expected_old_slot
       OR target_generation <> expected_old_generation + 1 THEN
        RAISE EXCEPTION 'rebuild identity must advance exactly once';
    END IF;
    SELECT * INTO STRICT target
    FROM shiba_internal.validate_source_rebuild_target(
        target_relation, target_identity_index, target_publication, target_slot
    );
    PERFORM pg_catalog.pg_advisory_xact_lock(
        '-9223372036854775808'::bigint + requested_source_id
    );
    PERFORM 1
    FROM shiba_internal.source_bootstrap AS bootstrap
    JOIN shiba_internal.source_ingress_config AS config
      ON config.source_id = bootstrap.source_id
     AND config.slot_name = bootstrap.slot_name
     AND config.slot_generation = bootstrap.slot_generation
    WHERE bootstrap.source_id = requested_source_id
      AND bootstrap.phase = 'active'
      AND bootstrap.bootstrap_id = expected_old_bootstrap_id
      AND bootstrap.slot_name = expected_old_slot
      AND bootstrap.slot_generation = expected_old_generation
      AND config.database_oid = target.database_oid
      AND config.publication_objid = expected_old_publication
      AND config.slot_name = expected_old_slot
      AND config.slot_generation = expected_old_generation
    FOR UPDATE OF bootstrap, config;
    IF NOT FOUND THEN RAISE EXCEPTION 'stale active rebuild identity'; END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_replication_slots AS slot
        WHERE slot.slot_name = expected_old_slot
          AND slot.slot_type = 'logical' AND slot.plugin = 'pgoutput'
          AND slot.datoid = target.database_oid AND NOT slot.temporary
          AND NOT slot.active AND NOT slot.two_phase
          AND NOT slot.failover AND NOT slot.synced
    ) THEN
        RAISE EXCEPTION 'old slot is not exact inactive pgoutput authority';
    END IF;
    IF 3 <> (SELECT count(*) FROM shiba_internal.source_binding
             WHERE source_id = requested_source_id)
       OR NOT EXISTS (
           SELECT 1 FROM shiba_internal.source_binding
           WHERE source_id = requested_source_id AND binding_kind = 'relation'
             AND address_objid = expected_old_relation AND address_objsubid = 0
       ) OR 2 <> (
           SELECT count(*) FROM shiba_internal.source_binding
           WHERE source_id = requested_source_id AND binding_kind = 'column'
             AND address_objid = expected_old_relation
             AND address_objsubid IN (1, expected_sum_input_subid)
       ) OR NOT EXISTS (
           SELECT 1 FROM pg_catalog.pg_index AS old_identity
           WHERE old_identity.indexrelid = expected_old_identity_index
             AND old_identity.indrelid = expected_old_relation
             AND old_identity.indisprimary AND old_identity.indisunique
             AND old_identity.indisvalid AND old_identity.indisready
             AND old_identity.indnkeyatts = 1 AND old_identity.indnatts = 1
             AND (old_identity.indkey::smallint[])[0] = 1
             AND old_identity.indexprs IS NULL AND old_identity.indpred IS NULL
       ) THEN RAISE EXCEPTION 'stale source binding identity'; END IF;
    IF EXISTS (
        SELECT 1 FROM shiba_internal.source_binding
        WHERE source_id <> requested_source_id
          AND address_classid = 'pg_class'::regclass
          AND address_objid = target_relation
    ) THEN RAISE EXCEPTION 'target objects are bound by another source'; END IF;
    IF 2 <> (SELECT count(*) FROM shiba_internal.operator_definition
             WHERE source_id = requested_source_id)
       OR NOT EXISTS (
           SELECT 1 FROM shiba_internal.operator_definition AS definition
           JOIN shiba_internal.operator_state AS state USING (operator_id)
           JOIN shiba.operator_result AS result USING (operator_id, operator_kind)
           WHERE definition.source_id = requested_source_id
             AND definition.operator_id = expected_count_operator_id
             AND definition.operator_kind = 'count_rows'
             AND result.result_status = 'active'
       ) OR NOT EXISTS (
           SELECT 1 FROM shiba_internal.operator_definition AS definition
           JOIN shiba_internal.operator_state AS state USING (operator_id)
           JOIN shiba.operator_result AS result USING (operator_id, operator_kind)
           WHERE definition.source_id = requested_source_id
             AND definition.operator_id = expected_sum_operator_id
             AND definition.operator_kind = 'sum_int8'
             AND definition.input_classid = 'pg_class'::regclass
             AND definition.input_objid = expected_old_relation
             AND definition.input_objsubid = expected_sum_input_subid
             AND result.result_status = 'active'
       ) THEN RAISE EXCEPTION 'stale operator plan identity'; END IF;
    database_oid := target.database_oid;
    publication_name := target.publication_name;
    publication_insert := target.publication_insert;
    publication_update := target.publication_update;
    publication_delete := target.publication_delete;
    publication_truncate := target.publication_truncate;
    publication_via_root := target.publication_via_root;
    publication_attnums := target.publication_attnums;
    RETURN NEXT;
END
$function$;

REVOKE ALL ON FUNCTION shiba_internal.validate_source_rebuild_current(
    bigint, bigint, oid, oid, oid, name, bigint, bigint,
    bigint, integer, bigint, regclass, regclass, oid, name, bigint
) FROM PUBLIC;
