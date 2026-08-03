-- The only destructive rebuild writer.  Validation locks the exact old and
-- target identities before this transaction installs one building authority.

CREATE FUNCTION shiba_internal.prepare_source_rebuild(
    requested_source_id bigint, expected_old_bootstrap_id bigint,
    expected_old_relation oid, expected_old_identity_index oid,
    expected_old_publication oid, expected_old_slot name,
    expected_old_generation bigint, expected_count_operator_id bigint,
    expected_sum_operator_id bigint, expected_sum_input_subid integer,
    new_bootstrap_id bigint, target_relation regclass,
    target_identity_index regclass, target_publication oid,
    target_slot name, target_generation bigint
)
RETURNS void
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE target record;
BEGIN
    SELECT * INTO STRICT target
    FROM shiba_internal.validate_source_rebuild_current(
        requested_source_id, expected_old_bootstrap_id,
        expected_old_relation, expected_old_identity_index,
        expected_old_publication, expected_old_slot,
        expected_old_generation, expected_count_operator_id,
        expected_sum_operator_id, expected_sum_input_subid,
        new_bootstrap_id, target_relation, target_identity_index,
        target_publication, target_slot, target_generation
    );
    SET CONSTRAINTS shiba_internal.source_ingress_bound_source,
                    shiba_internal.source_bootstrap_exact_ingress DEFERRED;
    DELETE FROM shiba_internal.source_invalidation
    WHERE source_id = requested_source_id;
    DELETE FROM shiba_internal.source_ingress_invalidation
    WHERE source_id = requested_source_id;
    DELETE FROM shiba_internal.source_continuation
    WHERE source_id = requested_source_id;
    DELETE FROM shiba_internal.source_row_state
    WHERE source_id = requested_source_id;
    DELETE FROM shiba_internal.source_binding
    WHERE source_id = requested_source_id;
    INSERT INTO shiba_internal.source_binding
        (source_id, binding_kind, address_classid, address_objid, address_objsubid)
    VALUES
        (requested_source_id, 'relation', 'pg_class'::regclass, target_relation, 0),
        (requested_source_id, 'column', 'pg_class'::regclass, target_relation, 1),
        (requested_source_id, 'column', 'pg_class'::regclass, target_relation, 2);
    UPDATE shiba_internal.source_ingress_config SET
        database_oid = target.database_oid,
        publication_objid = target_publication,
        publication_name = target.publication_name,
        publication_insert = target.publication_insert,
        publication_update = target.publication_update,
        publication_delete = target.publication_delete,
        publication_truncate = target.publication_truncate,
        publication_via_root = target.publication_via_root,
        publication_attnums = target.publication_attnums,
        slot_name = target_slot, slot_generation = target_generation
    WHERE source_id = requested_source_id;
    UPDATE shiba_internal.operator_definition SET
        input_objid = target_relation, input_objsubid = 2
    WHERE source_id = requested_source_id
      AND operator_id = expected_sum_operator_id;
    UPDATE shiba_internal.operator_state SET value_bigint = 0
    WHERE operator_id IN (expected_count_operator_id, expected_sum_operator_id);
    UPDATE shiba.operator_result SET result_status = 'building', value_bigint = NULL
    WHERE operator_id IN (expected_count_operator_id, expected_sum_operator_id);
    UPDATE shiba_internal.source_bootstrap SET
        bootstrap_id = new_bootstrap_id, slot_name = target_slot,
        slot_generation = target_generation, phase = 'rebuild_prepared',
        consistent_point = NULL, last_batch_ordinal = 0,
        last_source_row_id = NULL, last_batch_digest = NULL,
        fence_token = pg_catalog.gen_random_uuid(), catchup_fence_lsn = NULL,
        activation_end_lsn = NULL,
        retired_bootstrap_id = expected_old_bootstrap_id,
        retired_slot_name = expected_old_slot,
        retired_slot_generation = expected_old_generation
    WHERE source_id = requested_source_id;
END
$function$;

REVOKE ALL ON FUNCTION shiba_internal.prepare_source_rebuild(
    bigint, bigint, oid, oid, oid, name, bigint, bigint,
    bigint, integer, bigint, regclass, regclass, oid, name, bigint
) FROM PUBLIC;
