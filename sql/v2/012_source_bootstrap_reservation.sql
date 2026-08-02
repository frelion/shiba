-- M11.1's sole pristine reservation writer freezes source/publication identity,
-- reserves an absent slot name, and hides results in one PostgreSQL transaction.

CREATE FUNCTION shiba_internal.reserve_source_bootstrap(
    requested_bootstrap_id bigint,
    requested_source_id bigint,
    requested_publication oid,
    requested_slot name,
    requested_generation bigint
)
RETURNS void
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
    bound_relation oid;
    current_database_oid oid;
    configured_name name;
    configured_insert boolean;
    configured_update boolean;
    configured_delete boolean;
    configured_truncate boolean;
    configured_via_root boolean;
    configured_attnums smallint[];
BEGIN
    SELECT binding.address_objid INTO STRICT bound_relation
    FROM shiba_internal.source_binding AS binding
    WHERE binding.source_id = requested_source_id
      AND binding.binding_kind = 'relation'
      AND binding.address_objsubid = 0
    FOR UPDATE;
    IF EXISTS (SELECT 1 FROM shiba_internal.source_invalidation
               WHERE source_id = requested_source_id) THEN
        RAISE EXCEPTION 'source is invalidated';
    END IF;
    IF EXISTS (
        SELECT 1 FROM shiba_internal.source_row_state
        WHERE source_id = requested_source_id
        UNION ALL
        SELECT 1 FROM shiba_internal.source_continuation
        WHERE source_id = requested_source_id
    ) THEN
        RAISE EXCEPTION 'bootstrap requires pristine source state';
    END IF;

    SELECT database.oid INTO STRICT current_database_oid
    FROM pg_catalog.pg_database AS database
    WHERE database.datname = pg_catalog.current_database();
    SELECT pubname, pubinsert, pubupdate, pubdelete, pubtruncate, pubviaroot
    INTO STRICT configured_name, configured_insert, configured_update,
                configured_delete, configured_truncate, configured_via_root
    FROM pg_catalog.pg_publication
    WHERE oid = requested_publication AND NOT puballtables
      AND pubinsert AND pubupdate AND pubdelete AND NOT pubviaroot;
    SELECT CASE WHEN member.prattrs IS NULL THEN ARRAY(
               SELECT attribute.attnum::smallint
               FROM pg_catalog.pg_attribute AS attribute
               WHERE attribute.attrelid = bound_relation
                 AND attribute.attnum > 0 AND NOT attribute.attisdropped
               ORDER BY attribute.attnum
           ) ELSE ARRAY(
               SELECT listed.attnum
               FROM pg_catalog.unnest(member.prattrs::smallint[]) AS listed(attnum)
               ORDER BY listed.attnum
           ) END
    INTO STRICT configured_attnums
    FROM pg_catalog.pg_publication_rel AS member
    WHERE member.prpubid = requested_publication
      AND member.prrelid = bound_relation AND member.prqual IS NULL
      AND 1 = (SELECT count(*) FROM pg_catalog.pg_publication_rel
               WHERE prpubid = requested_publication);

    IF EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots
               WHERE slot_name = requested_slot) THEN
        RAISE EXCEPTION 'bootstrap slot must not exist before reservation';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM shiba_internal.operator_definition
                   WHERE source_id = requested_source_id)
       OR EXISTS (
           SELECT 1 FROM shiba_internal.operator_definition AS definition
           LEFT JOIN shiba_internal.operator_state AS state USING (operator_id)
           LEFT JOIN shiba.operator_result AS result USING (operator_id)
           WHERE definition.source_id = requested_source_id
             AND (state.operator_id IS NULL OR state.value_bigint <> 0
                  OR result.operator_id IS NULL OR result.result_status <> 'active'
                  OR result.value_bigint <> 0)
       ) THEN
        RAISE EXCEPTION 'bootstrap requires pristine operator state';
    END IF;

    INSERT INTO shiba_internal.source_ingress_config (
        source_id, database_oid, publication_objid, publication_name,
        publication_insert, publication_update, publication_delete,
        publication_truncate, publication_via_root, publication_attnums,
        slot_name, slot_generation
    ) VALUES (
        requested_source_id, current_database_oid, requested_publication,
        configured_name, configured_insert, configured_update,
        configured_delete, configured_truncate, configured_via_root,
        configured_attnums, requested_slot, requested_generation
    );
    INSERT INTO shiba_internal.source_bootstrap (
        source_id, bootstrap_id, slot_name, slot_generation, phase
    ) VALUES (
        requested_source_id, requested_bootstrap_id,
        requested_slot, requested_generation, 'creating'
    );
    UPDATE shiba.operator_result AS result
    SET result_status = 'building', value_bigint = NULL
    FROM shiba_internal.operator_definition AS definition
    WHERE definition.source_id = requested_source_id
      AND result.operator_id = definition.operator_id;
END
$function$;

REVOKE ALL ON FUNCTION shiba_internal.reserve_source_bootstrap(
    bigint, bigint, oid, name, bigint
) FROM PUBLIC;
