-- M11.3 retires only an exact, pre-active bootstrap attempt after its physical
-- slot is already absent, then reserves one new attempt atomically.

CREATE FUNCTION shiba_internal.replace_pristine_source_bootstrap(
    old_bootstrap_id bigint,
    requested_source_id bigint,
    old_slot name,
    old_generation bigint,
    new_bootstrap_id bigint,
    requested_publication oid,
    new_slot name,
    new_generation bigint
)
RETURNS void
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
    current_phase text;
BEGIN
    IF old_bootstrap_id = new_bootstrap_id
       OR new_generation <= old_generation THEN
        RAISE EXCEPTION 'replacement identity must advance';
    END IF;

    SELECT bootstrap.phase INTO STRICT current_phase
    FROM shiba_internal.source_binding AS binding
    JOIN shiba_internal.source_bootstrap AS bootstrap
      ON bootstrap.source_id = binding.source_id
    JOIN shiba_internal.source_ingress_config AS config
      ON config.source_id = bootstrap.source_id
     AND config.slot_name = bootstrap.slot_name
     AND config.slot_generation = bootstrap.slot_generation
    WHERE binding.source_id = requested_source_id
      AND binding.binding_kind = 'relation'
      AND binding.address_objsubid = 0
      AND bootstrap.bootstrap_id = old_bootstrap_id
      AND bootstrap.slot_name = old_slot
      AND bootstrap.slot_generation = old_generation
      AND config.publication_objid = requested_publication
    FOR UPDATE OF binding, bootstrap, config;
    IF current_phase NOT IN ('creating', 'scanning', 'cleanup_pending', 'failed') THEN
        RAISE EXCEPTION 'bootstrap attempt is not replaceable';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_replication_slots
        WHERE slot_name = old_slot OR slot_name = new_slot
    ) THEN
        RAISE EXCEPTION 'replacement requires absent old and new slots';
    END IF;
    IF EXISTS (
        SELECT 1 FROM shiba_internal.source_continuation
        WHERE source_id = requested_source_id
    ) THEN
        RAISE EXCEPTION 'replacement requires pristine continuation';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM shiba_internal.operator_definition
        WHERE source_id = requested_source_id
    ) OR EXISTS (
        SELECT 1
        FROM shiba_internal.operator_definition AS definition
        LEFT JOIN shiba_internal.operator_state AS state USING (operator_id)
        LEFT JOIN shiba.operator_result AS result USING (operator_id)
        WHERE definition.source_id = requested_source_id
          AND (state.operator_id IS NULL OR result.operator_id IS NULL
               OR result.result_status <> 'building'
               OR result.value_bigint IS NOT NULL)
    ) THEN
        RAISE EXCEPTION 'replacement requires private building results';
    END IF;

    DELETE FROM shiba_internal.source_row_state
    WHERE source_id = requested_source_id;
    UPDATE shiba_internal.operator_state AS state
    SET value_bigint = 0
    FROM shiba_internal.operator_definition AS definition
    WHERE definition.source_id = requested_source_id
      AND state.operator_id = definition.operator_id;
    -- This normalization is transaction-local and lets the pristine reservation
    -- writer perform its unchanged strict validation before restoring building.
    UPDATE shiba.operator_result AS result
    SET result_status = 'active', value_bigint = 0
    FROM shiba_internal.operator_definition AS definition
    WHERE definition.source_id = requested_source_id
      AND result.operator_id = definition.operator_id;

    DELETE FROM shiba_internal.source_bootstrap
    WHERE source_id = requested_source_id
      AND bootstrap_id = old_bootstrap_id
      AND slot_name = old_slot AND slot_generation = old_generation;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'bootstrap replacement lost exact ownership';
    END IF;
    DELETE FROM shiba_internal.source_ingress_config
    WHERE source_id = requested_source_id
      AND slot_name = old_slot AND slot_generation = old_generation;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'bootstrap replacement lost ingress ownership';
    END IF;

    PERFORM shiba_internal.reserve_source_bootstrap(
        new_bootstrap_id, requested_source_id, requested_publication,
        new_slot, new_generation
    );
END
$function$;

REVOKE ALL ON FUNCTION shiba_internal.replace_pristine_source_bootstrap(
    bigint, bigint, name, bigint, bigint, oid, name, bigint
) FROM PUBLIC;
