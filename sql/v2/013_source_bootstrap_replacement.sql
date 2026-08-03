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
    m12_attempt boolean;
BEGIN
    IF old_bootstrap_id = new_bootstrap_id
       OR new_generation <= old_generation THEN
        RAISE EXCEPTION 'replacement identity must advance';
    END IF;

    SELECT bootstrap.phase, bootstrap.retired_bootstrap_id IS NOT NULL
    INTO STRICT current_phase, m12_attempt
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
    IF m12_attempt AND (
        new_generation <> old_generation + 1 OR new_slot = old_slot
    ) THEN
        RAISE EXCEPTION 'M12 replacement must be an exact fresh successor';
    END IF;
    IF m12_attempt AND NOT (
        SELECT count(*) = 4
          AND count(*) FILTER (WHERE binding_kind = 'relation') = 1
          AND count(*) FILTER (WHERE binding_kind = 'column') = 2
          AND count(*) FILTER (WHERE binding_kind = 'identity_index') = 1
        FROM shiba_internal.source_binding
        WHERE source_id = requested_source_id
    ) THEN
        RAISE EXCEPTION 'M12 replacement binding identity drifted';
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
               OR state.codec_version <> definition.state_codec_version
               OR result.output_shape <> definition.output_shape
               OR result.result_status <> 'building'
               OR result.value_bigint IS NOT NULL)
    ) THEN
        RAISE EXCEPTION 'replacement requires private building results';
    END IF;

    DELETE FROM shiba_internal.source_row_state
    WHERE source_id = requested_source_id;
    IF EXISTS (
        SELECT 1
        FROM shiba_internal.operator_result_row AS result_row
        JOIN shiba_internal.operator_definition AS definition USING (operator_id)
        WHERE definition.source_id = requested_source_id
    ) THEN
        RAISE EXCEPTION 'replacement requires reset keyed results';
    END IF;

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
    IF m12_attempt THEN
        UPDATE shiba_internal.source_bootstrap SET
            retired_bootstrap_id = old_bootstrap_id,
            retired_slot_name = old_slot,
            retired_slot_generation = old_generation
        WHERE source_id = requested_source_id
          AND bootstrap_id = new_bootstrap_id
          AND slot_name = new_slot AND slot_generation = new_generation
          AND phase = 'creating' AND retired_bootstrap_id IS NULL;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'M12 replacement lost successor ownership';
        END IF;
    END IF;
END
$function$;

REVOKE ALL ON FUNCTION shiba_internal.replace_pristine_source_bootstrap(
    bigint, bigint, name, bigint, bigint, oid, name, bigint
) FROM PUBLIC;
