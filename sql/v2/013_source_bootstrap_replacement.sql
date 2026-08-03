-- Exact recovery of a pristine pre-active graph reservation. It reuses the
-- sole reservation writer and records one forward-only retired identity.

CREATE FUNCTION shiba_internal.replace_pristine_graph_bootstrap(
    old_bootstrap_id bigint, requested_graph_id bigint,
    old_slot name, old_generation bigint, new_bootstrap_id bigint,
    requested_publication oid, new_slot name, new_generation bigint
) RETURNS void LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $function$
DECLARE actual_phase text;
BEGIN
    IF new_bootstrap_id = old_bootstrap_id OR new_slot = old_slot
       OR new_generation <> old_generation + 1 THEN
        RAISE EXCEPTION 'replacement identity must advance exactly once';
    END IF;
    PERFORM pg_catalog.pg_advisory_xact_lock(
        '-4611686018427387904'::bigint + requested_graph_id
    );
    SELECT bootstrap.phase INTO STRICT actual_phase
      FROM shiba_internal.graph_bootstrap AS bootstrap
      JOIN shiba_internal.graph_ingress_config AS config USING (graph_id)
      WHERE bootstrap.graph_id = requested_graph_id
        AND bootstrap.bootstrap_id = old_bootstrap_id
        AND bootstrap.slot_name = old_slot
        AND bootstrap.slot_generation = old_generation
        AND config.slot_name = old_slot AND config.slot_generation = old_generation
      FOR UPDATE OF bootstrap, config;
    IF actual_phase NOT IN ('creating', 'scanning', 'cleanup_pending', 'failed') THEN
        RAISE EXCEPTION 'replacement requires a pre-active lifecycle';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots
               WHERE slot_name IN (old_slot, new_slot)) THEN
        RAISE EXCEPTION 'replacement requires absent old and new slots';
    END IF;
    IF EXISTS (SELECT 1 FROM shiba_internal.graph_continuation
               WHERE graph_id = requested_graph_id)
       OR EXISTS (
           SELECT 1 FROM shiba_internal.source_row_state AS row_state
           JOIN shiba_internal.graph_source_member AS member USING (source_id)
           WHERE member.graph_id = requested_graph_id
       ) OR EXISTS (
           SELECT 1 FROM shiba_internal.graph_node_state
           WHERE graph_id = requested_graph_id
       ) OR EXISTS (
           SELECT 1 FROM shiba_internal.graph_result_row
           WHERE graph_id = requested_graph_id
       ) OR EXISTS (SELECT 1 FROM shiba.graph_result
                    WHERE graph_id = requested_graph_id
                      AND result_status <> 'building') THEN
        RAISE EXCEPTION 'replacement requires pristine building state';
    END IF;
    DELETE FROM shiba_internal.graph_bootstrap_checkpoint
      WHERE graph_id = requested_graph_id;
    DELETE FROM shiba_internal.graph_bootstrap WHERE graph_id = requested_graph_id;
    DELETE FROM shiba_internal.graph_ingress_source WHERE graph_id = requested_graph_id;
    DELETE FROM shiba_internal.graph_ingress_config WHERE graph_id = requested_graph_id;
    PERFORM shiba_internal.reserve_graph_bootstrap(
        new_bootstrap_id, requested_graph_id, requested_publication,
        new_slot, new_generation
    );
    UPDATE shiba_internal.graph_bootstrap SET
        retired_bootstrap_id = old_bootstrap_id,
        retired_slot_name = old_slot,
        retired_slot_generation = old_generation
      WHERE graph_id = requested_graph_id AND bootstrap_id = new_bootstrap_id;
    IF NOT FOUND THEN RAISE EXCEPTION 'replacement lost successor ownership'; END IF;
END
$function$;

REVOKE ALL ON FUNCTION shiba_internal.replace_pristine_graph_bootstrap(
    bigint, bigint, name, bigint, bigint, oid, name, bigint
) FROM PUBLIC;
