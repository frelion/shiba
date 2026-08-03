-- Shared exact old-slot shape check for graph rebuild CAS. Slot DDL remains a
-- trusted replication-control-plane action outside Catalog transactions.

CREATE FUNCTION shiba_internal.graph_rebuild_slot_is_exact(
    requested_slot name, requested_database oid
) RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $function$
    SELECT EXISTS (
        SELECT 1 FROM pg_catalog.pg_replication_slots AS slot
        WHERE slot.slot_name = requested_slot
          AND slot.slot_type = 'logical' AND slot.plugin = 'pgoutput'
          AND slot.datoid = requested_database AND NOT slot.temporary
          AND NOT slot.active AND NOT slot.two_phase
          AND NOT slot.failover AND NOT slot.synced
    )
$function$;

REVOKE ALL ON FUNCTION shiba_internal.graph_rebuild_slot_is_exact(name, oid)
FROM PUBLIC;
