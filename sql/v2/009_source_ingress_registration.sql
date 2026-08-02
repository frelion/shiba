-- M10.4 is the sole ingress-definition writer. It resolves the exact OID once
-- and normalizes an unrestricted publication column set to live attnums.

CREATE FUNCTION shiba_internal.configure_source_ingress(
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

    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_replication_slots AS slot
        WHERE slot.slot_name = requested_slot
          AND slot.slot_type = 'logical' AND slot.plugin = 'pgoutput'
          AND slot.datoid = current_database_oid
          AND NOT slot.temporary AND NOT slot.active
    ) THEN
        RAISE EXCEPTION 'slot must be an inactive persistent pgoutput slot in this database';
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
END
$function$;

REVOKE ALL ON FUNCTION shiba_internal.configure_source_ingress(
    bigint, oid, name, bigint
) FROM PUBLIC;
