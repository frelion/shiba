-- M10.4 database-local ingress authority. OID is publication identity; name,
-- semantic flags, and columns are a frozen drift-detection snapshot only.

ALTER TABLE shiba_internal.source_binding
    ADD CONSTRAINT source_binding_source_kind_sub_unique UNIQUE (
        source_id, binding_kind, address_objsubid
    );

CREATE TABLE shiba_internal.source_ingress_config (
    source_id bigint PRIMARY KEY CHECK (source_id > 0),
    database_oid oid NOT NULL CHECK (database_oid > 0),
    source_binding_kind text NOT NULL DEFAULT 'relation'
        CHECK (source_binding_kind = 'relation'),
    source_binding_objsubid integer NOT NULL DEFAULT 0
        CHECK (source_binding_objsubid = 0),
    publication_classid oid NOT NULL DEFAULT 'pg_publication'::regclass
        CHECK (publication_classid = 'pg_publication'::regclass),
    publication_objid oid NOT NULL CHECK (publication_objid > 0),
    publication_objsubid integer NOT NULL DEFAULT 0
        CHECK (publication_objsubid = 0),
    publication_name name NOT NULL,
    publication_insert boolean NOT NULL CHECK (publication_insert),
    publication_update boolean NOT NULL CHECK (publication_update),
    publication_delete boolean NOT NULL CHECK (publication_delete),
    publication_truncate boolean NOT NULL,
    publication_via_root boolean NOT NULL CHECK (NOT publication_via_root),
    publication_attnums smallint[] NOT NULL,
    slot_name name NOT NULL UNIQUE,
    slot_generation bigint NOT NULL CHECK (slot_generation > 0),
    CONSTRAINT source_ingress_bound_source FOREIGN KEY (
        source_id, source_binding_kind, source_binding_objsubid
    ) REFERENCES shiba_internal.source_binding (
        source_id, binding_kind, address_objsubid
    ),
    CONSTRAINT source_ingress_publication_address UNIQUE (
        source_id, publication_classid,
        publication_objid, publication_objsubid
    )
);

CREATE TABLE shiba_internal.source_ingress_invalidation (
    source_id bigint PRIMARY KEY,
    publication_classid oid NOT NULL,
    publication_objid oid NOT NULL,
    publication_objsubid integer NOT NULL,
    CONSTRAINT source_ingress_invalidation_exact_config FOREIGN KEY (
        source_id, publication_classid,
        publication_objid, publication_objsubid
    ) REFERENCES shiba_internal.source_ingress_config (
        source_id, publication_classid,
        publication_objid, publication_objsubid
    )
);

REVOKE ALL ON TABLE shiba_internal.source_ingress_config FROM PUBLIC;
REVOKE ALL ON TABLE shiba_internal.source_ingress_invalidation FROM PUBLIC;

CREATE FUNCTION shiba_internal.rotate_source_ingress_slot(
    requested_source_id bigint, expected_generation bigint, requested_slot name
)
RETURNS void LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
    configured_database oid;
    configured_slot name;
    actual_generation bigint;
BEGIN
    SELECT database_oid, slot_name, slot_generation
    INTO STRICT configured_database, configured_slot, actual_generation
    FROM shiba_internal.source_ingress_config
    WHERE source_id = requested_source_id FOR UPDATE;
    IF actual_generation <> expected_generation THEN
        RAISE EXCEPTION 'stale source ingress generation';
    END IF;
    IF EXISTS (SELECT 1 FROM shiba_internal.source_ingress_invalidation
               WHERE source_id = requested_source_id) THEN
        RAISE EXCEPTION 'source ingress publication is invalidated';
    END IF;
    IF configured_slot = requested_slot THEN
        RAISE EXCEPTION 'replacement slot must differ';
    END IF;
    IF EXISTS (
        SELECT 1 FROM shiba_internal.source_continuation
        WHERE source_id = requested_source_id
        UNION ALL
        SELECT 1 FROM shiba_internal.source_row_state
        WHERE source_id = requested_source_id
    ) THEN
        RAISE EXCEPTION 'source rebuild required before slot rotation';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots
               WHERE slot_name = configured_slot AND active) THEN
        RAISE EXCEPTION 'configured slot has an active receiver';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_replication_slots AS slot
        WHERE slot.slot_name = requested_slot
          AND slot.slot_type = 'logical' AND slot.plugin = 'pgoutput'
          AND slot.datoid = configured_database
          AND NOT slot.temporary AND NOT slot.active
    ) THEN
        RAISE EXCEPTION 'replacement slot must be inactive pgoutput in the configured database';
    END IF;
    UPDATE shiba_internal.source_ingress_config
    SET slot_name = requested_slot, slot_generation = expected_generation + 1
    WHERE source_id = requested_source_id
      AND slot_generation = expected_generation;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'stale source ingress generation';
    END IF;
END
$function$;

REVOKE ALL ON FUNCTION shiba_internal.rotate_source_ingress_slot(
    bigint, bigint, name
) FROM PUBLIC;
COMMENT ON TABLE shiba_internal.source_ingress_config IS
    'Exact publication OID plus transport locator and frozen semantic snapshot';
COMMENT ON TABLE shiba_internal.source_ingress_invalidation IS
    'Persistent exact publication-address invalidation authority';
