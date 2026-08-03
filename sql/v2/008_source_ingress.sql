-- One graph owns one database-local publication, logical slot and generation.
-- Per-member column snapshots are subordinate drift-detection facts.

CREATE TABLE shiba_internal.graph_ingress_config (
    graph_id bigint PRIMARY KEY CHECK (graph_id > 0),
    graph_digest bytea NOT NULL CHECK (pg_catalog.octet_length(graph_digest) = 32),
    database_oid oid NOT NULL CHECK (database_oid > 0),
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
    slot_name name NOT NULL UNIQUE,
    slot_generation bigint NOT NULL CHECK (slot_generation > 0),
    CONSTRAINT graph_ingress_exact_definition FOREIGN KEY (graph_id, graph_digest)
        REFERENCES shiba_internal.graph_definition (graph_id, graph_digest)
        DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT graph_ingress_publication_address UNIQUE (
        publication_classid, publication_objid, publication_objsubid
    ),
    CONSTRAINT graph_ingress_exact_publication UNIQUE (
        graph_id, publication_classid, publication_objid, publication_objsubid
    ),
    CONSTRAINT graph_ingress_slot_generation_unique UNIQUE (
        graph_id, slot_name, slot_generation
    ),
    CONSTRAINT graph_ingress_generation_unique UNIQUE (graph_id, slot_generation)
);

CREATE TABLE shiba_internal.graph_ingress_source (
    graph_id bigint NOT NULL,
    source_id bigint NOT NULL,
    publication_attnums smallint[] NOT NULL,
    CONSTRAINT graph_ingress_source_primary PRIMARY KEY (graph_id, source_id),
    CONSTRAINT graph_ingress_source_config FOREIGN KEY (graph_id)
        REFERENCES shiba_internal.graph_ingress_config (graph_id)
        DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT graph_ingress_source_member FOREIGN KEY (graph_id, source_id)
        REFERENCES shiba_internal.graph_source_member (graph_id, source_id)
        DEFERRABLE INITIALLY IMMEDIATE
);

CREATE TABLE shiba_internal.graph_ingress_invalidation (
    graph_id bigint PRIMARY KEY,
    publication_classid oid NOT NULL,
    publication_objid oid NOT NULL,
    publication_objsubid integer NOT NULL,
    CONSTRAINT graph_ingress_invalidation_exact_config FOREIGN KEY (
        graph_id, publication_classid, publication_objid, publication_objsubid
    ) REFERENCES shiba_internal.graph_ingress_config (
        graph_id, publication_classid, publication_objid, publication_objsubid
    )
);

ALTER TABLE shiba_internal.graph_continuation
    ADD CONSTRAINT graph_continuation_exact_ingress FOREIGN KEY (
        graph_id, slot_generation
    ) REFERENCES shiba_internal.graph_ingress_config (
        graph_id, slot_generation
    ) DEFERRABLE INITIALLY IMMEDIATE;

REVOKE ALL ON TABLE shiba_internal.graph_ingress_config FROM PUBLIC;
REVOKE ALL ON TABLE shiba_internal.graph_ingress_source FROM PUBLIC;
REVOKE ALL ON TABLE shiba_internal.graph_ingress_invalidation FROM PUBLIC;

CREATE FUNCTION shiba_internal.rotate_graph_ingress_slot(
    requested_graph_id bigint, expected_generation bigint, requested_slot name
) RETURNS void LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $function$
DECLARE configured_database oid; configured_slot name; actual_generation bigint;
BEGIN
    SELECT database_oid, slot_name, slot_generation
      INTO STRICT configured_database, configured_slot, actual_generation
      FROM shiba_internal.graph_ingress_config
      WHERE graph_id = requested_graph_id FOR UPDATE;
    IF actual_generation <> expected_generation THEN
        RAISE EXCEPTION 'stale graph ingress generation';
    END IF;
    IF EXISTS (SELECT 1 FROM shiba_internal.graph_ingress_invalidation
               WHERE graph_id = requested_graph_id) THEN
        RAISE EXCEPTION 'graph ingress publication is invalidated';
    END IF;
    IF configured_slot = requested_slot THEN RAISE EXCEPTION 'replacement slot must differ'; END IF;
    IF EXISTS (SELECT 1 FROM shiba_internal.graph_continuation
               WHERE graph_id = requested_graph_id)
       OR EXISTS (
           SELECT 1 FROM shiba_internal.source_row_state AS row_state
           JOIN shiba_internal.graph_source_member AS member USING (source_id)
           WHERE member.graph_id = requested_graph_id
       ) THEN RAISE EXCEPTION 'graph rebuild required before slot rotation'; END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots
               WHERE slot_name = configured_slot AND active) THEN
        RAISE EXCEPTION 'configured slot has an active receiver';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots AS slot
        WHERE slot.slot_name = requested_slot AND slot.slot_type = 'logical'
          AND slot.plugin = 'pgoutput' AND slot.datoid = configured_database
          AND NOT slot.temporary AND NOT slot.active) THEN
        RAISE EXCEPTION 'replacement slot must be inactive pgoutput in configured database';
    END IF;
    UPDATE shiba_internal.graph_ingress_config SET slot_name = requested_slot,
        slot_generation = expected_generation + 1
    WHERE graph_id = requested_graph_id AND slot_generation = expected_generation;
    IF NOT FOUND THEN RAISE EXCEPTION 'stale graph ingress generation'; END IF;
END
$function$;

REVOKE ALL ON FUNCTION shiba_internal.rotate_graph_ingress_slot(
    bigint, bigint, name
) FROM PUBLIC;
