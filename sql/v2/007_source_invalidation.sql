-- Source ObjectAddresses remain source facts. Ordered graph membership is the
-- sole association from those facts to one singleton or two-source graph.

CREATE TABLE shiba_internal.source_binding (
    source_id bigint NOT NULL CHECK (source_id > 0),
    binding_kind text NOT NULL,
    address_classid oid NOT NULL CHECK (address_classid = 'pg_class'::regclass),
    address_objid oid NOT NULL,
    address_objsubid integer NOT NULL CHECK (address_objsubid >= 0),
    CONSTRAINT source_binding_kind_address CHECK (
        (binding_kind = 'relation' AND address_objsubid = 0)
        OR (binding_kind = 'column' AND address_objsubid > 0)
        OR (binding_kind = 'identity_index' AND address_objsubid = 0)
    ),
    PRIMARY KEY (source_id, address_classid, address_objid, address_objsubid),
    CONSTRAINT source_binding_address_unique UNIQUE (
        address_classid, address_objid, address_objsubid
    ),
    CONSTRAINT source_binding_source_kind_sub_unique UNIQUE (
        source_id, binding_kind, address_objsubid
    )
);

CREATE TABLE shiba_internal.source_invalidation (
    source_id bigint PRIMARY KEY,
    address_classid oid NOT NULL,
    address_objid oid NOT NULL,
    address_objsubid integer NOT NULL,
    CONSTRAINT source_invalidation_exact_binding FOREIGN KEY (
        address_classid, address_objid, address_objsubid
    ) REFERENCES shiba_internal.source_binding (
        address_classid, address_objid, address_objsubid
    )
);

CREATE TABLE shiba_internal.graph_source_member (
    graph_id bigint NOT NULL CHECK (graph_id > 0),
    source_id bigint NOT NULL CHECK (source_id > 0),
    input_ordinal smallint NOT NULL CHECK (input_ordinal IN (0, 1)),
    graph_digest bytea NOT NULL CHECK (pg_catalog.octet_length(graph_digest) = 32),
    relation_binding_kind text NOT NULL DEFAULT 'relation'
        CHECK (relation_binding_kind = 'relation'),
    relation_binding_objsubid integer NOT NULL DEFAULT 0
        CHECK (relation_binding_objsubid = 0),
    CONSTRAINT graph_source_member_primary PRIMARY KEY (graph_id, source_id),
    CONSTRAINT graph_source_member_ordinal UNIQUE (graph_id, input_ordinal),
    CONSTRAINT graph_source_member_one_graph UNIQUE (source_id),
    CONSTRAINT graph_source_member_exact_graph FOREIGN KEY (graph_id, graph_digest)
        REFERENCES shiba_internal.graph_definition (graph_id, graph_digest)
        DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT graph_source_member_exact_relation FOREIGN KEY (
        source_id, relation_binding_kind, relation_binding_objsubid
    ) REFERENCES shiba_internal.source_binding (
        source_id, binding_kind, address_objsubid
    ) DEFERRABLE INITIALLY IMMEDIATE
);

CREATE FUNCTION shiba_internal.validate_graph_member_cardinality()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE checked_graph bigint := COALESCE(NEW.graph_id, OLD.graph_id);
DECLARE expected smallint;
BEGIN
    SELECT source_count INTO expected FROM shiba_internal.graph_definition
    WHERE graph_id = checked_graph;
    IF FOUND AND (
        (SELECT count(*) FROM shiba_internal.graph_source_member
         WHERE graph_id = checked_graph) <> expected
        OR (SELECT array_agg(input_ordinal ORDER BY input_ordinal)
            FROM shiba_internal.graph_source_member WHERE graph_id = checked_graph)
           <> CASE expected WHEN 1 THEN ARRAY[0::smallint]
                            ELSE ARRAY[0::smallint, 1::smallint] END
    ) THEN RAISE EXCEPTION 'graph source membership is incomplete'; END IF;
    RETURN NULL;
END
$function$;

CREATE CONSTRAINT TRIGGER shiba_graph_member_cardinality
AFTER INSERT OR UPDATE OR DELETE ON shiba_internal.graph_source_member
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION shiba_internal.validate_graph_member_cardinality();

CREATE CONSTRAINT TRIGGER shiba_graph_definition_cardinality
AFTER INSERT OR UPDATE ON shiba_internal.graph_definition
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION shiba_internal.validate_graph_member_cardinality();

REVOKE ALL ON TABLE shiba_internal.source_binding FROM PUBLIC;
REVOKE ALL ON TABLE shiba_internal.source_invalidation FROM PUBLIC;
REVOKE ALL ON TABLE shiba_internal.graph_source_member FROM PUBLIC;
REVOKE ALL ON FUNCTION shiba_internal.validate_graph_member_cardinality() FROM PUBLIC;

CREATE FUNCTION shiba_internal.register_source(
    requested_source_id bigint, requested_relation regclass
) RETURNS void LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $function$
DECLARE effective_identity_indexes oid[];
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class
                   WHERE oid = requested_relation AND relkind = 'r') THEN
        RAISE EXCEPTION 'source relation must be an ordinary table';
    END IF;
    SELECT pg_catalog.array_agg(identity.indexrelid ORDER BY identity.indexrelid)
      INTO effective_identity_indexes
      FROM pg_catalog.pg_class AS relation
      JOIN pg_catalog.pg_index AS identity ON identity.indrelid = relation.oid
      WHERE relation.oid = requested_relation
        AND ((relation.relreplident = 'd' AND identity.indisprimary)
             OR (relation.relreplident = 'i' AND identity.indisreplident))
        AND identity.indisunique AND identity.indisvalid AND identity.indisready
        AND identity.indexprs IS NULL AND identity.indpred IS NULL;
    IF pg_catalog.cardinality(effective_identity_indexes) IS DISTINCT FROM 1 THEN
        RAISE EXCEPTION 'source relation requires exactly one effective identity index';
    END IF;
    INSERT INTO shiba_internal.source_binding
        (source_id, binding_kind, address_classid, address_objid, address_objsubid)
    SELECT requested_source_id, 'relation', 'pg_class'::regclass, requested_relation, 0
    UNION ALL SELECT requested_source_id, 'column', 'pg_class'::regclass,
        requested_relation, attribute.attnum::integer
      FROM pg_catalog.pg_attribute AS attribute
      WHERE attribute.attrelid = requested_relation AND attribute.attnum > 0
        AND NOT attribute.attisdropped
    UNION ALL SELECT requested_source_id, 'identity_index', 'pg_class'::regclass,
        effective_identity_indexes[1], 0;
END
$function$;

REVOKE ALL ON FUNCTION shiba_internal.register_source(bigint, regclass) FROM PUBLIC;
