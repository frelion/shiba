-- M7.1 binds each admitted source to one PostgreSQL ObjectAddress. Binding
-- creation and DDL invalidation own separate immutable facts with one writer each.

CREATE TABLE shiba_internal.source_binding (
    source_id bigint NOT NULL CHECK (source_id > 0),
    address_classid oid NOT NULL CHECK (address_classid = 'pg_class'::regclass),
    address_objid oid NOT NULL,
    address_objsubid integer NOT NULL CHECK (address_objsubid >= 0),
    PRIMARY KEY (source_id, address_classid, address_objid, address_objsubid),
    CONSTRAINT source_binding_address_unique UNIQUE (
        address_classid, address_objid, address_objsubid
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

REVOKE ALL ON TABLE shiba_internal.source_binding FROM PUBLIC;
REVOKE ALL ON TABLE shiba_internal.source_invalidation FROM PUBLIC;

CREATE FUNCTION shiba_internal.register_source(
    requested_source_id bigint,
    requested_relation regclass
)
RETURNS void
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class
        WHERE oid = requested_relation AND relkind = 'r'
    ) THEN
        RAISE EXCEPTION 'source relation must be an ordinary table';
    END IF;

    INSERT INTO shiba_internal.source_binding (
        source_id, address_classid, address_objid, address_objsubid
    )
    SELECT requested_source_id, 'pg_class'::regclass, requested_relation, 0
    UNION ALL
    SELECT requested_source_id, 'pg_class'::regclass, requested_relation,
           attribute.attnum::integer
    FROM pg_catalog.pg_attribute AS attribute
    WHERE attribute.attrelid = requested_relation
      AND attribute.attnum > 0
      AND NOT attribute.attisdropped;
END
$function$;

REVOKE ALL ON FUNCTION shiba_internal.register_source(bigint, regclass) FROM PUBLIC;

CREATE FUNCTION shiba_internal.invalidate_source_object()
RETURNS event_trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
BEGIN
    IF TG_EVENT = 'ddl_command_end' THEN
        INSERT INTO shiba_internal.source_invalidation (
            source_id, address_classid, address_objid, address_objsubid
        )
        SELECT binding.source_id, command.classid, command.objid, command.objsubid
        FROM pg_catalog.pg_event_trigger_ddl_commands() AS command
        JOIN shiba_internal.source_binding AS binding
          ON (binding.address_classid, binding.address_objid, binding.address_objsubid)
           = (command.classid, command.objid, command.objsubid)
        WHERE NOT command.in_extension
        ON CONFLICT (source_id) DO NOTHING;
    ELSIF TG_EVENT = 'sql_drop' THEN
        INSERT INTO shiba_internal.source_invalidation (
            source_id, address_classid, address_objid, address_objsubid
        )
        SELECT binding.source_id, dropped.classid, dropped.objid, dropped.objsubid
        FROM pg_catalog.pg_event_trigger_dropped_objects() AS dropped
        JOIN shiba_internal.source_binding AS binding
          ON (binding.address_classid, binding.address_objid, binding.address_objsubid)
           = (dropped.classid, dropped.objid, dropped.objsubid)
        WHERE NOT dropped.is_temporary
        ON CONFLICT (source_id) DO NOTHING;
    ELSE
        RAISE EXCEPTION 'unsupported source invalidation event %', TG_EVENT;
    END IF;
END
$function$;

CREATE EVENT TRIGGER shiba_source_ddl_command_end
    ON ddl_command_end
    EXECUTE FUNCTION shiba_internal.invalidate_source_object();

CREATE EVENT TRIGGER shiba_source_sql_drop
    ON sql_drop
    EXECUTE FUNCTION shiba_internal.invalidate_source_object();

COMMENT ON TABLE shiba_internal.source_binding IS
    'Immutable source relation and column ObjectAddress binding authority';
COMMENT ON TABLE shiba_internal.source_invalidation IS
    'Exact ObjectAddress invalidation facts written in the owning DDL transaction';
