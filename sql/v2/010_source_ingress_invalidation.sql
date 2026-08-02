-- M10.4 keeps one event writer. Command-address joins cover ordinary source
-- DDL/drop; an exact live snapshot catches publication membership drift even
-- when pg_event_trigger_ddl_commands() returns no rows.

CREATE OR REPLACE FUNCTION shiba_internal.invalidate_source_object()
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

        INSERT INTO shiba_internal.source_ingress_invalidation (
            source_id, publication_classid,
            publication_objid, publication_objsubid
        )
        SELECT config.source_id, config.publication_classid,
               config.publication_objid, config.publication_objsubid
        FROM shiba_internal.source_ingress_config AS config
        JOIN shiba_internal.source_binding AS binding
          ON binding.source_id = config.source_id
         AND binding.binding_kind = 'relation'
         AND binding.address_objsubid = 0
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_publication AS publication
            JOIN pg_catalog.pg_publication_rel AS member
              ON member.prpubid = publication.oid
            WHERE publication.oid = config.publication_objid
              AND publication.pubname = config.publication_name
              AND NOT publication.puballtables
              AND publication.pubinsert = config.publication_insert
              AND publication.pubupdate = config.publication_update
              AND publication.pubdelete = config.publication_delete
              AND publication.pubtruncate = config.publication_truncate
              AND publication.pubviaroot = config.publication_via_root
              AND member.prrelid = binding.address_objid
              AND member.prqual IS NULL
              AND 1 = (SELECT count(*) FROM pg_catalog.pg_publication_rel
                       WHERE prpubid = publication.oid)
              AND config.publication_attnums = CASE
                  WHEN member.prattrs IS NULL THEN ARRAY(
                      SELECT attribute.attnum::smallint
                      FROM pg_catalog.pg_attribute AS attribute
                      WHERE attribute.attrelid = binding.address_objid
                        AND attribute.attnum > 0 AND NOT attribute.attisdropped
                      ORDER BY attribute.attnum
                  ) ELSE ARRAY(
                      SELECT listed.attnum
                      FROM pg_catalog.unnest(member.prattrs::smallint[])
                           AS listed(attnum)
                      ORDER BY listed.attnum
                  ) END
        )
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

        INSERT INTO shiba_internal.source_ingress_invalidation (
            source_id, publication_classid,
            publication_objid, publication_objsubid
        )
        SELECT config.source_id, dropped.classid, dropped.objid, dropped.objsubid
        FROM pg_catalog.pg_event_trigger_dropped_objects() AS dropped
        JOIN shiba_internal.source_ingress_config AS config
          ON (config.publication_classid, config.publication_objid,
              config.publication_objsubid)
           = (dropped.classid, dropped.objid, dropped.objsubid)
        WHERE NOT dropped.is_temporary
        ON CONFLICT (source_id) DO NOTHING;
    ELSE
        RAISE EXCEPTION 'unsupported source invalidation event %', TG_EVENT;
    END IF;
END
$function$;
