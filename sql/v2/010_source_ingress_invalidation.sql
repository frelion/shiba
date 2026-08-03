-- One event writer records exact member ObjectAddress invalidation and promotes
-- publication/member-set drift to the owning graph in the same DDL transaction.

CREATE FUNCTION shiba_internal.invalidate_graph_object()
RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $function$
BEGIN
    IF TG_EVENT = 'ddl_command_end' THEN
        INSERT INTO shiba_internal.source_invalidation (
            source_id, address_classid, address_objid, address_objsubid
        ) SELECT binding.source_id, command.classid, command.objid, command.objsubid
          FROM pg_catalog.pg_event_trigger_ddl_commands() AS command
          JOIN shiba_internal.source_binding AS binding
            ON (binding.address_classid, binding.address_objid, binding.address_objsubid)
             = (command.classid, command.objid, command.objsubid)
          WHERE NOT command.in_extension ON CONFLICT (source_id) DO NOTHING;

        INSERT INTO shiba_internal.graph_ingress_invalidation (
            graph_id, publication_classid, publication_objid, publication_objsubid
        ) SELECT config.graph_id, config.publication_classid,
                 config.publication_objid, config.publication_objsubid
          FROM shiba_internal.graph_ingress_config AS config
          WHERE NOT EXISTS (
              SELECT 1 FROM pg_catalog.pg_publication AS publication
              WHERE publication.oid = config.publication_objid
                AND publication.pubname = config.publication_name
                AND NOT publication.puballtables
                AND publication.pubinsert = config.publication_insert
                AND publication.pubupdate = config.publication_update
                AND publication.pubdelete = config.publication_delete
                AND publication.pubtruncate = config.publication_truncate
                AND publication.pubviaroot = config.publication_via_root
          ) OR (SELECT count(*) FROM pg_catalog.pg_publication_rel
                WHERE prpubid = config.publication_objid)
               <> (SELECT count(*) FROM shiba_internal.graph_source_member
                   WHERE graph_id = config.graph_id)
             OR EXISTS (
              SELECT 1 FROM shiba_internal.graph_ingress_source AS ingress_source
              JOIN shiba_internal.graph_source_member AS member
                ON (member.graph_id, member.source_id)
                 = (ingress_source.graph_id, ingress_source.source_id)
              JOIN shiba_internal.source_binding AS binding
                ON binding.source_id = member.source_id
               AND binding.binding_kind = 'relation' AND binding.address_objsubid = 0
              LEFT JOIN pg_catalog.pg_publication_rel AS publication_member
                ON publication_member.prpubid = config.publication_objid
               AND publication_member.prrelid = binding.address_objid
              WHERE ingress_source.graph_id = config.graph_id
                AND (publication_member.prrelid IS NULL
                 OR publication_member.prqual IS NOT NULL
                 OR ingress_source.publication_attnums <> CASE
                    WHEN publication_member.prattrs IS NULL THEN ARRAY(
                        SELECT attribute.attnum::smallint
                        FROM pg_catalog.pg_attribute AS attribute
                        WHERE attribute.attrelid = binding.address_objid
                          AND attribute.attnum > 0 AND NOT attribute.attisdropped
                        ORDER BY attribute.attnum
                    ) ELSE ARRAY(
                        SELECT listed.attnum FROM pg_catalog.unnest(
                            publication_member.prattrs::smallint[]) AS listed(attnum)
                        ORDER BY listed.attnum
                    ) END)
          ) ON CONFLICT (graph_id) DO NOTHING;
    ELSIF TG_EVENT = 'sql_drop' THEN
        INSERT INTO shiba_internal.source_invalidation (
            source_id, address_classid, address_objid, address_objsubid
        ) SELECT binding.source_id, dropped.classid, dropped.objid, dropped.objsubid
          FROM pg_catalog.pg_event_trigger_dropped_objects() AS dropped
          JOIN shiba_internal.source_binding AS binding
            ON (binding.address_classid, binding.address_objid, binding.address_objsubid)
             = (dropped.classid, dropped.objid, dropped.objsubid)
          WHERE NOT dropped.is_temporary ON CONFLICT (source_id) DO NOTHING;
        INSERT INTO shiba_internal.graph_ingress_invalidation (
            graph_id, publication_classid, publication_objid, publication_objsubid
        ) SELECT config.graph_id, dropped.classid, dropped.objid, dropped.objsubid
          FROM pg_catalog.pg_event_trigger_dropped_objects() AS dropped
          JOIN shiba_internal.graph_ingress_config AS config
            ON (config.publication_classid, config.publication_objid,
                config.publication_objsubid)
             = (dropped.classid, dropped.objid, dropped.objsubid)
          WHERE NOT dropped.is_temporary ON CONFLICT (graph_id) DO NOTHING;
    ELSE RAISE EXCEPTION 'unsupported graph invalidation event %', TG_EVENT; END IF;
END
$function$;

CREATE EVENT TRIGGER shiba_source_ddl_command_end ON ddl_command_end
EXECUTE FUNCTION shiba_internal.invalidate_graph_object();
CREATE EVENT TRIGGER shiba_source_sql_drop ON sql_drop
EXECUTE FUNCTION shiba_internal.invalidate_graph_object();

REVOKE ALL ON FUNCTION shiba_internal.invalidate_graph_object() FROM PUBLIC;
