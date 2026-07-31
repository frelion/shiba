-- Result writes are legal only inside the Runtime's transactional Sink step.
CREATE FUNCTION shiba_internal.reject_result_write()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
BEGIN
    IF shiba.invoker_oid() <> (
        SELECT extension.extowner
        FROM pg_catalog.pg_extension AS extension
        WHERE extension.extname = 'shiba'
    ) THEN
        RAISE EXCEPTION 'cannot modify Shiba result table %.% directly',
          TG_TABLE_SCHEMA, TG_TABLE_NAME
          USING ERRCODE = 'read_only_sql_transaction';
    END IF;
    RETURN NULL;
END;
$$;

REVOKE ALL ON FUNCTION shiba._lock_dataflow_sources(oid[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION shiba._register_dataflow(oid, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION
  shiba_internal.create_effect_stream_payload(bigint, jsonb)
  FROM PUBLIC;
REVOKE ALL ON FUNCTION
  shiba_internal.validate_effect_stream_payload(bigint, jsonb)
  FROM PUBLIC;
REVOKE ALL ON FUNCTION
  shiba_internal.prepare_dataflow_source(oid)
  FROM PUBLIC;
REVOKE ALL ON FUNCTION shiba_internal.reject_result_write() FROM PUBLIC;
