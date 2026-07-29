CREATE FUNCTION shiba._register_compiled_stream_filter(
    result_relation oid,
    input_side text,
    source_relation oid,
    predicate_sql text,
    filter_phase text DEFAULT 'pre'
)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    source_name text;
BEGIN
    IF input_side NOT IN ('left', 'right') OR predicate_sql IS NULL
       OR filter_phase NOT IN ('pre','post') THEN
        RAISE EXCEPTION 'invalid compiled Shiba filter'
            USING ERRCODE = 'feature_not_supported';
    END IF;
    SELECT format('%I.%I', n.nspname, c.relname) INTO STRICT source_name
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = source_relation;
    EXECUTE format(
        'SELECT %s FROM (SELECT jsonb_populate_record(NULL::%s, $1) AS row) input',
        predicate_sql, source_name
    ) USING '{}'::jsonb;
    INSERT INTO shiba_internal.stream_filters
        (result_oid, input_side, source_oid, phase, predicate_sql)
    VALUES
        (result_relation, input_side, source_relation, filter_phase, predicate_sql);
END;
$$;

CREATE FUNCTION shiba._row_passes_filter(
    result_relation oid,
    p_input_side text,
    row_data jsonb
)
RETURNS boolean
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    filter_row shiba_internal.stream_filters%ROWTYPE;
    source_name text;
    passes boolean;
BEGIN
    SELECT * INTO filter_row FROM shiba_internal.stream_filters
    WHERE result_oid = result_relation AND input_side = p_input_side;
    IF NOT FOUND THEN RETURN true; END IF;
    IF filter_row.phase <> 'pre' THEN RETURN true; END IF;
    SELECT format('%I.%I', n.nspname, c.relname)
    INTO source_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = filter_row.source_oid;
    EXECUTE format(
        'SELECT %s FROM (SELECT jsonb_populate_record(NULL::%s, $1) AS row) input',
        filter_row.predicate_sql, source_name
    ) USING row_data INTO passes;
    RETURN COALESCE(passes, false);
END;
$$;

CREATE FUNCTION shiba._joined_rows_pass_filters(
    result_relation oid,
    left_row jsonb,
    right_row jsonb
)
RETURNS boolean
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    filter_row shiba_internal.stream_filters%ROWTYPE;
    source_name text;
    left_source_name text;
    right_source_name text;
    left_source_oid oid;
    right_source_oid oid;
    joined_predicate text;
    passes boolean;
BEGIN
    FOR filter_row IN
      SELECT * FROM shiba_internal.stream_filters
      WHERE result_oid=result_relation AND phase='post'
    LOOP
      SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT source_name
      FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE c.oid=filter_row.source_oid;
      EXECUTE format(
        'SELECT coalesce((%s),false)
         FROM (SELECT jsonb_populate_record(NULL::%s,$1) row) input',
        filter_row.predicate_sql,source_name
      ) USING CASE filter_row.input_side WHEN 'left' THEN left_row ELSE right_row END
        INTO passes;
      IF NOT passes THEN RETURN false; END IF;
    END LOOP;
    SELECT stream.source_oid,joined.right_source_oid,filter.predicate_sql
    INTO left_source_oid,right_source_oid,joined_predicate
    FROM shiba_internal.stream_views stream
    JOIN shiba_internal.inner_join_views joined USING(result_oid)
    JOIN shiba_internal.stream_join_filters filter USING(result_oid)
    WHERE stream.result_oid=result_relation;
    IF joined_predicate IS NOT NULL THEN
      SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT left_source_name
      FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE c.oid=left_source_oid;
      SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT right_source_name
      FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE c.oid=right_source_oid;
      EXECUTE format(
        'SELECT coalesce((%s),false)
         FROM (SELECT jsonb_populate_record(NULL::%s,$1) row) %I
         CROSS JOIN (SELECT jsonb_populate_record(NULL::%s,$2) row) %I',
        joined_predicate,left_source_name,format('input_%s',left_source_oid),
        right_source_name,format('input_%s',right_source_oid)
      ) USING left_row,right_row INTO passes;
      IF NOT passes THEN RETURN false; END IF;
    END IF;
    RETURN true;
END;
$$;
