CREATE FUNCTION shiba._store_query_analysis(query_text text, analysis jsonb)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal
AS $$
DECLARE
    normalized text;
    declaration text[];
    result_relation oid;
BEGIN
    normalized := regexp_replace(trim(query_text), '\s+', ' ', 'g');
    declaration := regexp_match(
        normalized,
        '^CREATE TABLE (IF NOT EXISTS )?shiba\.([a-z_][a-z_0-9]*) AS ',
        'i'
    );
    IF declaration IS NULL THEN
        RAISE EXCEPTION 'invalid Shiba CTAS while storing Query analysis'
            USING ERRCODE = 'data_corrupted';
    END IF;
    result_relation := format('%I.%I', 'shiba', declaration[2])::regclass;
    UPDATE shiba_internal.stream_graphs
    SET analyzed_query = analysis
    WHERE result_oid = result_relation;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Shiba logical graph is missing for result %', result_relation
            USING ERRCODE = 'data_corrupted';
    END IF;
END;
$$;

CREATE FUNCTION shiba._compile_stream_graph(result_relation oid, definition text)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    explained jsonb;
BEGIN
    EXECUTE 'EXPLAIN (VERBOSE, FORMAT JSON) ' || definition INTO explained;
    INSERT INTO shiba_internal.stream_graphs (result_oid, plan, logical_plan)
    VALUES (result_relation, explained, shiba.compile_logical_plan(result_relation))
    ON CONFLICT (result_oid) DO UPDATE
    SET plan = EXCLUDED.plan, logical_plan = EXCLUDED.logical_plan;

    INSERT INTO shiba_internal.operator_instances
        (result_oid, node_id, operator, config, stateful)
    SELECT result_relation,
           node ->> 'id',
           node ->> 'operator',
           node -> 'config',
           (node ->> 'operator') IN
             ('distinct', 'aggregate', 'window', 'top_n', 'inner_join', 'left_join',
              'right_join', 'full_join', 'semi_join', 'anti_join',
              'null_aware_anti_join')
    FROM jsonb_array_elements(shiba.compile_logical_plan(result_relation)::jsonb -> 'nodes') node;

    WITH RECURSIVE walk(node_id, parent_id, plan_node) AS (
        SELECT 'n0'::text, NULL::text, explained -> 0 -> 'Plan'
        UNION ALL
        SELECT
            walk.node_id || '.' || child.ordinality::text,
            walk.node_id,
            child.plan_node
        FROM walk
        CROSS JOIN LATERAL jsonb_array_elements(COALESCE(walk.plan_node -> 'Plans', '[]'::jsonb))
            WITH ORDINALITY AS child(plan_node, ordinality)
    )
    INSERT INTO shiba_internal.stream_graph_nodes (result_oid, node_id, operator, properties)
    SELECT result_relation, node_id, plan_node ->> 'Node Type', plan_node
    FROM walk;

    WITH RECURSIVE walk(node_id, parent_id, plan_node) AS (
        SELECT 'n0'::text, NULL::text, explained -> 0 -> 'Plan'
        UNION ALL
        SELECT walk.node_id || '.' || child.ordinality::text, walk.node_id, child.plan_node
        FROM walk
        CROSS JOIN LATERAL jsonb_array_elements(COALESCE(walk.plan_node -> 'Plans', '[]'::jsonb))
            WITH ORDINALITY AS child(plan_node, ordinality)
    )
    INSERT INTO shiba_internal.stream_graph_edges (result_oid, upstream_node_id, downstream_node_id)
    SELECT result_relation, node_id, parent_id FROM walk WHERE parent_id IS NOT NULL;

    INSERT INTO shiba_internal.stream_graph_nodes (result_oid, node_id, operator, properties)
    VALUES (result_relation, 'sink', 'Shiba Sink', jsonb_build_object('result_oid', result_relation));
    INSERT INTO shiba_internal.stream_graph_edges (result_oid, upstream_node_id, downstream_node_id)
    VALUES (result_relation, 'n0', 'sink');
END;
$$;

CREATE FUNCTION shiba._register_inner_join_stream_table(query_text text, analysis jsonb)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal, public AS $$
DECLARE
    normalized text; d text[]; left_oid oid; right_oid oid; target_oid oid;
    left_name text; right_name text; target_name text; group_side text; publication_member boolean;
    activation_lsn_value pg_lsn; sum_is_not_null boolean;
    definition text; predicate_sql text; predicate_source oid;
    result_group_name name; filter_side text; filter_source_oid oid;
    group_column_name name; count_column_name name;
    count_distinct_value boolean; count_input_name name; count_input_oid oid;
    count_input_side text;
    sum_input_name name; sum_column_name name; left_join_column_name name; right_join_column_name name;
    group_oid oid; sum_oid oid; join_left_oid oid; join_right_oid oid;
    left_join_type oid; right_join_type oid;
    left_join_collation oid; right_join_collation oid;
    left_collation_deterministic boolean := true;
    right_collation_deterministic boolean := true;
BEGIN
    normalized := regexp_replace(regexp_replace(trim(query_text), '\s+', ' ', 'g'), ';$', '');
    d := regexp_match(normalized, '^CREATE TABLE (IF NOT EXISTS )?shiba\.([a-z_][a-z_0-9]*) AS (SELECT .*)$', 'i');
    IF d IS NULL THEN RAISE EXCEPTION 'invalid Shiba JOIN declaration' USING ERRCODE='feature_not_supported'; END IF;
    definition := d[3];
    IF analysis -> 'where_predicate' ->> 'error' IS NOT NULL THEN
      RAISE EXCEPTION 'unsupported Shiba JOIN filter: %', analysis -> 'where_predicate' ->> 'error'
        USING ERRCODE='feature_not_supported';
    END IF;
    predicate_sql := analysis -> 'where_predicate' ->> 'sql';
    IF jsonb_array_length(analysis -> 'sources') <> 2
       OR jsonb_array_length(analysis -> 'joins') <> 1
       OR analysis -> 'joins' -> 0 ->> 'kind'
          NOT IN ('inner','left','right','full','semi','anti','null_anti')
       OR analysis -> 'joins' -> 0 ->> 'operator' <> '='
       OR (analysis ->> 'group_keys')::integer <> 1
       OR jsonb_array_length(analysis -> 'targets') <> 3
       OR analysis -> 'targets' -> 0 ->> 'expression' <> 'column'
       OR (analysis -> 'targets' -> 0 ->> 'grouping_reference')::integer = 0
       OR analysis -> 'targets' -> 1 ->> 'expression' <> 'aggregate'
       OR lower(analysis -> 'targets' -> 1 ->> 'aggregate') <> 'count'
       OR NOT (
         ((analysis -> 'targets' -> 1 ->> 'aggregate_star')::boolean
          AND NOT (analysis -> 'targets' -> 1 ->> 'aggregate_distinct')::boolean)
         OR
         (NOT (analysis -> 'targets' -> 1 ->> 'aggregate_star')::boolean
          AND (analysis -> 'targets' -> 1 ->> 'aggregate_distinct')::boolean
          AND (analysis -> 'targets' -> 1 ->> 'input_table_oid')::oid <> 0
          AND (analysis -> 'targets' -> 1 ->> 'input_column')::smallint > 0)
       )
       OR analysis -> 'targets' -> 2 ->> 'expression' <> 'aggregate'
       OR lower(analysis -> 'targets' -> 2 ->> 'aggregate') <> 'sum'
       OR (analysis -> 'targets' -> 2 ->> 'aggregate_distinct')::boolean THEN
      RAISE EXCEPTION 'Shiba requires two sources, one equality JOIN, one group key, COUNT(*) or COUNT(DISTINCT column), and SUM(left_column)'
        USING ERRCODE='feature_not_supported';
    END IF;
    left_oid := (analysis -> 'sources' -> 0 ->> 'oid')::oid;
    right_oid := (analysis -> 'sources' -> 1 ->> 'oid')::oid;
    IF left_oid=right_oid THEN
      RAISE EXCEPTION 'Shiba does not yet support self-joins'
        USING ERRCODE='feature_not_supported';
    END IF;
    group_oid := (analysis -> 'targets' -> 0 ->> 'origin_table_oid')::oid;
    count_distinct_value := (analysis -> 'targets' -> 1 ->> 'aggregate_distinct')::boolean;
    count_input_oid := CASE WHEN count_distinct_value
      THEN (analysis -> 'targets' -> 1 ->> 'input_table_oid')::oid
      ELSE NULL
    END;
    IF jsonb_array_length(
         coalesce(analysis -> 'having_distinct_inputs','[]'::jsonb)
       )>0
       AND (
         NOT count_distinct_value
         OR jsonb_array_length(analysis -> 'having_distinct_inputs')<>1
         OR (analysis -> 'having_distinct_inputs' -> 0 ->> 'table_oid')::oid
              <>count_input_oid
         OR (analysis -> 'having_distinct_inputs' -> 0 ->> 'column')::smallint
              <>(analysis -> 'targets' -> 1 ->> 'input_column')::smallint
       ) THEN
      RAISE EXCEPTION 'HAVING COUNT(DISTINCT) must match the maintained SELECT aggregate'
        USING ERRCODE='feature_not_supported';
    END IF;
    sum_oid := (analysis -> 'targets' -> 2 ->> 'input_table_oid')::oid;
    IF jsonb_array_length(coalesce(analysis -> 'having_sum_inputs','[]'::jsonb))>0
       AND (
         jsonb_array_length(analysis -> 'having_sum_inputs')<>1
         OR (analysis -> 'having_sum_inputs' -> 0 ->> 'table_oid')::oid<>sum_oid
         OR (analysis -> 'having_sum_inputs' -> 0 ->> 'column')::smallint
              <>(analysis -> 'targets' -> 2 ->> 'input_column')::smallint
       ) THEN
      RAISE EXCEPTION 'HAVING SUM must match the maintained SELECT aggregate'
        USING ERRCODE='feature_not_supported';
    END IF;
    join_left_oid := (analysis -> 'joins' -> 0 ->> 'left_table_oid')::oid;
    join_right_oid := (analysis -> 'joins' -> 0 ->> 'right_table_oid')::oid;
    IF sum_oid <> left_oid OR group_oid NOT IN (left_oid, right_oid)
       OR (count_distinct_value AND count_input_oid NOT IN (left_oid,right_oid))
       OR NOT ((join_left_oid = left_oid AND join_right_oid = right_oid)
            OR (join_left_oid = right_oid AND join_right_oid = left_oid)) THEN
      RAISE EXCEPTION 'Shiba JOIN target or equality columns do not belong to the expected inputs'
        USING ERRCODE='feature_not_supported';
    END IF;
    SELECT attname INTO STRICT group_column_name FROM pg_attribute
    WHERE attrelid=group_oid AND attnum=(analysis -> 'targets' -> 0 ->> 'origin_column')::smallint;
    SELECT attname INTO STRICT sum_input_name FROM pg_attribute
    WHERE attrelid=sum_oid AND attnum=(analysis -> 'targets' -> 2 ->> 'input_column')::smallint;
    IF count_distinct_value THEN
      SELECT attname INTO STRICT count_input_name FROM pg_attribute
      WHERE attrelid=count_input_oid
        AND attnum=(analysis -> 'targets' -> 1 ->> 'input_column')::smallint;
      count_input_side := CASE WHEN count_input_oid=left_oid THEN 'left' ELSE 'right' END;
    END IF;
    IF join_left_oid = left_oid THEN
      SELECT attname INTO STRICT left_join_column_name FROM pg_attribute
      WHERE attrelid=left_oid AND attnum=(analysis -> 'joins' -> 0 ->> 'left_column')::smallint;
      SELECT attname INTO STRICT right_join_column_name FROM pg_attribute
      WHERE attrelid=right_oid AND attnum=(analysis -> 'joins' -> 0 ->> 'right_column')::smallint;
    ELSE
      SELECT attname INTO STRICT left_join_column_name FROM pg_attribute
      WHERE attrelid=left_oid AND attnum=(analysis -> 'joins' -> 0 ->> 'right_column')::smallint;
      SELECT attname INTO STRICT right_join_column_name FROM pg_attribute
      WHERE attrelid=right_oid AND attnum=(analysis -> 'joins' -> 0 ->> 'left_column')::smallint;
    END IF;
    SELECT atttypid,attcollation
    INTO STRICT left_join_type,left_join_collation
    FROM pg_attribute
    WHERE attrelid=left_oid AND attname=left_join_column_name
      AND attnum>0 AND NOT attisdropped;
    SELECT atttypid,attcollation
    INTO STRICT right_join_type,right_join_collation
    FROM pg_attribute
    WHERE attrelid=right_oid AND attname=right_join_column_name
      AND attnum>0 AND NOT attisdropped;
    IF left_join_collation<>0 THEN
      SELECT collisdeterministic INTO STRICT left_collation_deterministic
      FROM pg_collation WHERE oid=left_join_collation;
    END IF;
    IF right_join_collation<>0 THEN
      SELECT collisdeterministic INTO STRICT right_collation_deterministic
      FROM pg_collation WHERE oid=right_join_collation;
    END IF;
    IF left_join_type<>right_join_type
       OR NOT left_collation_deterministic
       OR NOT right_collation_deterministic THEN
      RAISE EXCEPTION
        'Shiba equality JOIN keys must have the same PostgreSQL type and deterministic collations'
        USING ERRCODE='feature_not_supported';
    END IF;
    target_oid := format('%I.%I','shiba',d[2])::regclass;
    SELECT format('%I.%I',n.nspname,c.relname) INTO left_name FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=left_oid;
    SELECT format('%I.%I',n.nspname,c.relname) INTO right_name FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=right_oid;
    target_name := format('%I.%I','shiba',d[2]);
    result_group_name := (analysis -> 'targets' -> 0 ->> 'name')::name;
    count_column_name := (analysis -> 'targets' -> 1 ->> 'name')::name;
    sum_column_name := (analysis -> 'targets' -> 2 ->> 'name')::name;
    group_side := CASE WHEN group_oid=left_oid THEN 'left' ELSE 'right' END;
    SELECT attribute.attnotnull INTO sum_is_not_null
    FROM pg_attribute attribute
    WHERE attribute.attrelid = left_oid AND attribute.attname = sum_input_name
      AND attribute.attnum > 0 AND NOT attribute.attisdropped;
    IF sum_is_not_null IS DISTINCT FROM true THEN
      RAISE EXCEPTION 'Shiba JOIN requires SUM input column % to be NOT NULL', sum_input_name
        USING ERRCODE='feature_not_supported';
    END IF;
    PERFORM shiba._validate_source_table(left_oid);
    PERFORM shiba._validate_source_table(right_oid);
    PERFORM shiba._ensure_replica_identity_full(left_oid);
    PERFORM shiba._ensure_replica_identity_full(right_oid);
    EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %I UNIQUE NULLS NOT DISTINCT (%I)',target_name,format('shiba_key_%s',target_oid),result_group_name);
    activation_lsn_value := pg_current_wal_lsn();
    INSERT INTO shiba_internal.stream_views
      (result_oid,source_oid,group_column,result_group_column,count_column,
       count_distinct,count_input_source,count_input_column,
       sum_input_column,sum_column,activation_lsn)
    VALUES
      (target_oid,left_oid,group_column_name,result_group_name,count_column_name,
       count_distinct_value,count_input_side,count_input_name,
       sum_input_name,sum_column_name,activation_lsn_value);
    IF analysis -> 'having_predicate' ->> 'sql' IS NOT NULL THEN
      INSERT INTO shiba_internal.stream_having(result_oid,predicate_sql)
      VALUES(target_oid,analysis -> 'having_predicate' ->> 'sql');
    END IF;
    INSERT INTO shiba_internal.inner_join_views
      (result_oid,join_type,right_source_oid,left_join_column,right_join_column,group_source,group_column,sum_source)
    VALUES (target_oid,analysis -> 'joins' -> 0 ->> 'kind',right_oid,
      left_join_column_name,right_join_column_name,group_side,group_column_name,'left');
    IF predicate_sql IS NOT NULL THEN
      IF jsonb_array_length(analysis -> 'where_predicate' -> 'source_oids')=2 THEN
        INSERT INTO shiba_internal.stream_join_filters(result_oid,predicate_sql)
        VALUES(target_oid,predicate_sql);
      ELSIF jsonb_array_length(analysis -> 'where_predicate' -> 'source_oids')=1 THEN
        predicate_source := (analysis -> 'where_predicate' -> 'source_oids' ->> 0)::oid;
        IF predicate_source = left_oid THEN filter_side := 'left'; filter_source_oid := left_oid;
        ELSIF predicate_source = right_oid THEN filter_side := 'right'; filter_source_oid := right_oid;
        ELSE RAISE EXCEPTION 'Shiba JOIN filter source is not a JOIN input' USING ERRCODE='undefined_table'; END IF;
        predicate_sql := replace(
          predicate_sql,format('input_%s',predicate_source),'input'
        );
        PERFORM shiba._register_compiled_stream_filter(
          target_oid,filter_side,filter_source_oid,predicate_sql,
          CASE
            WHEN analysis -> 'joins' -> 0 ->> 'kind'
              IN ('inner','semi','anti','null_anti') THEN 'pre'
            WHEN analysis -> 'joins' -> 0 ->> 'kind' = 'left' AND filter_side = 'left' THEN 'pre'
            WHEN analysis -> 'joins' -> 0 ->> 'kind' = 'right' AND filter_side = 'right' THEN 'pre'
            ELSE 'post'
          END
        );
      ELSE
        RAISE EXCEPTION 'a Shiba JOIN filter must reference one or both inputs'
          USING ERRCODE='feature_not_supported';
      END IF;
    END IF;
    INSERT INTO shiba_internal.view_progress (result_oid) VALUES (target_oid);
    INSERT INTO shiba_internal.dag_worker_state (result_oid) VALUES (target_oid);
    PERFORM shiba._compile_stream_graph(target_oid,d[3]);
    PERFORM shiba._initialize_aggregate_state(target_oid);
    EXECUTE format('INSERT INTO shiba_internal.join_arrangements SELECT %s,''left'',coalesce(j.row->>%L,''''),j.row,count(*) FROM %s x CROSS JOIN LATERAL (SELECT jsonb_object_agg(key,value) row FROM jsonb_each_text(to_jsonb(x))) j WHERE shiba._row_passes_filter(%s,''left'',j.row) GROUP BY j.row->>%L,j.row',target_oid,left_join_column_name,left_name,target_oid,left_join_column_name);
    EXECUTE format('INSERT INTO shiba_internal.join_arrangements SELECT %s,''right'',coalesce(j.row->>%L,''''),j.row,count(*) FROM %s x CROSS JOIN LATERAL (SELECT jsonb_object_agg(key,value) row FROM jsonb_each_text(to_jsonb(x))) j WHERE shiba._row_passes_filter(%s,''right'',j.row) GROUP BY j.row->>%L,j.row',target_oid,right_join_column_name,right_name,target_oid,right_join_column_name);
    FOREACH left_oid IN ARRAY ARRAY[left_oid,right_oid] LOOP
      SELECT EXISTS(SELECT 1 FROM pg_publication_rel pr JOIN pg_publication p ON p.oid=pr.prpubid WHERE p.pubname='shiba_publication' AND pr.prrelid=left_oid) INTO publication_member;
      IF NOT publication_member THEN EXECUTE format('ALTER PUBLICATION shiba_publication ADD TABLE %s',(SELECT format('%I.%I',n.nspname,c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=left_oid)); END IF;
    END LOOP;
    EXECUTE format('CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION shiba._request_worker()',format('shiba_wakeup_%s_left',target_oid),left_name);
    EXECUTE format('CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION shiba._request_worker()',format('shiba_wakeup_%s_right',target_oid),right_name);
    EXECUTE format('CREATE TRIGGER %I BEFORE TRUNCATE ON %s FOR EACH STATEMENT EXECUTE FUNCTION shiba._reject_source_truncate()',format('shiba_no_truncate_%s_left',target_oid),left_name);
    EXECUTE format('CREATE TRIGGER %I BEFORE TRUNCATE ON %s FOR EACH STATEMENT EXECUTE FUNCTION shiba._reject_source_truncate()',format('shiba_no_truncate_%s_right',target_oid),right_name);
    EXECUTE format('CREATE TRIGGER %I BEFORE INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION shiba._protect_result_table()',format('shiba_protect_%s',target_oid),target_name);
    EXECUTE format('GRANT SELECT ON %s TO %I', target_name, session_user);
    EXECUTE format('ALTER TABLE %s OWNER TO %I', target_name, shiba_internal.extension_owner());
END; $$;

CREATE FUNCTION shiba._register_subquery_stream_table(query_text text, analysis jsonb)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal, public
AS $$
DECLARE
    subquery jsonb;
    transformed jsonb;
    outer_oid oid;
BEGIN
    IF analysis -> 'where_predicate' ->> 'error' IS NOT NULL THEN
      RAISE EXCEPTION 'unsupported Shiba subquery: %',
        analysis -> 'where_predicate' ->> 'error'
        USING ERRCODE='feature_not_supported';
    END IF;
    IF jsonb_array_length(analysis -> 'sources')<>1
       OR jsonb_array_length(coalesce(analysis -> 'subqueries','[]'::jsonb))<>1 THEN
      RAISE EXCEPTION 'Shiba supports one decorrelatable EXISTS, NOT EXISTS, or IN subquery'
        USING ERRCODE='feature_not_supported';
    END IF;
    subquery := analysis -> 'subqueries' -> 0;
    outer_oid := (analysis -> 'sources' -> 0 ->> 'oid')::oid;
    IF (subquery ->> 'left_table_oid')::oid<>outer_oid
       OR (subquery ->> 'right_table_oid')::oid<>(subquery ->> 'source_oid')::oid THEN
      RAISE EXCEPTION 'Shiba subquery correlation does not connect its outer and inner sources'
        USING ERRCODE='feature_not_supported';
    END IF;
    transformed := jsonb_set(
      jsonb_set(
        jsonb_set(analysis,'{has_sublinks}','false'::jsonb),
        '{sources}',
        (analysis -> 'sources') || jsonb_build_array(
          jsonb_build_object('oid',(subquery ->> 'source_oid')::oid,'alias',NULL)
        )
      ),
      '{joins}',
      jsonb_build_array(jsonb_build_object(
        'kind',subquery ->> 'kind',
        'operator','=',
        'left_table_oid',(subquery ->> 'left_table_oid')::oid,
        'left_column',(subquery ->> 'left_column')::smallint,
        'right_table_oid',(subquery ->> 'right_table_oid')::oid,
        'right_column',(subquery ->> 'right_column')::smallint
      ))
    );
    IF jsonb_array_length(
         coalesce(analysis -> 'where_predicate' -> 'source_oids','[]'::jsonb)
       )=0 THEN
      transformed := jsonb_set(transformed,'{where_predicate}','null'::jsonb);
    END IF;
    PERFORM shiba._register_inner_join_stream_table(query_text,transformed);
END;
$$;

CREATE FUNCTION shiba._register_window_stream_table(query_text text, analysis jsonb)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,shiba_internal,public
AS $$
DECLARE
    normalized text;
    declaration text[];
    definition text;
    source_oid oid;
    source_name text;
    target_oid oid;
    target_name text;
    window_spec jsonb;
    target jsonb;
    input_column_name name;
    partition_column_name name;
    order_column_name name;
    result_partition_name name;
    output_columns name[] := ARRAY[]::name[];
    target_expressions text[] := ARRAY[]::text[];
    actual_outputs name[];
    function_name text;
    function_expression text;
    window_suffix text;
    predicate_sql text;
    activation_lsn_value pg_lsn;
    source_in_publication boolean;
BEGIN
    IF (analysis ->> 'has_sublinks')::boolean
       OR (analysis ->> 'has_aggregates')::boolean
       OR (analysis ->> 'has_distinct')::boolean
       OR (analysis ->> 'has_set_operations')::boolean
       OR (analysis ->> 'has_limit')::boolean
       OR (analysis ->> 'group_keys')::integer<>0
       OR (analysis ->> 'has_having')::boolean
       OR jsonb_array_length(analysis -> 'sources')<>1
       OR jsonb_array_length(analysis -> 'joins')<>0
       OR jsonb_array_length(analysis -> 'windows')<>1 THEN
      RAISE EXCEPTION 'Shiba window tables require one source and one non-aggregate window specification'
        USING ERRCODE='feature_not_supported',DETAIL=analysis::text;
    END IF;
    IF analysis -> 'where_predicate' ->> 'error' IS NOT NULL THEN
      RAISE EXCEPTION 'unsupported Shiba window filter: %',
        analysis -> 'where_predicate' ->> 'error'
        USING ERRCODE='feature_not_supported';
    END IF;
    window_spec := analysis -> 'windows' -> 0;
    IF (window_spec ->> 'partition_keys')::integer<>1
       OR (window_spec ->> 'order_keys')::integer<>1
       OR window_spec ->> 'frame_error' IS NOT NULL THEN
      RAISE EXCEPTION 'Shiba windows require one PARTITION BY key, one ORDER BY key, and a constant frame: %',
        coalesce(window_spec ->> 'frame_error','invalid key count')
        USING ERRCODE='feature_not_supported';
    END IF;
    source_oid := (analysis -> 'sources' -> 0 ->> 'oid')::oid;
    IF (window_spec ->> 'partition_table_oid')::oid<>source_oid
       OR (window_spec ->> 'order_table_oid')::oid<>source_oid THEN
      RAISE EXCEPTION 'Shiba window partition and order keys must be source columns'
        USING ERRCODE='feature_not_supported';
    END IF;
    SELECT attname INTO STRICT partition_column_name FROM pg_attribute
    WHERE attrelid=source_oid
      AND attnum=(window_spec ->> 'partition_column')::smallint;
    SELECT attname INTO STRICT order_column_name FROM pg_attribute
    WHERE attrelid=source_oid
      AND attnum=(window_spec ->> 'order_column')::smallint;

    normalized := regexp_replace(regexp_replace(trim(query_text),'\s+',' ','g'),';$','');
    declaration := regexp_match(
      normalized,
      '^CREATE TABLE (IF NOT EXISTS )?shiba\.([a-z_][a-z_0-9]*) AS (SELECT .*)$',
      'i'
    );
    IF declaration IS NULL THEN
      RAISE EXCEPTION 'invalid Shiba window declaration'
        USING ERRCODE='feature_not_supported';
    END IF;
    definition := declaration[3];
    target_name := format('%I.%I','shiba',declaration[2]);
    target_oid := target_name::regclass;
    SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT source_name
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=source_oid;

    window_suffix := format(
      'OVER (PARTITION BY input.%I ORDER BY input.%I %s NULLS %s%s)',
      partition_column_name,order_column_name,
      upper(window_spec ->> 'order_direction'),
      CASE (window_spec ->> 'nulls_first')::boolean WHEN true THEN 'FIRST' ELSE 'LAST' END,
      CASE WHEN window_spec ->> 'frame_clause' IS NULL
        THEN '' ELSE ' ' || (window_spec ->> 'frame_clause') END
    );
    FOR target IN
      SELECT value FROM jsonb_array_elements(analysis -> 'targets')
      WHERE NOT (value ->> 'resjunk')::boolean
    LOOP
      IF target ->> 'name' IS NULL THEN
        RAISE EXCEPTION 'every Shiba window output requires a column name'
          USING ERRCODE='feature_not_supported';
      END IF;
      output_columns := array_append(output_columns,(target ->> 'name')::name);
      IF target ->> 'expression'='column' THEN
        IF (target ->> 'origin_table_oid')::oid<>source_oid
           OR (target ->> 'origin_column')::smallint<=0 THEN
          RAISE EXCEPTION 'Shiba window projections must be ordinary source columns'
            USING ERRCODE='feature_not_supported';
        END IF;
        SELECT attname INTO STRICT input_column_name FROM pg_attribute
        WHERE attrelid=source_oid AND attnum=(target ->> 'origin_column')::smallint;
        target_expressions := array_append(
          target_expressions,format('input.%I',input_column_name)
        );
        IF input_column_name=partition_column_name THEN
          result_partition_name := (target ->> 'name')::name;
        END IF;
      ELSIF target ->> 'expression'='window' THEN
        IF (target ->> 'window_ref')::integer<>(window_spec ->> 'window_ref')::integer THEN
          RAISE EXCEPTION 'all Shiba window functions must use the same window specification'
            USING ERRCODE='feature_not_supported';
        END IF;
        function_name := lower(target ->> 'window_function');
        IF function_name IN ('row_number','rank','dense_rank')
           AND (target ->> 'input_table_oid')::oid=0 THEN
          function_expression := format('%I()',function_name);
        ELSIF function_name='count' AND (target ->> 'window_star')::boolean THEN
          function_expression := 'count(*)';
        ELSIF function_name IN ('count','sum','avg','min','max')
              AND (target ->> 'input_table_oid')::oid=source_oid
              AND (target ->> 'input_column')::smallint>0 THEN
          SELECT attname INTO STRICT input_column_name FROM pg_attribute
          WHERE attrelid=source_oid
            AND attnum=(target ->> 'input_column')::smallint;
          function_expression := format('%I(input.%I)',function_name,input_column_name);
        ELSE
          RAISE EXCEPTION 'unsupported Shiba window function %',function_name
            USING ERRCODE='feature_not_supported';
        END IF;
        target_expressions := array_append(
          target_expressions,format('%s %s',function_expression,window_suffix)
        );
      ELSE
        RAISE EXCEPTION 'unsupported Shiba window target expression %',
          target ->> 'expression' USING ERRCODE='feature_not_supported';
      END IF;
    END LOOP;
    IF result_partition_name IS NULL THEN
      RAISE EXCEPTION 'the PARTITION BY column must be projected by a Shiba window table'
        USING ERRCODE='feature_not_supported';
    END IF;
    SELECT array_agg(attname ORDER BY attnum) INTO actual_outputs
    FROM pg_attribute
    WHERE attrelid=target_oid AND attnum>0 AND NOT attisdropped;
    IF actual_outputs IS DISTINCT FROM output_columns THEN
      RAISE EXCEPTION 'Shiba window output metadata does not match the CTAS result'
        USING ERRCODE='data_exception';
    END IF;

    PERFORM shiba._validate_source_table(source_oid);
    PERFORM shiba._ensure_replica_identity_full(source_oid);
    activation_lsn_value := pg_current_wal_lsn();
    INSERT INTO shiba_internal.stream_views
      (result_oid,view_kind,source_oid,activation_lsn)
    VALUES(target_oid,'window',source_oid,activation_lsn_value);
    INSERT INTO shiba_internal.window_views
      (result_oid,partition_column,result_partition_column,order_column,
       order_direction,nulls_first,output_columns,target_expressions)
    VALUES(target_oid,partition_column_name,result_partition_name,order_column_name,
      window_spec ->> 'order_direction',(window_spec ->> 'nulls_first')::boolean,
      output_columns,target_expressions);
    predicate_sql := analysis -> 'where_predicate' ->> 'sql';
    predicate_sql := replace(predicate_sql,format('input_%s',source_oid),'input');
    IF predicate_sql IS NOT NULL THEN
      IF jsonb_array_length(analysis -> 'where_predicate' -> 'source_oids')<>1
         OR (analysis -> 'where_predicate' -> 'source_oids' ->> 0)::oid<>source_oid THEN
        RAISE EXCEPTION 'Shiba window filter must reference its source'
          USING ERRCODE='feature_not_supported';
      END IF;
      PERFORM shiba._register_compiled_stream_filter(
        target_oid,'left',source_oid,predicate_sql
      );
    END IF;
    EXECUTE format(
      'INSERT INTO shiba_internal.window_rows
         (result_oid,partition_key,row_data,multiplicity)
       SELECT $1,coalesce(to_jsonb(x.%I),''null''::jsonb),canonical.row,count(*)
       FROM %s x
       CROSS JOIN LATERAL (
         SELECT jsonb_object_agg(key,value) AS row
         FROM jsonb_each_text(to_jsonb(x))
       ) canonical
       WHERE shiba._row_passes_filter($1,''left'',to_jsonb(x))
       GROUP BY x.%I,canonical.row',
      partition_column_name,source_name,partition_column_name
    ) USING target_oid;
    INSERT INTO shiba_internal.view_progress(result_oid) VALUES(target_oid);
    INSERT INTO shiba_internal.dag_worker_state(result_oid) VALUES(target_oid);
    PERFORM shiba._compile_stream_graph(target_oid,definition);

    SELECT EXISTS(
      SELECT 1 FROM pg_publication_rel pr JOIN pg_publication p ON p.oid=pr.prpubid
      WHERE p.pubname='shiba_publication' AND pr.prrelid=source_oid
    ) INTO source_in_publication;
    IF NOT source_in_publication THEN
      EXECUTE format('ALTER PUBLICATION shiba_publication ADD TABLE %s',source_name);
    END IF;
    EXECUTE format(
      'CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %s
       FOR EACH ROW EXECUTE FUNCTION shiba._request_worker()',
      format('shiba_wakeup_%s',target_oid),source_name
    );
    EXECUTE format(
      'CREATE TRIGGER %I BEFORE TRUNCATE ON %s
       FOR EACH STATEMENT EXECUTE FUNCTION shiba._reject_source_truncate()',
      format('shiba_no_truncate_%s',target_oid),source_name
    );
    EXECUTE format(
      'CREATE TRIGGER %I BEFORE INSERT OR UPDATE OR DELETE ON %s
       FOR EACH ROW EXECUTE FUNCTION shiba._protect_result_table()',
      format('shiba_protect_%s',target_oid),target_name
    );
    EXECUTE format('GRANT SELECT ON %s TO %I',target_name,session_user);
    EXECUTE format('ALTER TABLE %s OWNER TO %I',target_name,shiba_internal.extension_owner());
END;
$$;

CREATE FUNCTION shiba._register_distinct_stream_table(query_text text, analysis jsonb)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,shiba_internal,public
AS $$
DECLARE
    normalized text;
    declaration text[];
    definition text;
    source_oid oid;
    source_name text;
    target_oid oid;
    target_name text;
    target jsonb;
    current_source_column name;
    source_columns name[] := ARRAY[]::name[];
    output_columns name[] := ARRAY[]::name[];
    actual_outputs name[];
    key_arguments text;
    predicate_sql text;
    activation_lsn_value pg_lsn;
    source_in_publication boolean;
BEGIN
    IF (analysis ->> 'has_distinct_on')::boolean
       OR (analysis ->> 'has_window_functions')::boolean
       OR (analysis ->> 'has_sublinks')::boolean
       OR (analysis ->> 'has_aggregates')::boolean
       OR (analysis ->> 'has_set_operations')::boolean
       OR (analysis ->> 'has_limit')::boolean
       OR (analysis ->> 'group_keys')::integer<>0
       OR jsonb_array_length(analysis -> 'sources')<>1
       OR jsonb_array_length(analysis -> 'joins')<>0 THEN
      RAISE EXCEPTION 'Shiba SELECT DISTINCT requires ordinary columns from one source'
        USING ERRCODE='feature_not_supported',DETAIL=analysis::text;
    END IF;
    IF analysis -> 'where_predicate' ->> 'error' IS NOT NULL THEN
      RAISE EXCEPTION 'unsupported Shiba DISTINCT filter: %',
        analysis -> 'where_predicate' ->> 'error'
        USING ERRCODE='feature_not_supported';
    END IF;
    source_oid := (analysis -> 'sources' -> 0 ->> 'oid')::oid;
    FOR target IN
      SELECT value FROM jsonb_array_elements(analysis -> 'targets')
      WHERE NOT (value ->> 'resjunk')::boolean
    LOOP
      IF target ->> 'expression'<>'column'
         OR (target ->> 'origin_table_oid')::oid<>source_oid
         OR (target ->> 'origin_column')::smallint<=0
         OR target ->> 'name' IS NULL THEN
        RAISE EXCEPTION 'Shiba SELECT DISTINCT outputs must be ordinary source columns'
          USING ERRCODE='feature_not_supported';
      END IF;
      SELECT attname INTO STRICT current_source_column FROM pg_attribute
      WHERE attrelid=source_oid
        AND attnum=(target ->> 'origin_column')::smallint;
      source_columns := array_append(source_columns,current_source_column);
      output_columns := array_append(output_columns,(target ->> 'name')::name);
    END LOOP;
    IF cardinality(output_columns)=0 THEN
      RAISE EXCEPTION 'Shiba SELECT DISTINCT requires at least one output column'
        USING ERRCODE='feature_not_supported';
    END IF;
    normalized := regexp_replace(regexp_replace(trim(query_text),'\s+',' ','g'),';$','');
    declaration := regexp_match(
      normalized,
      '^CREATE TABLE (IF NOT EXISTS )?shiba\.([a-z_][a-z_0-9]*) AS (SELECT .*)$',
      'i'
    );
    IF declaration IS NULL THEN
      RAISE EXCEPTION 'invalid Shiba DISTINCT declaration'
        USING ERRCODE='feature_not_supported';
    END IF;
    definition := declaration[3];
    target_name := format('%I.%I','shiba',declaration[2]);
    target_oid := target_name::regclass;
    SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT source_name
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=source_oid;
    SELECT array_agg(attname ORDER BY attnum) INTO actual_outputs FROM pg_attribute
    WHERE attrelid=target_oid AND attnum>0 AND NOT attisdropped;
    IF actual_outputs IS DISTINCT FROM output_columns THEN
      RAISE EXCEPTION 'Shiba DISTINCT output metadata does not match the CTAS result'
        USING ERRCODE='data_exception';
    END IF;

    PERFORM shiba._validate_source_table(source_oid);
    PERFORM shiba._ensure_replica_identity_full(source_oid);
    activation_lsn_value := pg_current_wal_lsn();
    INSERT INTO shiba_internal.stream_views
      (result_oid,view_kind,source_oid,activation_lsn)
    VALUES(target_oid,'distinct',source_oid,activation_lsn_value);
    INSERT INTO shiba_internal.distinct_views
      (result_oid,source_columns,output_columns)
    VALUES(target_oid,source_columns,output_columns);
    predicate_sql := analysis -> 'where_predicate' ->> 'sql';
    predicate_sql := replace(predicate_sql,format('input_%s',source_oid),'input');
    IF predicate_sql IS NOT NULL THEN
      IF jsonb_array_length(analysis -> 'where_predicate' -> 'source_oids')<>1
         OR (analysis -> 'where_predicate' -> 'source_oids' ->> 0)::oid<>source_oid THEN
        RAISE EXCEPTION 'Shiba DISTINCT filter must reference its source'
          USING ERRCODE='feature_not_supported';
      END IF;
      PERFORM shiba._register_compiled_stream_filter(
        target_oid,'left',source_oid,predicate_sql
      );
    END IF;
    SELECT string_agg(
      format('%L,to_jsonb(x.%I)',output_column,source_column),
      ',' ORDER BY ordinal
    ) INTO key_arguments
    FROM unnest(source_columns,output_columns)
      WITH ORDINALITY columns(source_column,output_column,ordinal);
    EXECUTE format(
      'INSERT INTO shiba_internal.projection_state(result_oid,row_key,multiplicity)
       SELECT $1,projected.row_key,count(*)
       FROM (
         SELECT jsonb_build_object(%s) row_key
         FROM %s x
         WHERE shiba._row_passes_filter($1,''left'',to_jsonb(x))
       ) projected
       GROUP BY projected.row_key',
      key_arguments,source_name
    ) USING target_oid;
    INSERT INTO shiba_internal.view_progress(result_oid) VALUES(target_oid);
    INSERT INTO shiba_internal.dag_worker_state(result_oid) VALUES(target_oid);
    PERFORM shiba._compile_stream_graph(target_oid,definition);
    SELECT EXISTS(
      SELECT 1 FROM pg_publication_rel pr JOIN pg_publication p ON p.oid=pr.prpubid
      WHERE p.pubname='shiba_publication' AND pr.prrelid=source_oid
    ) INTO source_in_publication;
    IF NOT source_in_publication THEN
      EXECUTE format('ALTER PUBLICATION shiba_publication ADD TABLE %s',source_name);
    END IF;
    EXECUTE format(
      'CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %s
       FOR EACH ROW EXECUTE FUNCTION shiba._request_worker()',
      format('shiba_wakeup_%s',target_oid),source_name
    );
    EXECUTE format(
      'CREATE TRIGGER %I BEFORE TRUNCATE ON %s
       FOR EACH STATEMENT EXECUTE FUNCTION shiba._reject_source_truncate()',
      format('shiba_no_truncate_%s',target_oid),source_name
    );
    EXECUTE format(
      'CREATE TRIGGER %I BEFORE INSERT OR UPDATE OR DELETE ON %s
       FOR EACH ROW EXECUTE FUNCTION shiba._protect_result_table()',
      format('shiba_protect_%s',target_oid),target_name
    );
    EXECUTE format('GRANT SELECT ON %s TO %I',target_name,session_user);
    EXECUTE format('ALTER TABLE %s OWNER TO %I',target_name,shiba_internal.extension_owner());
END;
$$;

CREATE FUNCTION shiba._register_topn_stream_table(query_text text, analysis jsonb)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog,shiba_internal,public
AS $$
DECLARE
    normalized text;
    declaration text[];
    definition text;
    source_oid oid;
    source_name text;
    target_oid oid;
    target_name text;
    order_spec jsonb;
    order_column_name name;
    target jsonb;
    current_source_column name;
    source_columns name[] := ARRAY[]::name[];
    output_columns name[] := ARRAY[]::name[];
    actual_outputs name[];
    predicate_sql text;
    activation_lsn_value pg_lsn;
    source_in_publication boolean;
    limit_value bigint;
    offset_value bigint;
BEGIN
    IF (analysis ->> 'has_window_functions')::boolean
       OR (analysis ->> 'has_sublinks')::boolean
       OR (analysis ->> 'has_aggregates')::boolean
       OR (analysis ->> 'has_distinct')::boolean
       OR (analysis ->> 'has_set_operations')::boolean
       OR (analysis ->> 'group_keys')::integer<>0
       OR jsonb_array_length(analysis -> 'sources')<>1
       OR jsonb_array_length(analysis -> 'joins')<>0
       OR jsonb_array_length(analysis -> 'ordering')<>1
       OR analysis ->> 'limit_count' IS NULL THEN
      RAISE EXCEPTION 'Shiba TopN requires one source, one ORDER BY column, and a constant LIMIT'
        USING ERRCODE='feature_not_supported',DETAIL=analysis::text;
    END IF;
    IF analysis -> 'where_predicate' ->> 'error' IS NOT NULL THEN
      RAISE EXCEPTION 'unsupported Shiba TopN filter: %',
        analysis -> 'where_predicate' ->> 'error'
        USING ERRCODE='feature_not_supported';
    END IF;
    limit_value := (analysis ->> 'limit_count')::bigint;
    offset_value := coalesce((analysis ->> 'limit_offset')::bigint,0);
    IF limit_value<=0 OR offset_value<0 THEN
      RAISE EXCEPTION 'Shiba TopN LIMIT must be positive and OFFSET nonnegative'
        USING ERRCODE='invalid_parameter_value';
    END IF;
    source_oid := (analysis -> 'sources' -> 0 ->> 'oid')::oid;
    order_spec := analysis -> 'ordering' -> 0;
    IF (order_spec ->> 'table_oid')::oid<>source_oid
       OR (order_spec ->> 'column')::smallint<=0 THEN
      RAISE EXCEPTION 'Shiba TopN ORDER BY must be an ordinary source column'
        USING ERRCODE='feature_not_supported';
    END IF;
    SELECT attname INTO STRICT order_column_name FROM pg_attribute
    WHERE attrelid=source_oid AND attnum=(order_spec ->> 'column')::smallint;
    FOR target IN
      SELECT value FROM jsonb_array_elements(analysis -> 'targets')
      WHERE NOT (value ->> 'resjunk')::boolean
    LOOP
      IF target ->> 'expression'<>'column'
         OR (target ->> 'origin_table_oid')::oid<>source_oid
         OR (target ->> 'origin_column')::smallint<=0
         OR target ->> 'name' IS NULL THEN
        RAISE EXCEPTION 'Shiba TopN outputs must be ordinary source columns'
          USING ERRCODE='feature_not_supported';
      END IF;
      SELECT attname INTO STRICT current_source_column FROM pg_attribute
      WHERE attrelid=source_oid
        AND attnum=(target ->> 'origin_column')::smallint;
      source_columns := array_append(source_columns,current_source_column);
      output_columns := array_append(output_columns,(target ->> 'name')::name);
    END LOOP;
    normalized := regexp_replace(regexp_replace(trim(query_text),'\s+',' ','g'),';$','');
    declaration := regexp_match(
      normalized,
      '^CREATE TABLE (IF NOT EXISTS )?shiba\.([a-z_][a-z_0-9]*) AS (SELECT .*)$',
      'i'
    );
    IF declaration IS NULL THEN
      RAISE EXCEPTION 'invalid Shiba TopN declaration'
        USING ERRCODE='feature_not_supported';
    END IF;
    definition := declaration[3];
    target_name := format('%I.%I','shiba',declaration[2]);
    target_oid := target_name::regclass;
    SELECT format('%I.%I',n.nspname,c.relname) INTO STRICT source_name
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=source_oid;
    SELECT array_agg(attname ORDER BY attnum) INTO actual_outputs FROM pg_attribute
    WHERE attrelid=target_oid AND attnum>0 AND NOT attisdropped;
    IF actual_outputs IS DISTINCT FROM output_columns THEN
      RAISE EXCEPTION 'Shiba TopN output metadata does not match the CTAS result'
        USING ERRCODE='data_exception';
    END IF;
    PERFORM shiba._validate_source_table(source_oid);
    PERFORM shiba._ensure_replica_identity_full(source_oid);
    activation_lsn_value := pg_current_wal_lsn();
    INSERT INTO shiba_internal.stream_views
      (result_oid,view_kind,source_oid,activation_lsn)
    VALUES(target_oid,'topn',source_oid,activation_lsn_value);
    INSERT INTO shiba_internal.topn_views
      (result_oid,order_column,order_direction,nulls_first,limit_count,
       limit_offset,source_columns,output_columns)
    VALUES(target_oid,order_column_name,order_spec ->> 'direction',
      (order_spec ->> 'nulls_first')::boolean,limit_value,offset_value,
      source_columns,output_columns);
    predicate_sql := analysis -> 'where_predicate' ->> 'sql';
    predicate_sql := replace(predicate_sql,format('input_%s',source_oid),'input');
    IF predicate_sql IS NOT NULL THEN
      IF jsonb_array_length(analysis -> 'where_predicate' -> 'source_oids')<>1
         OR (analysis -> 'where_predicate' -> 'source_oids' ->> 0)::oid<>source_oid THEN
        RAISE EXCEPTION 'Shiba TopN filter must reference its source'
          USING ERRCODE='feature_not_supported';
      END IF;
      PERFORM shiba._register_compiled_stream_filter(
        target_oid,'left',source_oid,predicate_sql
      );
    END IF;
    EXECUTE format(
      'INSERT INTO shiba_internal.topn_rows(result_oid,row_data,multiplicity)
       SELECT $1,canonical.row,count(*)
       FROM %s x
       CROSS JOIN LATERAL (
         SELECT jsonb_object_agg(key,value) row
         FROM jsonb_each_text(to_jsonb(x))
       ) canonical
       WHERE shiba._row_passes_filter($1,''left'',to_jsonb(x))
       GROUP BY canonical.row',
      source_name
    ) USING target_oid;
    INSERT INTO shiba_internal.view_progress(result_oid) VALUES(target_oid);
    INSERT INTO shiba_internal.dag_worker_state(result_oid) VALUES(target_oid);
    PERFORM shiba._compile_stream_graph(target_oid,definition);
    SELECT EXISTS(
      SELECT 1 FROM pg_publication_rel pr JOIN pg_publication p ON p.oid=pr.prpubid
      WHERE p.pubname='shiba_publication' AND pr.prrelid=source_oid
    ) INTO source_in_publication;
    IF NOT source_in_publication THEN
      EXECUTE format('ALTER PUBLICATION shiba_publication ADD TABLE %s',source_name);
    END IF;
    EXECUTE format(
      'CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %s
       FOR EACH ROW EXECUTE FUNCTION shiba._request_worker()',
      format('shiba_wakeup_%s',target_oid),source_name
    );
    EXECUTE format(
      'CREATE TRIGGER %I BEFORE TRUNCATE ON %s
       FOR EACH STATEMENT EXECUTE FUNCTION shiba._reject_source_truncate()',
      format('shiba_no_truncate_%s',target_oid),source_name
    );
    EXECUTE format(
      'CREATE TRIGGER %I BEFORE INSERT OR UPDATE OR DELETE ON %s
       FOR EACH ROW EXECUTE FUNCTION shiba._protect_result_table()',
      format('shiba_protect_%s',target_oid),target_name
    );
    EXECUTE format('GRANT SELECT ON %s TO %I',target_name,session_user);
    EXECUTE format('ALTER TABLE %s OWNER TO %I',target_name,shiba_internal.extension_owner());
END;
$$;

CREATE FUNCTION shiba._register_stream_table(query_text text, analysis jsonb)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, shiba_internal, public
AS $$
DECLARE
    normalized text;
    declaration_match text[];
    definition text;
    predicate_sql text;
    result_group_name name;
    group_column_name name;
    count_column_name name;
    count_distinct_value boolean;
    count_input_name name;
    sum_input_name name;
    sum_column_name name;
    source_oid oid;
    source_name text;
    target_name text;
    target_oid oid;
    trigger_name text;
    key_constraint_name text;
    output_columns name[];
    sum_is_not_null boolean;
    source_in_publication boolean;
    activation_lsn_value pg_lsn;
BEGIN
    IF coalesce((analysis ->> 'has_aggregate_filters')::boolean,false)
       OR coalesce((analysis ->> 'has_window_filters')::boolean,false) THEN
        RAISE EXCEPTION 'aggregate and window FILTER clauses are not yet executable by Shiba'
            USING ERRCODE='feature_not_supported';
    END IF;
    IF coalesce((analysis ->> 'limit_with_ties')::boolean,false) THEN
        RAISE EXCEPTION 'FETCH ... WITH TIES is not yet executable by Shiba'
            USING ERRCODE='feature_not_supported';
    END IF;
    IF (analysis ->> 'has_window_functions')::boolean THEN
        PERFORM shiba._register_window_stream_table(query_text,analysis);
        RETURN;
    END IF;
    IF (analysis ->> 'has_distinct')::boolean THEN
        PERFORM shiba._register_distinct_stream_table(query_text,analysis);
        RETURN;
    END IF;
    IF (analysis ->> 'has_limit')::boolean THEN
        PERFORM shiba._register_topn_stream_table(query_text,analysis);
        RETURN;
    END IF;
    IF (analysis ->> 'has_sublinks')::boolean THEN
        PERFORM shiba._register_subquery_stream_table(query_text,analysis);
        RETURN;
    END IF;
    IF (analysis ->> 'has_set_operations')::boolean
       OR (analysis ->> 'has_ordering')::boolean
       OR (analysis ->> 'has_limit')::boolean THEN
        RAISE EXCEPTION 'the analyzed PostgreSQL Query tree contains operators not yet executable by Shiba'
            USING ERRCODE = 'feature_not_supported', DETAIL = analysis::text;
    END IF;
    IF analysis -> 'having_predicate' ->> 'error' IS NOT NULL THEN
        RAISE EXCEPTION 'unsupported Shiba HAVING: %', analysis -> 'having_predicate' ->> 'error'
            USING ERRCODE = 'feature_not_supported';
    END IF;
    normalized := regexp_replace(trim(query_text), '\s+', ' ', 'g');
    normalized := regexp_replace(normalized, ';$', '');
    declaration_match := regexp_match(
        normalized,
        '^CREATE TABLE (IF NOT EXISTS )?shiba\.([a-z_][a-z_0-9]*) AS (SELECT .*)$',
        'i'
    );
    IF declaration_match IS NULL THEN
        RAISE EXCEPTION 'invalid Shiba table declaration'
            USING ERRCODE = 'feature_not_supported';
    END IF;

    definition := declaration_match[3];
    IF definition ~* '\mjoin\M' THEN
        PERFORM shiba._register_inner_join_stream_table(query_text, analysis);
        RETURN;
    END IF;
    IF jsonb_array_length(analysis -> 'sources') <> 1
       OR jsonb_array_length(analysis -> 'joins') <> 0 THEN
        RAISE EXCEPTION 'single-source Shiba aggregate requires exactly one relation source'
            USING ERRCODE = 'feature_not_supported';
    END IF;
    IF analysis -> 'where_predicate' ->> 'error' IS NOT NULL THEN
        RAISE EXCEPTION 'unsupported Shiba filter: %', analysis -> 'where_predicate' ->> 'error'
            USING ERRCODE = 'feature_not_supported';
    END IF;
    predicate_sql := analysis -> 'where_predicate' ->> 'sql';

    IF (analysis ->> 'group_keys')::integer <> 1
       OR jsonb_array_length(analysis -> 'targets') <> 3
       OR analysis -> 'targets' -> 0 ->> 'expression' <> 'column'
       OR (analysis -> 'targets' -> 0 ->> 'grouping_reference')::integer = 0
       OR analysis -> 'targets' -> 1 ->> 'expression' <> 'aggregate'
       OR lower(analysis -> 'targets' -> 1 ->> 'aggregate') <> 'count'
       OR NOT (
         ((analysis -> 'targets' -> 1 ->> 'aggregate_star')::boolean
          AND NOT (analysis -> 'targets' -> 1 ->> 'aggregate_distinct')::boolean)
         OR
         (NOT (analysis -> 'targets' -> 1 ->> 'aggregate_star')::boolean
          AND (analysis -> 'targets' -> 1 ->> 'aggregate_distinct')::boolean
          AND (analysis -> 'targets' -> 1 ->> 'input_table_oid')::oid <> 0
          AND (analysis -> 'targets' -> 1 ->> 'input_column')::smallint > 0)
       )
       OR analysis -> 'targets' -> 2 ->> 'expression' <> 'aggregate'
       OR lower(analysis -> 'targets' -> 2 ->> 'aggregate') <> 'sum'
       OR (analysis -> 'targets' -> 2 ->> 'aggregate_distinct')::boolean THEN
        RAISE EXCEPTION
            'Shiba supports SELECT group_key, count(*) or count(DISTINCT column), sum(value) FROM source GROUP BY group_key'
            USING ERRCODE = 'feature_not_supported', DETAIL = analysis::text;
    END IF;

    result_group_name := (analysis -> 'targets' -> 0 ->> 'name')::name;
    count_column_name := (analysis -> 'targets' -> 1 ->> 'name')::name;
    count_distinct_value := (analysis -> 'targets' -> 1 ->> 'aggregate_distinct')::boolean;
    sum_column_name := (analysis -> 'targets' -> 2 ->> 'name')::name;
    source_oid := (analysis -> 'sources' -> 0 ->> 'oid')::oid;
    IF jsonb_array_length(
         coalesce(analysis -> 'having_distinct_inputs','[]'::jsonb)
       )>0
       AND (
         NOT count_distinct_value
         OR jsonb_array_length(analysis -> 'having_distinct_inputs')<>1
         OR (analysis -> 'having_distinct_inputs' -> 0 ->> 'table_oid')::oid
              <>source_oid
         OR (analysis -> 'having_distinct_inputs' -> 0 ->> 'column')::smallint
              <>(analysis -> 'targets' -> 1 ->> 'input_column')::smallint
       ) THEN
      RAISE EXCEPTION 'HAVING COUNT(DISTINCT) must match the maintained SELECT aggregate'
        USING ERRCODE='feature_not_supported';
    END IF;
    IF jsonb_array_length(coalesce(analysis -> 'having_sum_inputs','[]'::jsonb))>0
       AND (
         jsonb_array_length(analysis -> 'having_sum_inputs')<>1
         OR (analysis -> 'having_sum_inputs' -> 0 ->> 'table_oid')::oid<>source_oid
         OR (analysis -> 'having_sum_inputs' -> 0 ->> 'column')::smallint
              <>(analysis -> 'targets' -> 2 ->> 'input_column')::smallint
       ) THEN
      RAISE EXCEPTION 'HAVING SUM must match the maintained SELECT aggregate'
        USING ERRCODE='feature_not_supported';
    END IF;
    predicate_sql := replace(predicate_sql,format('input_%s',source_oid),'input');
    IF (analysis -> 'targets' -> 0 ->> 'origin_table_oid')::oid <> source_oid
       OR (count_distinct_value
           AND (analysis -> 'targets' -> 1 ->> 'input_table_oid')::oid <> source_oid)
       OR (analysis -> 'targets' -> 2 ->> 'input_table_oid')::oid <> source_oid THEN
        RAISE EXCEPTION 'Shiba group and SUM expressions must read the registered source table'
            USING ERRCODE = 'feature_not_supported';
    END IF;
    SELECT attname INTO STRICT group_column_name
    FROM pg_attribute
    WHERE attrelid = source_oid
      AND attnum = (analysis -> 'targets' -> 0 ->> 'origin_column')::smallint;
    SELECT attname INTO STRICT sum_input_name
    FROM pg_attribute
    WHERE attrelid = source_oid
      AND attnum = (analysis -> 'targets' -> 2 ->> 'input_column')::smallint;
    IF count_distinct_value THEN
        SELECT attname INTO STRICT count_input_name
        FROM pg_attribute
        WHERE attrelid=source_oid
          AND attnum=(analysis -> 'targets' -> 1 ->> 'input_column')::smallint;
    END IF;
    PERFORM shiba._validate_source_table(source_oid);

    SELECT format('%I.%I', source_namespace.nspname, source.relname)
    INTO source_name
    FROM pg_class AS source
    JOIN pg_namespace AS source_namespace ON source_namespace.oid = source.relnamespace
    WHERE source.oid = source_oid;

    target_name := format('%I.%I', 'shiba', declaration_match[2]);
    target_oid := target_name::regclass;

    SELECT array_agg(attribute.attname ORDER BY attribute.attnum)
    INTO output_columns
    FROM pg_attribute AS attribute
    WHERE attribute.attrelid = target_oid
      AND attribute.attnum > 0
      AND NOT attribute.attisdropped;
    IF output_columns IS DISTINCT FROM ARRAY[
        result_group_name,
        count_column_name,
        sum_column_name
    ] THEN
        RAISE EXCEPTION
            'Shiba v0.1 requires an unaliased group key and explicit count/sum aliases'
            USING ERRCODE = 'feature_not_supported';
    END IF;

    SELECT attribute.attnotnull
    INTO sum_is_not_null
    FROM pg_attribute AS attribute
    WHERE attribute.attrelid = source_oid
      AND attribute.attname = sum_input_name
      AND attribute.attnum > 0
      AND NOT attribute.attisdropped;
    IF sum_is_not_null IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'Shiba v0.1 requires SUM input column % to be NOT NULL', sum_input_name
            USING ERRCODE = 'feature_not_supported';
    END IF;

    -- WAL must contain the complete old row so an UPDATE can be represented as
    -- a deletion plus an insertion in the aggregate state.
    PERFORM shiba._ensure_replica_identity_full(source_oid);

    key_constraint_name := format('shiba_key_%s', target_oid);
    EXECUTE format(
        'ALTER TABLE %s ADD CONSTRAINT %I UNIQUE NULLS NOT DISTINCT (%I)',
        target_name,
        key_constraint_name,
        result_group_name
    );

    activation_lsn_value := pg_current_wal_lsn();
    INSERT INTO shiba_internal.stream_views (
        result_oid,
        source_oid,
        group_column,
        result_group_column,
        count_column,
        count_distinct,
        count_input_source,
        count_input_column,
        sum_input_column,
        sum_column,
        activation_lsn
    ) VALUES (
        target_oid,
        source_oid,
        group_column_name,
        result_group_name,
        count_column_name,
        count_distinct_value,
        CASE WHEN count_distinct_value THEN 'left' ELSE NULL END,
        count_input_name,
        sum_input_name,
        sum_column_name,
        activation_lsn_value
    );
    IF analysis -> 'having_predicate' ->> 'sql' IS NOT NULL THEN
        INSERT INTO shiba_internal.stream_having(result_oid,predicate_sql)
        VALUES(target_oid,analysis -> 'having_predicate' ->> 'sql');
    END IF;
    IF predicate_sql IS NOT NULL THEN
        IF jsonb_array_length(analysis -> 'where_predicate' -> 'source_oids') <> 1
           OR (analysis -> 'where_predicate' -> 'source_oids' ->> 0)::oid <> source_oid THEN
            RAISE EXCEPTION 'Shiba filter does not reference the registered source'
                USING ERRCODE = 'feature_not_supported';
        END IF;
        PERFORM shiba._register_compiled_stream_filter(
            target_oid, 'left', source_oid, predicate_sql
        );
    END IF;
    INSERT INTO shiba_internal.view_progress (result_oid) VALUES (target_oid);
    INSERT INTO shiba_internal.dag_worker_state (result_oid) VALUES (target_oid);
    PERFORM shiba._compile_stream_graph(target_oid, definition);
    PERFORM shiba._initialize_aggregate_state(target_oid);

    SELECT EXISTS (
        SELECT 1
        FROM pg_publication_rel AS publication_relation
        JOIN pg_publication AS publication ON publication.oid = publication_relation.prpubid
        WHERE publication.pubname = 'shiba_publication'
          AND publication_relation.prrelid = source_oid
    ) INTO source_in_publication;
    IF NOT source_in_publication THEN
        EXECUTE format('ALTER PUBLICATION shiba_publication ADD TABLE %s', source_name);
    END IF;

    trigger_name := format('shiba_wakeup_%s', target_oid);
    EXECUTE format(
        'CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION shiba._request_worker()',
        trigger_name,
        source_name
    );
    EXECUTE format(
        'CREATE TRIGGER %I BEFORE TRUNCATE ON %s FOR EACH STATEMENT EXECUTE FUNCTION shiba._reject_source_truncate()',
        format('shiba_no_truncate_%s', target_oid),
        source_name
    );
    EXECUTE format(
        'CREATE TRIGGER %I BEFORE INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION shiba._protect_result_table()',
        format('shiba_protect_%s', target_oid),
        target_name
    );
    EXECUTE format('GRANT SELECT ON %s TO %I', target_name, session_user);
    EXECUTE format('ALTER TABLE %s OWNER TO %I', target_name, shiba_internal.extension_owner());
END;
$$;
