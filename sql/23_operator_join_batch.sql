-- JOIN transitions remain ordered because outer/semi/anti visibility depends
-- on first/last-match boundaries. During a commit, defer aggregate sink writes
-- and remember each affected group so it is synchronized once after every
-- arrangement transition has completed.
CREATE FUNCTION shiba._begin_join_batch(result_relation oid)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, shiba_internal
AS $$
BEGIN
    IF to_regclass('pg_temp.shiba_join_batch_groups') IS NULL THEN
      CREATE TEMP TABLE shiba_join_batch_groups (
        result_oid oid NOT NULL,
        group_key jsonb NOT NULL,
        PRIMARY KEY(result_oid,group_key)
      ) ON COMMIT DELETE ROWS;
    END IF;
    DELETE FROM pg_temp.shiba_join_batch_groups
    WHERE result_oid=result_relation;
END;
$$;

CREATE FUNCTION shiba._finish_join_batch(result_relation oid)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, shiba, shiba_internal
AS $$
DECLARE
    stream_view shiba_internal.stream_views%ROWTYPE;
    affected_group jsonb;
BEGIN
    SELECT * INTO STRICT stream_view
    FROM shiba_internal.stream_views
    WHERE result_oid=result_relation;
    FOR affected_group IN
      SELECT group_key
      FROM pg_temp.shiba_join_batch_groups
      WHERE result_oid=result_relation
      ORDER BY group_key::text
    LOOP
      PERFORM shiba._sync_aggregate_sink(stream_view,affected_group);
    END LOOP;
    DELETE FROM pg_temp.shiba_join_batch_groups
    WHERE result_oid=result_relation;
END;
$$;
