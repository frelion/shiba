\set ON_ERROR_STOP on

CREATE TABLE public.batch_distinct_source (
    row_id integer NOT NULL,
    group_id integer NOT NULL,
    customer_id integer,
    amount integer NOT NULL
);
CREATE TABLE public.batch_distinct_expected
    (LIKE public.batch_distinct_source);

CREATE TABLE shiba.batch_distinct_result AS
SELECT group_id,
       count(DISTINCT customer_id) AS customer_count,
       sum(amount) AS total_amount
FROM public.batch_distinct_source
GROUP BY group_id
HAVING count(DISTINCT customer_id) >= 10;

-- Seventy events force the aggregate batch path. Repeated values and NULL
-- exercise collisions without contributing extra distinct counts.
WITH batch_rows AS (
    SELECT row_id,
           1 AS group_id,
           CASE WHEN row_id % 10 = 0 THEN NULL ELSE row_id % 20 END
             AS customer_id,
           row_id AS amount
    FROM generate_series(1,70) row_id
),
payload AS (
    SELECT jsonb_agg(
             jsonb_build_object(
               'source_oid','public.batch_distinct_source'::regclass::oid,
               'row_data',to_jsonb(batch_rows),
               'delta',1
             )
             ORDER BY row_id
           ) AS events
    FROM batch_rows
)
SELECT shiba._apply_dag_delta_batch(
         'shiba.batch_distinct_result'::regclass,
         shiba._logical_execution_descriptor(
           'shiba.batch_distinct_result'::regclass
         ),
         events,
         '0/100'
       )
FROM payload;

INSERT INTO public.batch_distinct_expected
SELECT row_id,
       1,
       CASE WHEN row_id % 10 = 0 THEN NULL ELSE row_id % 20 END,
       row_id
FROM generate_series(1,70) row_id;

-- One commit retracts every old row and inserts its migrated replacement.
-- This covers group migration, duplicate collisions and NULL transitions.
WITH old_rows AS (
    SELECT * FROM public.batch_distinct_expected
),
new_rows AS (
    SELECT row_id,
           CASE WHEN row_id <= 35 THEN 2 ELSE 3 END AS group_id,
           CASE WHEN row_id % 7 = 0 THEN NULL ELSE row_id % 12 END
             AS customer_id,
           amount + 100 AS amount
    FROM old_rows
),
ordered_events AS (
    SELECT row_id * 2 - 1 AS sequence,to_jsonb(old_rows) AS row_data,-1 AS delta
    FROM old_rows
    UNION ALL
    SELECT row_id * 2,to_jsonb(new_rows) AS row_data,1 AS delta
    FROM new_rows
),
payload AS (
    SELECT jsonb_agg(
             jsonb_build_object(
               'source_oid','public.batch_distinct_source'::regclass::oid,
               'row_data',row_data,
               'delta',delta
             )
             ORDER BY sequence
           ) AS events
    FROM ordered_events
)
SELECT shiba._apply_dag_delta_batch(
         'shiba.batch_distinct_result'::regclass,
         shiba._logical_execution_descriptor(
           'shiba.batch_distinct_result'::regclass
         ),
         events,
         '0/200'
       )
FROM payload;

UPDATE public.batch_distinct_expected
SET group_id=CASE WHEN row_id <= 35 THEN 2 ELSE 3 END,
    customer_id=CASE WHEN row_id % 7 = 0 THEN NULL ELSE row_id % 12 END,
    amount=amount+100;

DO $test$
BEGIN
  IF EXISTS (
    WITH expected AS (
      SELECT group_id,count(DISTINCT customer_id) AS customer_count,
             sum(amount) AS total_amount
      FROM public.batch_distinct_expected
      GROUP BY group_id
      HAVING count(DISTINCT customer_id)>=10
    )
    (SELECT * FROM expected EXCEPT ALL
     SELECT * FROM shiba.batch_distinct_result)
    UNION ALL
    (SELECT * FROM shiba.batch_distinct_result EXCEPT ALL
     SELECT * FROM expected)
  ) THEN
    RAISE EXCEPTION 'batch DISTINCT result differs after group migration';
  END IF;

  IF EXISTS (
    WITH expected AS (
      SELECT to_jsonb(group_id) AS group_key,
             to_jsonb(customer_id) AS value_key,
             count(*)::bigint AS multiplicity
      FROM public.batch_distinct_expected
      WHERE customer_id IS NOT NULL
      GROUP BY group_id,customer_id
    ),
    actual AS (
      SELECT group_key,value_key,multiplicity
      FROM shiba_internal.distinct_state
      WHERE result_oid='shiba.batch_distinct_result'::regclass
    )
    (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
    UNION ALL
    (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
  ) THEN
    RAISE EXCEPTION 'batch DISTINCT multiplicity state differs';
  END IF;
END
$test$;

-- Removing thirty rows leaves group 2 below HAVING while group 3 remains
-- visible. The batch still exceeds the crossover by adding harmless +1/-1
-- collision pairs for an existing group.
WITH deleted_rows AS (
    SELECT * FROM public.batch_distinct_expected WHERE row_id <= 30
),
filler AS (
    SELECT 1000 + n AS row_id,3 AS group_id,500 + n AS customer_id,
           n AS amount
    FROM generate_series(1,17) n
),
ordered_events AS (
    SELECT row_id AS sequence,to_jsonb(deleted_rows) AS row_data,-1 AS delta
    FROM deleted_rows
    UNION ALL
    SELECT 100 + row_id * 2,to_jsonb(filler) AS row_data,1 AS delta
    FROM filler
    UNION ALL
    SELECT 101 + row_id * 2,to_jsonb(filler) AS row_data,-1 AS delta
    FROM filler
),
payload AS (
    SELECT jsonb_agg(
             jsonb_build_object(
               'source_oid','public.batch_distinct_source'::regclass::oid,
               'row_data',row_data,
               'delta',delta
             )
             ORDER BY sequence
           ) AS events
    FROM ordered_events
)
SELECT shiba._apply_dag_delta_batch(
         'shiba.batch_distinct_result'::regclass,
         shiba._logical_execution_descriptor(
           'shiba.batch_distinct_result'::regclass
         ),
         events,
         '0/300'
       )
FROM payload;

DELETE FROM public.batch_distinct_expected WHERE row_id <= 30;

DO $test$
BEGIN
  IF EXISTS (
    WITH expected AS (
      SELECT group_id,count(DISTINCT customer_id) AS customer_count,
             sum(amount) AS total_amount
      FROM public.batch_distinct_expected
      GROUP BY group_id
      HAVING count(DISTINCT customer_id)>=10
    )
    (SELECT * FROM expected EXCEPT ALL
     SELECT * FROM shiba.batch_distinct_result)
    UNION ALL
    (SELECT * FROM shiba.batch_distinct_result EXCEPT ALL
     SELECT * FROM expected)
  ) THEN
    RAISE EXCEPTION 'batch DISTINCT HAVING transition differs';
  END IF;
END
$test$;

-- A net-zero key still cannot retract a value before it exists. The batch
-- update must fail atomically even though its final multiplicity would be 0.
DO $test$
DECLARE
  invalid_events jsonb;
BEGIN
  WITH event_rows AS (
    SELECT 1 AS sequence,
           jsonb_build_object(
             'row_id',9000,'group_id',3,'customer_id',999,'amount',1
           ) AS row_data,
           -1 AS delta
    UNION ALL
    SELECT 2,
           jsonb_build_object(
             'row_id',9000,'group_id',3,'customer_id',999,'amount',1
           ),
           1
    UNION ALL
    SELECT 2+n*2,
           jsonb_build_object(
             'row_id',9100+n,'group_id',3,'customer_id',1000+n,'amount',1
           ),
           1
    FROM generate_series(1,31) n
    UNION ALL
    SELECT 3+n*2,
           jsonb_build_object(
             'row_id',9100+n,'group_id',3,'customer_id',1000+n,'amount',1
           ),
           -1
    FROM generate_series(1,31) n
  )
  SELECT jsonb_agg(
           jsonb_build_object(
             'source_oid','public.batch_distinct_source'::regclass::oid,
             'row_data',row_data,
             'delta',delta
           )
           ORDER BY sequence
         )
  INTO invalid_events
  FROM event_rows;

  BEGIN
    PERFORM shiba._apply_dag_delta_batch(
      'shiba.batch_distinct_result'::regclass,
      shiba._logical_execution_descriptor(
        'shiba.batch_distinct_result'::regclass
      ),
      invalid_events,
      '0/400'
    );
    RAISE EXCEPTION 'invalid DISTINCT prefix unexpectedly succeeded';
  EXCEPTION WHEN data_corrupted THEN
    NULL;
  END;
END
$test$;

DROP TABLE shiba.batch_distinct_result;
DROP TABLE public.batch_distinct_expected;
DROP TABLE public.batch_distinct_source;
