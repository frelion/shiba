\set group_seed random(1, 10000)
INSERT INTO bench_events (event_id, group_id, amount)
SELECT nextval('bench_event_id_seq'),
       (:group_seed + value) % 10000,
       1 + ((:group_seed * 31 + value) % 1000)
FROM generate_series(1, :batch_size) AS value;
