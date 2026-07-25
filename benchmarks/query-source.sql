\set target_group random(0, 9999)
SELECT group_id, count(*) AS row_count, sum(amount) AS total_amount
FROM bench_events
WHERE group_id = :target_group
GROUP BY group_id;
