\set target_group random(0, 9999)
SELECT group_id, row_count, total_amount
FROM shiba.bench_stats
WHERE group_id = :target_group;
