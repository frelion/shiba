"""Declarative workload catalog for the full Shiba performance matrix.

Every schema-qualified SQL fragment uses ``{schema}``; the runner executes the
same setup and mutations once against an unregistered PostgreSQL baseline and
once against the Shiba source.  This keeps source-side comparisons identical.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Action:
    name: str
    sql: str
    affected_rows: int
    boundary: str


@dataclass(frozen=True)
class Scenario:
    name: str
    family: str
    profile: str
    setup_sql: str
    defining_query: str
    required_operators: tuple[str, ...]
    actions: tuple[Action, ...]
    source_rows: int
    notes: str


def _event_setup(
    rows: int,
    groups: int,
    *,
    hotspot: bool = False,
    nullable_score: bool = False,
) -> str:
    category = (
        f"CASE WHEN value % 10 < 8 THEN 0 ELSE value % {groups} END"
        if hotspot
        else f"CASE WHEN value % 97 = 0 THEN NULL ELSE value % {groups} END"
    )
    score = (
        "CASE WHEN value % 101 = 0 THEN NULL ELSE (value * 37) % 10000 END"
        if nullable_score
        else "(value * 37) % 10000"
    )
    return f"""
CREATE SCHEMA {{schema}};
CREATE TABLE {{schema}}.events (
  row_id integer NOT NULL,
  category_id integer,
  customer_id integer,
  label integer NOT NULL,
  amount integer NOT NULL,
  score integer,
  active boolean NOT NULL
);
INSERT INTO {{schema}}.events
SELECT value,
       {category},
       CASE WHEN value % 89 = 0 THEN NULL ELSE value % 257 END,
       value % 211,
       1 + ((value * 31) % 1000),
       {score},
       value % 3 <> 0
FROM generate_series(1,{rows}) AS value;
ANALYZE {{schema}}.events;
"""


def _event_actions(mutations: int) -> tuple[Action, ...]:
    return (
        Action(
            "insert",
            f"""
INSERT INTO {{schema}}.events
SELECT 10000000 + value,
       CASE WHEN value % 7 = 0 THEN NULL ELSE value % 113 END,
       CASE WHEN value % 11 = 0 THEN NULL ELSE value % 257 END,
       value % 211,
       CASE WHEN value % 2 = 0 THEN 500 ELSE 499 END,
       CASE WHEN value % 13 = 0 THEN NULL ELSE 1000000 - value END,
       value % 2 = 0
FROM generate_series(1,{mutations}) AS value
""",
            mutations,
            "new groups; NULL; filter boundary; TopN boundary; peers",
        ),
        Action(
            "update",
            f"""
UPDATE {{schema}}.events
SET category_id = CASE WHEN row_id % 5 = 0 THEN NULL ELSE category_id + 1000 END,
    customer_id = customer_id + 1000,
    label = label + 1000,
    amount = CASE WHEN amount < 500 THEN 750 ELSE 250 END,
    score = CASE WHEN score IS NULL THEN 99999 ELSE 99999 - score END,
    active = NOT active
WHERE row_id BETWEEN 1 AND {mutations}
""",
            mutations,
            "group/key/partition/order migration and filter crossing",
        ),
        Action(
            "delete",
            f"DELETE FROM {{schema}}.events WHERE row_id BETWEEN {mutations + 1} AND {mutations * 2}",
            mutations,
            "retraction and state removal",
        ),
    )


def _topn_actions(mutations: int) -> tuple[Action, ...]:
    half = max(mutations // 2, 1)
    return (
        Action(
            "insert_inside_outside",
            f"""
INSERT INTO {{schema}}.events
SELECT 11000000 + value, value % 17, value % 31, value, value,
       CASE WHEN value <= {half} THEN 100000 + value ELSE -100000 - value END,
       true
FROM generate_series(1,{mutations}) value
""",
            mutations,
            "rows inside and outside bounded result",
        ),
        Action(
            "update_order_boundary",
            f"""
UPDATE {{schema}}.events
SET score = CASE WHEN row_id <= {half} THEN 200000 + row_id ELSE -200000 - row_id END
WHERE row_id BETWEEN 1 AND {mutations}
""",
            mutations,
            "order changes across TopN boundary",
        ),
        Action(
            "delete_ranked_candidates",
            f"DELETE FROM {{schema}}.events WHERE row_id BETWEEN 1 AND {mutations}",
            mutations,
            "delete prior ranked and non-ranked candidates",
        ),
    )


def _join_setup(rows: int, keys: int, fanout: int) -> str:
    dim_rows = keys * fanout
    return f"""
CREATE SCHEMA {{schema}};
CREATE TABLE {{schema}}.facts (
  row_id integer NOT NULL,
  join_key integer,
  amount integer NOT NULL,
  gate integer NOT NULL
);
CREATE TABLE {{schema}}.dims (
  row_id integer NOT NULL,
  join_key integer,
  group_id integer,
  threshold integer NOT NULL,
  gate integer NOT NULL
);
INSERT INTO {{schema}}.facts
SELECT value,
       CASE WHEN value % 101 = 0 THEN NULL ELSE value % {keys * 2} END,
       1 + ((value * 17) % 1000),
       value % 2
FROM generate_series(1,{rows}) value;
INSERT INTO {{schema}}.dims
SELECT value,
       CASE WHEN value % 103 = 0 THEN NULL ELSE (value - 1) % {keys} END,
       ((value - 1) % {keys}) % 101,
       1 + ((value * 13) % 700),
       value % 2
FROM generate_series(1,{dim_rows}) value;
ANALYZE {{schema}}.facts;
ANALYZE {{schema}}.dims;
"""


def _join_actions(mutations: int, keys: int, fanout: int) -> tuple[Action, ...]:
    dim_start = keys * fanout + 1
    return (
        Action(
            "left_insert",
            f"""
INSERT INTO {{schema}}.facts
SELECT 12000000 + value,
       CASE WHEN value % 3 = 0 THEN NULL
            WHEN value % 2 = 0 THEN value % {keys}
            ELSE {keys} + value % {keys} END,
       400 + value, value % 2
FROM generate_series(1,{mutations}) value
""",
            mutations,
            "left matched/unmatched/NULL insertion",
        ),
        Action(
            "left_update_key",
            f"""
UPDATE {{schema}}.facts
SET join_key = CASE WHEN row_id % 3 = 0 THEN NULL
                    WHEN join_key < {keys} THEN join_key + {keys}
                    ELSE join_key % {keys} END,
    amount = amount + 37,
    gate = 1 - gate
WHERE row_id BETWEEN 1 AND {mutations}
""",
            mutations,
            "left join-key 0↔1 match and NULL boundary",
        ),
        Action(
            "left_delete",
            f"DELETE FROM {{schema}}.facts WHERE row_id BETWEEN {mutations + 1} AND {mutations * 2}",
            mutations,
            "left retraction",
        ),
        Action(
            "right_insert_first_match",
            f"""
INSERT INTO {{schema}}.dims
SELECT {dim_start} + value,
       {keys} + value % {keys},
       500 + value % 17,
       450,
       value % 2
FROM generate_series(1,{mutations}) value
""",
            mutations,
            "right 0→1 match boundary for previously unmatched facts",
        ),
        Action(
            "right_update_key",
            f"""
UPDATE {{schema}}.dims
SET join_key = CASE WHEN row_id % 4 = 0 THEN NULL
                    ELSE join_key + {keys} END,
    group_id = group_id + 1000,
    threshold = threshold + 100,
    gate = 1 - gate
WHERE row_id BETWEEN 1 AND {mutations}
""",
            mutations,
            "right join-key/group/filter migration",
        ),
        Action(
            "right_delete_last_match",
            f"DELETE FROM {{schema}}.dims WHERE row_id BETWEEN {dim_start + 1} AND {dim_start + mutations}",
            mutations,
            "right 1→0 match boundary",
        ),
    )


def _sublink_setup(rows: int, keys: int) -> str:
    return f"""
CREATE SCHEMA {{schema}};
CREATE TABLE {{schema}}.orders (
  row_id integer NOT NULL,
  join_key integer,
  amount integer NOT NULL
);
CREATE TABLE {{schema}}.permits (
  row_id integer NOT NULL,
  join_key integer
);
INSERT INTO {{schema}}.orders
SELECT value,
       CASE WHEN value % 101 = 0 THEN NULL ELSE value % {keys * 2} END,
       1 + ((value * 19) % 1000)
FROM generate_series(1,{rows}) value;
INSERT INTO {{schema}}.permits
SELECT value,
       CASE WHEN value = {keys + 1} THEN NULL ELSE (value - 1) % {keys} END
FROM generate_series(1,{keys + 1}) value;
ANALYZE {{schema}}.orders;
ANALYZE {{schema}}.permits;
"""


def _sublink_actions(mutations: int, keys: int) -> tuple[Action, ...]:
    return (
        Action(
            "left_insert",
            f"""
INSERT INTO {{schema}}.orders
SELECT 13000000 + value,
       CASE WHEN value % 3 = 0 THEN NULL
            WHEN value % 2 = 0 THEN value % {keys}
            ELSE {keys} + value % {keys} END,
       600 + value
FROM generate_series(1,{mutations}) value
""",
            mutations,
            "left matched/unmatched/NULL",
        ),
        Action(
            "left_update_key",
            f"""
UPDATE {{schema}}.orders
SET join_key = CASE WHEN row_id % 3 = 0 THEN NULL
                    WHEN join_key < {keys} THEN join_key + {keys}
                    ELSE join_key % {keys} END,
    amount = amount + 41
WHERE row_id BETWEEN 1 AND {mutations}
""",
            mutations,
            "left match and NULL transition",
        ),
        Action(
            "left_delete",
            f"DELETE FROM {{schema}}.orders WHERE row_id BETWEEN {mutations + 1} AND {mutations * 2}",
            mutations,
            "left retraction",
        ),
        Action(
            "right_insert_first_match",
            f"""
INSERT INTO {{schema}}.permits
SELECT 14000000 + value, {keys} + value % {keys}
FROM generate_series(1,{mutations}) value
""",
            mutations,
            "right 0→1 match boundary",
        ),
        Action(
            "right_insert_duplicate",
            f"""
INSERT INTO {{schema}}.permits
SELECT 14500000 + value,value % {keys}
FROM generate_series(1,{mutations}) value
""",
            mutations,
            "right multiplicity 1→2 without visibility change",
        ),
        Action(
            "right_delete_non_last",
            f"DELETE FROM {{schema}}.permits WHERE row_id BETWEEN 14500001 AND {14500000 + mutations}",
            mutations,
            "right multiplicity 2→1 without visibility change",
        ),
        Action(
            "right_delete_null",
            f"DELETE FROM {{schema}}.permits WHERE row_id = {keys + 1}",
            1,
            "right NULL removal for NOT IN global visibility",
        ),
        Action(
            "right_reinsert_null",
            "INSERT INTO {schema}.permits VALUES (15000000,NULL)",
            1,
            "right NULL insertion for NOT IN global suppression",
        ),
        Action(
            "right_delete_last_match",
            f"DELETE FROM {{schema}}.permits WHERE join_key BETWEEN 0 AND {mutations - 1}",
            mutations,
            "right multiplicity 1→0 match boundary",
        ),
    )


def build_scenarios(
    *,
    rows: int,
    groups: int,
    mutations: int,
) -> list[Scenario]:
    event_actions = _event_actions(mutations)
    window_unique_actions = tuple(
        Action(
            action.name,
            action.sql.replace(
                "CASE WHEN value % 13 = 0 THEN NULL ELSE 1000000 - value END",
                "1000000 - value",
            ),
            action.affected_rows,
            action.boundary,
        )
        for action in event_actions
    )
    scenarios: list[Scenario] = [
        Scenario(
            "aggregate_low_cardinality",
            "aggregate",
            "10 groups; uniform",
            _event_setup(rows, 10),
            """SELECT category_id AS group_key,count(*) AS row_count,
                      sum(amount) AS total_amount
               FROM {schema}.events GROUP BY category_id""",
            ("scan", "aggregate", "project", "sink"),
            event_actions,
            rows,
            "COUNT(*) and SUM with low group cardinality",
        ),
        Scenario(
            "aggregate_high_cardinality",
            "aggregate",
            "one group per row",
            _event_setup(rows, groups),
            """SELECT row_id AS group_key,count(*) AS row_count,
                      sum(amount) AS total_amount
               FROM {schema}.events GROUP BY row_id""",
            ("scan", "aggregate", "project", "sink"),
            (
                event_actions[0],
                Action(
                    "update_group_key",
                    f"""
UPDATE {{schema}}.events
SET row_id=17000000+row_id,amount=amount+17
WHERE row_id BETWEEN 1 AND {mutations}
""",
                    mutations,
                    "high-cardinality group-key migration",
                ),
                event_actions[2],
            ),
            rows,
            "COUNT(*) and SUM with high cardinality",
        ),
        Scenario(
            "aggregate_hotspot",
            "aggregate",
            "80% of rows in one group",
            _event_setup(rows, groups, hotspot=True),
            """SELECT category_id AS group_key,count(*) AS row_count,
                      sum(amount) AS total_amount
               FROM {schema}.events GROUP BY category_id""",
            ("scan", "aggregate", "project", "sink"),
            event_actions,
            rows,
            "skewed aggregate state and write hotspot",
        ),
        Scenario(
            "filter_project",
            "filter",
            "AND/OR/NOT/IS NULL and aliases",
            _event_setup(rows, groups),
            """SELECT category_id AS projected_group,count(*) AS projected_count,
                      sum(amount) AS projected_sum
               FROM {schema}.events
               WHERE (active = true AND amount >= 500)
                  OR (NOT active AND category_id IS NULL)
               GROUP BY category_id""",
            ("scan", "filter", "aggregate", "project", "sink"),
            event_actions,
            rows,
            "typed filter boundary and Project rename",
        ),
        Scenario(
            "having",
            "having",
            "visibility threshold",
            _event_setup(rows, groups),
            f"""SELECT category_id AS group_key,count(*) AS row_count,
                       sum(amount) AS total_amount
                FROM {{schema}}.events GROUP BY category_id
                HAVING count(*) >= {max(rows // groups, 2)}""",
            ("scan", "aggregate", "having", "project", "sink"),
            event_actions,
            rows,
            "hidden aggregate state crosses HAVING threshold",
        ),
        Scenario(
            "count_distinct_low",
            "distinct_aggregate",
            "customer cardinality 257",
            _event_setup(rows, groups),
            """SELECT category_id AS group_key,
                      count(DISTINCT customer_id) AS customer_count,
                      sum(amount) AS total_amount
               FROM {schema}.events GROUP BY category_id""",
            ("scan", "distinct", "aggregate", "project", "sink"),
            event_actions,
            rows,
            "COUNT(DISTINCT) multiplicity state",
        ),
        Scenario(
            "count_distinct_high",
            "distinct_aggregate",
            "customer cardinality approximately rows",
            _event_setup(rows, groups).replace(
                "CASE WHEN value % 89 = 0 THEN NULL ELSE value % 257 END",
                "CASE WHEN value % 89 = 0 THEN NULL ELSE value END",
            ),
            """SELECT category_id AS group_key,
                      count(DISTINCT customer_id) AS customer_count,
                      sum(amount) AS total_amount
               FROM {schema}.events GROUP BY category_id""",
            ("scan", "distinct", "aggregate", "project", "sink"),
            event_actions,
            rows,
            "high-cardinality COUNT(DISTINCT)",
        ),
        Scenario(
            "top_level_distinct",
            "distinct",
            "duplicate projected keys",
            _event_setup(rows, groups),
            "SELECT DISTINCT category_id AS projected_group,label FROM {schema}.events",
            ("scan", "distinct", "project", "sink"),
            event_actions,
            rows,
            "top-level DISTINCT threshold crossings",
        ),
        Scenario(
            "topn_limit",
            "topn",
            "LIMIT 50; DESC NULLS LAST; deterministic boundary",
            _event_setup(rows, groups).replace(
                "(value * 37) % 10000",
                "CASE WHEN value = 1 THEN NULL ELSE value END",
            ),
            """SELECT row_id,category_id,score,amount,active
               FROM {schema}.events
               ORDER BY score DESC NULLS LAST LIMIT 50""",
            ("scan", "top_n", "project", "sink"),
            _topn_actions(mutations),
            rows,
            "TopN inside/outside boundary and mixed-type projection",
        ),
        Scenario(
            "topn_offset",
            "topn",
            "OFFSET 20 LIMIT 50; ASC NULLS FIRST; deterministic boundary",
            _event_setup(rows, groups).replace(
                "(value * 37) % 10000",
                "CASE WHEN value = 1 THEN NULL ELSE value END",
            ),
            """SELECT row_id,category_id,score,amount
               FROM {schema}.events
               ORDER BY score ASC NULLS FIRST OFFSET 20 LIMIT 50""",
            ("scan", "top_n", "project", "sink"),
            _topn_actions(mutations),
            rows,
            "TopN offset and opposite NULL ordering",
        ),
        Scenario(
            "window_default_all_functions",
            "window",
            "default frame; ASC; unique ordering",
            _event_setup(rows, groups).replace(
                "(value * 37) % 10000", "value"
            ),
            """SELECT row_id,category_id,score,
                      row_number() OVER w AS row_number_value,
                      rank() OVER w AS rank_value,
                      dense_rank() OVER w AS dense_rank_value,
                      count(*) OVER w AS running_count,
                      sum(amount) OVER w AS running_sum,
                      avg(amount) OVER w AS running_avg,
                      min(amount) OVER w AS running_min,
                      max(amount) OVER w AS running_max
               FROM {schema}.events
               WINDOW w AS (PARTITION BY category_id ORDER BY score)""",
            ("scan", "window", "project", "sink"),
            window_unique_actions,
            rows,
            "all supported window functions; peer behavior is isolated separately",
        ),
        Scenario(
            "window_rows_frame",
            "window",
            "ROWS; DESC; unique ordering with one initial NULL",
            _event_setup(rows, groups).replace(
                "(value * 37) % 10000",
                "CASE WHEN value = 1 THEN NULL ELSE value END",
            ),
            """SELECT row_id,category_id,score,
                      sum(amount) OVER (
                        PARTITION BY category_id ORDER BY score DESC NULLS FIRST
                        ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING
                      ) AS framed_sum
               FROM {schema}.events""",
            ("scan", "window", "project", "sink"),
            window_unique_actions,
            rows,
            "bounded ROWS frame",
        ),
        Scenario(
            "window_range_frame",
            "window",
            "RANGE; peer-heavy",
            _event_setup(rows, groups),
            """SELECT row_id,category_id,score,
                      sum(amount) OVER (
                        PARTITION BY category_id ORDER BY score
                        RANGE BETWEEN 100 PRECEDING AND 100 FOLLOWING
                      ) AS framed_sum
               FROM {schema}.events""",
            ("scan", "window", "project", "sink"),
            event_actions,
            rows,
            "bounded RANGE frame",
        ),
        Scenario(
            "window_groups_frame",
            "window",
            "GROUPS; peer-heavy",
            _event_setup(rows, groups),
            """SELECT row_id,category_id,score,
                      sum(amount) OVER (
                        PARTITION BY category_id ORDER BY amount
                        GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING
                      ) AS framed_sum
               FROM {schema}.events""",
            ("scan", "window", "project", "sink"),
            event_actions,
            rows,
            "bounded GROUPS frame",
        ),
        Scenario(
            "window_skewed_partition",
            "window",
            "80% in one partition",
            _event_setup(rows, groups, hotspot=True).replace(
                "(value * 37) % 10000", "value"
            ),
            """SELECT row_id,category_id,score,
                      row_number() OVER w AS row_number_value,
                      sum(amount) OVER w AS running_sum
               FROM {schema}.events
               WINDOW w AS (PARTITION BY category_id ORDER BY score)""",
            ("scan", "window", "project", "sink"),
            window_unique_actions,
            rows,
            "large skewed partition rebuild cost",
        ),
        Scenario(
            "window_peer_groups",
            "window",
            "50 order-key values; large peer groups",
            _event_setup(rows, groups).replace(
                "(value * 37) % 10000", "value % 50"
            ),
            """SELECT row_id,category_id,score,
                      rank() OVER w AS rank_value,
                      dense_rank() OVER w AS dense_rank_value,
                      count(*) OVER w AS running_count,
                      sum(amount) OVER w AS running_sum
               FROM {schema}.events
               WINDOW w AS (PARTITION BY category_id ORDER BY score)""",
            ("scan", "window", "project", "sink"),
            event_actions,
            rows,
            "deterministic peer semantics without row_number tie ambiguity",
        ),
        Scenario(
            "bigint_filter_aggregate",
            "typed_filter",
            "bigint boundaries and boolean predicate",
            f"""
CREATE SCHEMA {{schema}};
CREATE TABLE {{schema}}.wide_events (
  row_id integer NOT NULL,
  group_id bigint NOT NULL,
  amount bigint NOT NULL,
  enabled boolean NOT NULL
);
INSERT INTO {{schema}}.wide_events
SELECT value,
       9007199254740000::bigint + value % {groups},
       4000000000000000000::bigint / {max(rows, 1)} + value,
       value % 3 <> 0
FROM generate_series(1,{rows}) value;
ANALYZE {{schema}}.wide_events;
""",
            """SELECT group_id AS projected_group,count(*) AS row_count,
                      sum(amount) AS total_amount
               FROM {schema}.wide_events
               WHERE enabled = true AND amount <> 0
               GROUP BY group_id""",
            ("scan", "filter", "aggregate", "project", "sink"),
            (
                Action(
                    "insert",
                    f"""
INSERT INTO {{schema}}.wide_events
SELECT 16000000 + value,9007199254745000::bigint + value,
       1000000000000::bigint + value,value % 2 = 0
FROM generate_series(1,{mutations}) value
""",
                    mutations,
                    "bigint and boolean insertion",
                ),
                Action(
                    "update",
                    f"""
UPDATE {{schema}}.wide_events
SET group_id=group_id+10000,amount=amount-1,enabled=NOT enabled
WHERE row_id BETWEEN 1 AND {mutations}
""",
                    mutations,
                    "bigint group migration and predicate crossing",
                ),
                Action(
                    "delete",
                    f"DELETE FROM {{schema}}.wide_events WHERE row_id BETWEEN {mutations + 1} AND {mutations * 2}",
                    mutations,
                    "bigint row retraction",
                ),
            ),
            rows,
            "fixed-width typed encoding and SUM wider than i64",
        ),
    ]

    join_profiles = [
        ("inner_join_1to1", "JOIN", "inner_join", 1),
        ("inner_join_fanout", "JOIN", "inner_join", 4),
        ("left_join", "LEFT JOIN", "left_join", 2),
        ("right_join", "RIGHT JOIN", "right_join", 2),
        ("full_join", "FULL JOIN", "full_join", 2),
    ]
    join_keys = max(groups // 2, 10)
    for name, join_sql, operator, fanout in join_profiles:
        query = f"""SELECT d.group_id AS group_key,count(*) AS row_count,
                           sum(f.amount) AS total_amount
                    FROM {{schema}}.facts f {join_sql} {{schema}}.dims d
                      ON f.join_key=d.join_key
                    GROUP BY d.group_id"""
        scenarios.append(
            Scenario(
                name,
                "join",
                f"{join_sql}; dimension fanout {fanout}",
                _join_setup(rows, join_keys, fanout),
                query,
                ("scan", operator, "aggregate", "project", "sink"),
                _join_actions(mutations, join_keys, fanout),
                rows + join_keys * fanout,
                "both input sides; matched/unmatched/NULL and 0↔1 boundary",
            )
        )

    scenarios.append(
        Scenario(
            "join_cross_filter_having_distinct",
            "composed_join",
            "fanout 2; cross-input predicate",
            _join_setup(rows, join_keys, 2),
            """SELECT d.group_id AS group_key,
                      count(DISTINCT f.row_id) AS row_count,
                      sum(f.amount) AS total_amount
               FROM {schema}.facts f JOIN {schema}.dims d
                 ON f.join_key=d.join_key
               WHERE f.amount >= d.threshold AND f.gate <> d.gate
               GROUP BY d.group_id
               HAVING count(*) >= 2""",
            (
                "scan",
                "inner_join",
                "filter",
                "distinct",
                "aggregate",
                "having",
                "project",
                "sink",
            ),
            _join_actions(mutations, join_keys, 2),
            rows + join_keys * 2,
            "representative composed chain",
        )
    )

    sublink_profiles = [
        (
            "semi_exists",
            """WHERE EXISTS (
                 SELECT 1 FROM {schema}.permits p
                 WHERE p.join_key=o.join_key
               )""",
            "semi_join",
        ),
        (
            "semi_in",
            "WHERE o.join_key IN (SELECT p.join_key FROM {schema}.permits p)",
            "semi_join",
        ),
        (
            "anti_not_exists",
            """WHERE NOT EXISTS (
                 SELECT 1 FROM {schema}.permits p
                 WHERE p.join_key=o.join_key
               )""",
            "anti_join",
        ),
        (
            "null_aware_not_in",
            "WHERE o.join_key NOT IN (SELECT p.join_key FROM {schema}.permits p)",
            "null_aware_anti_join",
        ),
    ]
    for name, predicate, operator in sublink_profiles:
        scenarios.append(
            Scenario(
                name,
                "sublink_join",
                "duplicate permits; unmatched and NULL keys",
                _sublink_setup(rows, join_keys),
                f"""SELECT o.join_key AS group_key,count(*) AS row_count,
                            sum(o.amount) AS total_amount
                     FROM {{schema}}.orders o
                     {predicate}
                     GROUP BY o.join_key""",
                ("scan", operator, "aggregate", "project", "sink"),
                _sublink_actions(mutations, join_keys),
                rows + join_keys + 1,
                "both sides; 0↔1 match and NULL global boundary",
            )
        )

    return scenarios


ALL_OPERATOR_KINDS = {
    "scan",
    "filter",
    "project",
    "inner_join",
    "left_join",
    "right_join",
    "full_join",
    "semi_join",
    "anti_join",
    "null_aware_anti_join",
    "distinct",
    "aggregate",
    "having",
    "window",
    "top_n",
    "sink",
}
