-- One graph bootstrap/rebuild lifecycle owns one exported snapshot and one
-- checkpoint per ordered member. It stores no WAL payload or second cursor.

CREATE TABLE shiba_internal.graph_bootstrap (
    graph_id bigint PRIMARY KEY CHECK (graph_id > 0),
    graph_digest bytea NOT NULL CHECK (pg_catalog.octet_length(graph_digest) = 32),
    bootstrap_id bigint NOT NULL UNIQUE CHECK (bootstrap_id > 0),
    slot_name name NOT NULL,
    slot_generation bigint NOT NULL CHECK (slot_generation > 0),
    consistent_point pg_lsn,
    phase text NOT NULL CHECK (phase IN (
        'creating', 'rebuild_prepared', 'scanning', 'scan_complete',
        'catching_up', 'active', 'cleanup_pending', 'failed'
    )),
    fence_token uuid NOT NULL UNIQUE DEFAULT pg_catalog.gen_random_uuid(),
    catchup_fence_lsn pg_lsn,
    activation_end_lsn pg_lsn,
    retired_bootstrap_id bigint,
    retired_slot_name name,
    retired_slot_generation bigint,
    CONSTRAINT graph_bootstrap_exact_definition FOREIGN KEY (graph_id, graph_digest)
        REFERENCES shiba_internal.graph_definition (graph_id, graph_digest)
        DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT graph_bootstrap_exact_ingress FOREIGN KEY (
        graph_id, slot_name, slot_generation
    ) REFERENCES shiba_internal.graph_ingress_config (
        graph_id, slot_name, slot_generation
    ) DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT graph_bootstrap_consistent_point CHECK (
        (phase IN ('creating', 'rebuild_prepared') AND consistent_point IS NULL)
        OR (phase IN ('scanning', 'scan_complete', 'catching_up', 'active')
            AND consistent_point IS NOT NULL AND consistent_point > '0/0'::pg_lsn)
        OR (phase IN ('cleanup_pending', 'failed') AND (
            consistent_point IS NULL OR consistent_point > '0/0'::pg_lsn))
    ),
    CONSTRAINT graph_bootstrap_catchup_fence CHECK (
        (phase IN ('catching_up', 'active') AND catchup_fence_lsn IS NOT NULL
         AND catchup_fence_lsn >= consistent_point)
        OR (phase IN ('creating', 'rebuild_prepared', 'scanning', 'scan_complete')
            AND catchup_fence_lsn IS NULL)
        OR (phase IN ('cleanup_pending', 'failed') AND (
            catchup_fence_lsn IS NULL OR (consistent_point IS NOT NULL
                AND catchup_fence_lsn >= consistent_point)))
    ),
    CONSTRAINT graph_bootstrap_activation_authorization CHECK (
        (phase = 'active' AND activation_end_lsn IS NOT NULL
         AND activation_end_lsn >= catchup_fence_lsn)
        OR (phase <> 'active' AND activation_end_lsn IS NULL)
    ),
    CONSTRAINT graph_bootstrap_retired_identity CHECK (
        (retired_bootstrap_id IS NULL AND retired_slot_name IS NULL
         AND retired_slot_generation IS NULL AND phase <> 'rebuild_prepared')
        OR (retired_bootstrap_id > 0 AND bootstrap_id <> retired_bootstrap_id
         AND retired_slot_name IS NOT NULL AND slot_name <> retired_slot_name
         AND retired_slot_generation > 0
         AND slot_generation = retired_slot_generation + 1)
    )
);

CREATE TABLE shiba_internal.graph_bootstrap_checkpoint (
    graph_id bigint NOT NULL,
    source_id bigint NOT NULL,
    last_batch_ordinal bigint NOT NULL DEFAULT 0 CHECK (last_batch_ordinal >= 0),
    last_source_row_id bigint,
    last_batch_digest bytea,
    CONSTRAINT graph_bootstrap_checkpoint_primary PRIMARY KEY (graph_id, source_id),
    CONSTRAINT graph_bootstrap_checkpoint_lifecycle FOREIGN KEY (graph_id)
        REFERENCES shiba_internal.graph_bootstrap (graph_id),
    CONSTRAINT graph_bootstrap_checkpoint_member FOREIGN KEY (graph_id, source_id)
        REFERENCES shiba_internal.graph_source_member (graph_id, source_id),
    CONSTRAINT graph_bootstrap_checkpoint_value CHECK (
        (last_batch_ordinal = 0 AND last_source_row_id IS NULL
         AND last_batch_digest IS NULL)
        OR (last_batch_ordinal > 0 AND last_source_row_id IS NOT NULL
            AND last_batch_digest IS NOT NULL
            AND pg_catalog.octet_length(last_batch_digest) = 32)
    )
);

REVOKE ALL ON TABLE shiba_internal.graph_bootstrap FROM PUBLIC;
REVOKE ALL ON TABLE shiba_internal.graph_bootstrap_checkpoint FROM PUBLIC;

COMMENT ON TABLE shiba_internal.graph_bootstrap IS
    'Sole graph bootstrap/rebuild lifecycle and exported-snapshot authority';
COMMENT ON TABLE shiba_internal.graph_bootstrap_checkpoint IS
    'Bounded per-member scan progress subordinate to one graph lifecycle';
