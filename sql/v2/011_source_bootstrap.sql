-- M11.1 owns one bootstrap lifecycle/checkpoint row per source. It stores no
-- WAL payload, moving transport cursor, EffectBatch, or second continuation.

ALTER TABLE shiba_internal.source_ingress_config
    ADD CONSTRAINT source_ingress_config_slot_generation_unique UNIQUE (
        source_id, slot_name, slot_generation
    );

CREATE TABLE shiba_internal.source_bootstrap (
    source_id bigint PRIMARY KEY CHECK (source_id > 0),
    bootstrap_id bigint NOT NULL UNIQUE CHECK (bootstrap_id > 0),
    slot_name name NOT NULL,
    slot_generation bigint NOT NULL CHECK (slot_generation > 0),
    consistent_point pg_lsn,
    phase text NOT NULL CHECK (phase IN (
        'creating', 'scanning', 'scan_complete', 'catching_up',
        'active', 'cleanup_pending', 'failed'
    )),
    last_batch_ordinal bigint NOT NULL DEFAULT 0
        CHECK (last_batch_ordinal >= 0),
    last_source_row_id bigint,
    last_batch_digest bytea,
    fence_token uuid NOT NULL UNIQUE DEFAULT pg_catalog.gen_random_uuid(),
    catchup_fence_lsn pg_lsn,
    activation_end_lsn pg_lsn,
    CONSTRAINT source_bootstrap_exact_ingress FOREIGN KEY (
        source_id, slot_name, slot_generation
    ) REFERENCES shiba_internal.source_ingress_config (
        source_id, slot_name, slot_generation
    ),
    CONSTRAINT source_bootstrap_consistent_point CHECK (
        (phase = 'creating' AND consistent_point IS NULL)
        OR (phase IN ('scanning', 'scan_complete', 'catching_up', 'active')
            AND consistent_point IS NOT NULL
            AND consistent_point > '0/0'::pg_lsn)
        OR (phase IN ('cleanup_pending', 'failed') AND (
            consistent_point IS NULL
            OR consistent_point > '0/0'::pg_lsn
        ))
    ),
    CONSTRAINT source_bootstrap_batch_checkpoint CHECK (
        (last_batch_ordinal = 0
         AND last_source_row_id IS NULL AND last_batch_digest IS NULL)
        OR (last_batch_ordinal > 0
            AND last_source_row_id IS NOT NULL
            AND last_batch_digest IS NOT NULL
            AND pg_catalog.octet_length(last_batch_digest) = 32)
    ),
    CONSTRAINT source_bootstrap_catchup_fence CHECK (
        (phase IN ('catching_up', 'active')
         AND catchup_fence_lsn IS NOT NULL
         AND catchup_fence_lsn >= consistent_point)
        OR (phase IN ('creating', 'scanning', 'scan_complete')
            AND catchup_fence_lsn IS NULL)
        OR (phase IN ('cleanup_pending', 'failed') AND (
            catchup_fence_lsn IS NULL
            OR (consistent_point IS NOT NULL
                AND catchup_fence_lsn >= consistent_point)
        ))
    ),
    CONSTRAINT source_bootstrap_activation_authorization CHECK (
        (phase = 'active'
         AND activation_end_lsn IS NOT NULL
         AND activation_end_lsn >= catchup_fence_lsn)
        OR (phase <> 'active' AND activation_end_lsn IS NULL)
    )
);

REVOKE ALL ON TABLE shiba_internal.source_bootstrap FROM PUBLIC;

COMMENT ON TABLE shiba_internal.source_bootstrap IS
    'Single source bootstrap lifecycle, bounded scan checkpoint, and worker fence authority';
