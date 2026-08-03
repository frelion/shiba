-- M12 reuses the sole bootstrap row as the durable rebuild lifecycle.  The
-- target coordinates occupy the existing fields; only the retired transport
-- identity remains while destructive prepare is recoverable.

ALTER TABLE shiba_internal.source_ingress_config
    DROP CONSTRAINT source_ingress_bound_source,
    ADD CONSTRAINT source_ingress_bound_source FOREIGN KEY (
        source_id, source_binding_kind, source_binding_objsubid
    ) REFERENCES shiba_internal.source_binding (
        source_id, binding_kind, address_objsubid
    ) DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE shiba_internal.source_bootstrap
    DROP CONSTRAINT source_bootstrap_exact_ingress,
    DROP CONSTRAINT source_bootstrap_phase_check,
    DROP CONSTRAINT source_bootstrap_consistent_point,
    DROP CONSTRAINT source_bootstrap_catchup_fence,
    ADD COLUMN retired_bootstrap_id bigint,
    ADD COLUMN retired_slot_name name,
    ADD COLUMN retired_slot_generation bigint,
    ADD CONSTRAINT source_bootstrap_exact_ingress FOREIGN KEY (
        source_id, slot_name, slot_generation
    ) REFERENCES shiba_internal.source_ingress_config (
        source_id, slot_name, slot_generation
    ) DEFERRABLE INITIALLY IMMEDIATE,
    ADD CONSTRAINT source_bootstrap_phase_check CHECK (phase IN (
        'creating', 'rebuild_prepared', 'scanning', 'scan_complete',
        'catching_up', 'active', 'cleanup_pending', 'failed'
    )),
    ADD CONSTRAINT source_bootstrap_consistent_point CHECK (
        (phase IN ('creating', 'rebuild_prepared') AND consistent_point IS NULL)
        OR (phase IN ('scanning', 'scan_complete', 'catching_up', 'active')
            AND consistent_point IS NOT NULL
            AND consistent_point > '0/0'::pg_lsn)
        OR (phase IN ('cleanup_pending', 'failed') AND (
            consistent_point IS NULL OR consistent_point > '0/0'::pg_lsn
        ))
    ),
    ADD CONSTRAINT source_bootstrap_catchup_fence CHECK (
        (phase IN ('catching_up', 'active')
         AND catchup_fence_lsn IS NOT NULL
         AND catchup_fence_lsn >= consistent_point)
        OR (phase IN ('creating', 'rebuild_prepared', 'scanning', 'scan_complete')
            AND catchup_fence_lsn IS NULL)
        OR (phase IN ('cleanup_pending', 'failed') AND (
            catchup_fence_lsn IS NULL
            OR (consistent_point IS NOT NULL
                AND catchup_fence_lsn >= consistent_point)
        ))
    ),
    ADD CONSTRAINT source_bootstrap_retired_identity CHECK (
        (phase = 'rebuild_prepared'
         AND retired_bootstrap_id > 0
         AND bootstrap_id <> retired_bootstrap_id
         AND retired_slot_name IS NOT NULL
         AND slot_name <> retired_slot_name
         AND retired_slot_generation > 0
         AND slot_generation = retired_slot_generation + 1)
        OR (phase <> 'rebuild_prepared'
            AND retired_bootstrap_id IS NULL
            AND retired_slot_name IS NULL
            AND retired_slot_generation IS NULL)
    );

COMMENT ON COLUMN shiba_internal.source_bootstrap.retired_bootstrap_id IS
    'Exact retired active bootstrap identity retained only during rebuild prepare';
COMMENT ON COLUMN shiba_internal.source_bootstrap.retired_slot_name IS
    'Exact old slot to retire before the prepared rebuild can reserve its new slot';
COMMENT ON COLUMN shiba_internal.source_bootstrap.retired_slot_generation IS
    'Old generation rejected after the target generation becomes building authority';
