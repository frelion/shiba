# ADR 0003: one-authority offline rebuild

Status: accepted for M12.1 contract; implementation pending M12.2--M12.6.

## Context

M11 initializes only a pristine source. An active source already owns current
rows, operator state/result, a continuation, active bootstrap lifecycle and a
logical-slot generation. Rebinding it cannot relabel old compute history as a
new source, and PostgreSQL slot operations cannot commit atomically with Shiba
catalog changes.

The existing M11 scanner and Runtime resolve the one current
`source_binding`/`source_ingress_config`. Adding a candidate binding would
create a second authority and a second execution path. PostgreSQL 17.10 and
18.4 also expose no immutable slot OID, creation ID or per-slot ACL: a
privileged actor can replace a slot with the same name and observable shape.

## Decision

Use an offline, forward-only rebuild. Before destructive prepare, the old
generation remains entirely active while all target checks are read-only.
Prepare uses exact-old CAS and atomically makes the target identity/generation
the sole building catalog authority, hides public results as `building/NULL`,
retires old compute state and disables the old generation. After that commit,
only forward recovery is legal.

The target slot supplies a real `EXPORT_SNAPSHOT`; M11 bounded scan/catch-up
and M10 decoding, Runtime, feedback and live handoff are reused. Activation
only promotes that same target authority and publishes its complete results. It
does not perform another binding/config switch.

Every observable physical-slot mismatch fails closed. The `REPLICATION`
credential is a trusted control-plane capability, not a per-slot database ACL.
Credential exclusivity and no external slot DDL are deployment assumptions.
An otherwise identical replacement by a superuser or holder of that credential
is explicitly outside the M12 correctness threat model.

## Rejected alternatives

- Parallel old/new generation computation: requires dual state, dual write and
  a second authority during catch-up.
- Candidate binding/config: the existing scanner and Runtime would require a
  candidate-specific path and ambiguous writers.
- Reusing or rebadging the old continuation: it has no valid relationship to
  the new slot's exported-snapshot boundary.
- Slot discovery/adoption by name: names do not prove incarnation or ownership.
- A new slot-birth marker: adds protocol state and crash windows without current
  evidence that M12 data correctness requires it.
- Falling back to the old generation after prepare: could publish stale state
  and makes the destructive boundary non-deterministic.

## Consequences

Rebuild has an explicit availability interval in which results are
`building/NULL`. The design retains one binding/config/bootstrap authority and
one continuation, and recovery has a unique forward direction. Slot/catalog
crash windows require explicit phase reconciliation. The system detects every
slot conflict PostgreSQL exposes, but cannot prove an identical privileged
replacement did not occur; hardening that threat requires a future incarnation
protocol and is outside M12.
