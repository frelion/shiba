# M14.4 two-source JOIN authority contract

Status: M14.4 authority accepted; M14.5 pure Compiler/Operator kernel
implemented. Runtime, Catalog and PostgreSQL evidence remain M14.6.

M14.4 admits one narrow relational operation: a bigint equality INNER JOIN
between exactly two explicitly registered `SourceId` values in the same
PostgreSQL database. The sources may occupy different schemas. This contract
does not admit name discovery, a third input, an outer join, a non-equality
predicate, SQL text, or a second execution path.

M14.5 gives the pure plan a nonzero `GraphId`, canonically ordered
`SourcePort` members and explicit `SourcePort(SourceId)` node inputs. Compiler
binds the exact effective right replica-identity index. The database-free
kernel stores left membership and right values in generic partitioned state,
evaluates both batches from one pre-state to one final state, and emits
normalized keyed mutations. A fixed-seed 300-step relational differential
covers mixed two-side changes. Fan-out 20,000 succeeds and 20,001 fails before
returning a transition. Ordered affected-row indexes replace the initial
quadratic scan with `O(n log n)` behavior. These are pure-code proofs, not
PostgreSQL integration evidence.

The compiled vertical slice has one unambiguous shape:

```text
left  (id bigint primary key, right_key bigint null)
right (id bigint non-null primary/unique key, payload bigint null)
ON left.right_key = right.id
Project(left.id, right.payload) -> Materialize(left.id, right.payload)
```

A NULL left join key has no match. A matched NULL right payload remains typed
NULL in the keyed result; it is not absent. Compiler resolves all four column
names and the exact right identity index once to ObjectAddresses. The admitted
right PK/UK must also be that source's exact effective replica identity index;
an arbitrary second lookup index is rejected because UPDATE/DELETE old-key WAL
would not prove the same row identity. Runtime never uses those names or
chooses a different current PK/UK.

## One graph, transport and progress authority

One canonical graph definition owns both ordered source members, their exact
relation and column `ObjectAddress` values, the join ports, state/output
contracts and digest. The right lookup relation additionally binds the exact
non-null bigint primary-key or unique-key index `ObjectAddress`. Registration
does not save a relation, column or index name as durable identity.

A source may be a member of at most one building or active graph. Admission
therefore cannot attach one source to two independently progressing graphs.
Both members use one publication, one logical slot and one slot generation.
The pgoutput assembler produces one graph transaction for each committed
PostgreSQL transaction and includes every admitted change from either or both
sides. It never splits a two-side commit into source transactions.

Progress belongs only to `(graph_id, slot_generation)`. The graph continuation
records the exact terminal WAL identity applied by the graph transaction; slot
feedback is authorized only after that graph transaction durably returns
Applied or exact AlreadyApplied. There is no per-source or per-node
continuation, ACK cursor, receiver, Runtime or retry decision. `DeltaBatch`
remains bounded transaction-local memory and is never persisted.

## Admission and lifecycle

Before installing a building graph, the sole registration/lifecycle writer
must atomically reject:

- anything other than two distinct explicit SourceIds in one database;
- publication membership, slot or generation mismatch;
- a source already owned by another building or active graph;
- relation/column ObjectAddress drift or an invalidated member;
- a missing, nullable, non-bigint, partial, expression, invalid or unready right
  PK/UK index, an index that is not the source's effective replica identity, or
  an index whose exact OID differs from the compiled binding;
- a noncanonical graph, input layout, plan digest or state/output contract;
- insufficient control, scan, Apply, replication or result-reader privileges.

Bootstrap creates one logical-slot `EXPORT_SNAPSHOT` boundary, scans both
relations under that same exported snapshot in bounded batches, catches up the
one slot, and atomically activates the complete graph and all terminal results.
Partial join results remain building/unavailable. Rebuild is graph-wide: it
retires the old graph generation, uses one new exported snapshot and activates
one complete successor. A member cannot be rebuilt or rebound independently.

## Apply transaction and lock order

Runtime owns the sole PostgreSQL Apply transaction. For one graph transaction
the lock order is fixed:

1. graph and generation ownership;
2. replay/graph-continuation probe;
3. exact source bindings in ascending SourceId order;
4. current source rows in `(SourceId, canonical row key)` order;
5. node/state keys in canonical `(NodeId, namespace, typed state key)` order;
6. pure graph computation;
7. ordered state and result persistence;
8. graph continuation last;
9. commit, then feedback authorization.

The two source batches are constructed once. Join computation observes one
pretransaction state and produces one final transition; pgoutput message order
inside the transaction cannot expose intermediate results. Operator code does
not execute SQL. Runtime performs bounded set-based reads and writes. A
right-side UPDATE or DELETE may affect many left rows, but its fan-out is
charged before allocation and persisted in bounded sets, never by a source
table lookup, per-row SQL round trip, unbounded queue or hidden spool.

Any binding, decoder, state, fan-out, arithmetic, sink, constraint,
serialization or backend failure rolls back both source-row mutations, every
node state/result and the graph continuation. Retry begins at the complete WAL
transaction. Exact replay stops before Source Apply and graph computation.

## DDL, crash and privilege evidence required

The later implementation slices are not proved until PG17.10 and PG18.4
independently prove:

- admission success plus every rejection listed above;
- left-only, right-only and same-transaction two-side I/U/D SQL differential;
- right key/value update, delete fan-out and empty/nonmatching joins;
- fan-out/output bounds with no partial state, result, continuation or ACK;
- receive-before-Apply, Apply-before-feedback, backend kill, serialization
  retry and exact replay crash windows;
- relation/column rename by stable OID, drop/recreate rejection, publication
  drift and exact right-index replacement invalidation;
- one exported snapshot for both sources, concurrent-WAL catch-up, graph-wide
  activation and graph-wide rebuild/recovery;
- one-winner registration/rebuild, same-graph serialization and independent
  progress for disjoint graphs;
- separate non-superuser control/Apply/scanner, trusted replication credential
  and read-only result reader, with missing/swapped privileges failing closed;
- frozen fan-out latency, set-based query count, memory/RSS, retained WAL and
  replay thresholds on both PostgreSQL versions.

The accepted contract adds no compatibility adapter, fallback, dual write,
second continuation, persisted EffectStream/DeltaBatch or second Runtime.
Until the tests above are green, the PostgreSQL two-source JOIN path,
graph-scoped lifecycle, bootstrap, rebuild and performance remain explicitly
unproved; the M14.5 pure compiler/kernel evidence does not close them.
