# schema-migration-orchestrator

A CLI for running large Postgres schema migrations — backfills, type changes, new constrained
columns — against **live** tables without taking long locks or requiring a maintenance window.

## The problem

`ALTER TABLE users ADD COLUMN status text NOT NULL DEFAULT 'active'` on a five-million-row table
looks harmless. In older Postgres it rewrites the whole table under an `AccessExclusiveLock` —
every read and write on `users` queues up behind it for the duration. A single backfill
`UPDATE users SET status = 'active' WHERE status IS NULL` has the same problem: one statement,
one lock, held for as long as the whole scan takes. This is one of the most common
self-inflicted production outages in Postgres-backed systems, and it's avoidable — the fix is to
never let one statement touch the whole table.

`smorch` runs the migration as a sequence of small, independently-committed steps instead:

1. **Add the column nullable, with no default.** In Postgres 11+ this is a metadata-only change —
   near-instant regardless of table size.
2. **Backfill in small batches**, each batch its own transaction, paced by an external loop
   instead of one giant `UPDATE`. Progress is durable after every batch, so a killed run resumes
   from where it left off instead of restarting or corrupting state.
3. **Add constraints (`NOT NULL`, indexes, defaults) only after the backfill is verified
   complete**, using lock-cheap patterns (`CHECK ... NOT VALID` + `VALIDATE CONSTRAINT` before
   `SET NOT NULL`) so even the final step doesn't scan the table under a blocking lock.

Throughout the backfill, the orchestrator checks a pluggable lag/back-pressure signal before every
batch and pauses automatically if it's elevated, instead of batching at a fixed rate regardless of
downstream load.

## Architecture and design decisions

```
spec (JSON) ──► orchestrator ──► progress table (Postgres)
                    │                  ▲
                    ▼                  │ every batch commits
              step executors           │ its own progress row
        add_column / backfill / add_constraint
                    │
                    ▼
              throttle controller ──► lag source (pluggable)
```

**Batched backfill via keyset pagination, not `OFFSET`.** Each batch is one SQL statement:

```sql
UPDATE "users" SET status = 'active'
WHERE "id" = ANY (
  SELECT "id" FROM "users"
  WHERE ("id" > $1 OR $1 IS NULL) AND (status IS NULL)
  ORDER BY "id" LIMIT $2
)
RETURNING "id"
```

Keyset (`id > last_seen_id`) was chosen over `OFFSET`/`LIMIT` paging because `OFFSET` degrades as
the table grows — every page re-scans and discards all preceding rows — and because a keyset
cursor is exactly the resume checkpoint we need to persist anyway. The tradeoff: the pk column
must be sortable and effectively append-ordered (a `bigserial`/`bigint` PK, in practice), which is
true of the overwhelming majority of real tables and is documented as a requirement of the
`backfill` step.

**Progress table is the entire resumability mechanism.** Each batch's data `UPDATE` and its
progress-row `UPDATE` (advancing the cursor and row count) run in the *same* Postgres transaction,
committed together. A process killed mid-batch loses that one in-flight transaction entirely
(rolled back, nothing partially applied) and resumes from the last committed cursor — never a
partial batch, never progress recorded for data that wasn't actually written. This is also what
makes `run` and `resume` the same code path: `run` always reads current progress first, so
re-running a partially-completed (or fully-completed) migration just continues (or no-ops) it.
There is no separate "resume" implementation to drift out of sync with "run".

**Nullable-then-constrain split, not a single `ADD COLUMN ... NOT NULL DEFAULT`.** Splitting into
three steps bounds every individual lock to metadata-change duration instead of one lock scaling
with table size. The rejected alternative — one `ALTER TABLE` — is simpler code but is exactly the
outage pattern this project exists to avoid. The constrain step itself uses
`CHECK (col IS NOT NULL) NOT VALID` → `VALIDATE CONSTRAINT` (takes a `ShareUpdateExclusiveLock`,
does not block writes) → `SET NOT NULL` (Postgres 12+ recognizes the validated check constraint
and skips its own table scan, so the final `AccessExclusiveLock` is metadata-only) → drop the
now-redundant check constraint. A plain `ALTER COLUMN SET NOT NULL` with no prior validated
constraint would instead scan the whole table under `AccessExclusiveLock`.

**Lag-based auto-throttle via a pluggable `LagSource`, not a fixed batch rate.** A fixed
"sleep 50ms between batches" either wastes time when the system is healthy or isn't enough backoff
when it isn't. The orchestrator instead asks a `LagSource.getLagMs()` before every batch and
blocks (polling at a configurable interval, with an optional give-up timeout) until lag drops back
under threshold. Two implementations ship:
- `PgReplicationLagSource` — production implementation, reads real `replay_lag` from
  `pg_stat_replication`.
- `SimulatedLagSource` — test/no-replica implementation; the value is set directly (`setLagMs`),
  used both by the test suite and available via `--simulated-lag-ms` on the CLI for operators
  running against a primary with no attached replica.

Both implement the same one-method interface, so the throttle logic itself never knows which one
it's talking to.

**Stack: TypeScript/Node, not the original spec's Python.** This project's spec originally called
for Python; per the repo owner's explicit instruction it was built in plain Node.js/TypeScript
instead, using `commander` for the CLI and `pg` for the Postgres client — no web framework, since
this is an operational CLI tool, not a service. The design (batched keyset backfill, transactional
progress tracking, pluggable lag source, nullable-then-constrain split) is language-agnostic; only
the implementation language changed.

### What was actually verified vs. documented

This project *was* tested against a real local Postgres cluster (Postgres 14, via `initdb`/
`pg_ctl`, see [Testing](#testing) below) — not mocked. Concretely, verified for real:

- A full 3-step migration run against a real table, asserting final schema (`is_nullable`) and
  data correctness.
- **Kill-and-resume**: the real CLI is launched as a child process via `spawn`, `SIGKILL`'d
  mid-backfill, and a second `smorch resume` invocation is asserted to produce the correct final
  row count and schema with no duplicated or lost rows.
- **Lock behavior**: while a migration runs, a second connection polls `pg_locks` for the target
  relation every 25ms and asserts no `AccessExclusiveLock` is ever observed; concurrently, a third
  connection continuously writes to the table with `lock_timeout = 500ms` set, asserting zero
  writer failures throughout the run. This is the closest practical proxy, in an automated test,
  to the spec's "measured zero long-held table locks" — it's a real Postgres instance and real
  concurrent traffic, not a simulation of one.
- **Lag-based throttling**: the orchestrator is run against a real table with a
  `SimulatedLagSource` starting above threshold; the test asserts the run actually pauses
  (`onPause` fires) and only proceeds once the injected lag is dropped.

What is **not** claimed: this was not run against a genuine multi-million-row table (the local
test tables are in the thousands-to-tens-of-thousands range, sized for a fast test suite, not a
storage benchmark) and there is no real streaming replica involved (`PgReplicationLagSource`'s
`pg_stat_replication` query is written and type-checked but its query shape is documented, not
exercised, since standing up a second replica node was out of scope here — `SimulatedLagSource` is
what's actually exercised in tests, exactly as it's meant to be used as the injectable stand-in).

## Setup

Requires Node.js 18+ and, for the full test suite, local Postgres client/server binaries
(`initdb`, `pg_ctl`, `psql`) — on macOS, `brew install postgresql@14` or similar.

```bash
git clone <this-repo>
cd schema-migration-orchestrator
npm install
npm run build
```

### Running the test suite

```bash
npm run pg:start   # starts a throwaway local Postgres cluster scoped to this repo
                    #   (.pgdata-test/, unix-socket only — never binds a TCP port, so it can't
                    #   collide with any other Postgres instance on the machine)
npm test            # builds, then runs the full suite (unit + integration) against it
npm run pg:stop      # stop it when done; `npm run pg:stop` again is a no-op if already stopped
```

If the test cluster isn't running, integration tests that need it are skipped automatically
(`describe.runIf(await isPgAvailable())`) — `npm run test:unit` runs only the pure-unit throttle
tests with no Postgres dependency at all.

## Usage

Migrations are described as a JSON spec — see [`examples/backfill-user-status.json`](./examples/backfill-user-status.json):

```json
{
  "id": "backfill-user-status-v1",
  "table": "users",
  "steps": [
    { "type": "add_column", "name": "add-status-column", "column": "status", "dataType": "text" },
    {
      "type": "backfill",
      "name": "backfill-status",
      "pkColumn": "id",
      "batchSize": 500,
      "setClause": "status = 'active'",
      "whereClause": "status IS NULL",
      "batchDelayMs": 0
    },
    {
      "type": "add_constraint",
      "name": "enforce-status-not-null",
      "sql": "ALTER TABLE users ADD CONSTRAINT status_not_null CHECK (status IS NOT NULL) NOT VALID; ALTER TABLE users VALIDATE CONSTRAINT status_not_null; ALTER TABLE users ALTER COLUMN status SET NOT NULL; ALTER TABLE users DROP CONSTRAINT status_not_null;"
    }
  ]
}
```

Run it (real output, against a local test table of 5,000 rows):

```
$ smorch run examples/backfill-user-status.json --db-url "$DATABASE_URL"
Migration "backfill-user-status-v1" on table "users" (3 step(s))
[0] add-status-column (add_column) — starting (status was "pending")
  ALTER TABLE "users" ADD COLUMN IF NOT EXISTS "status" text NULL
[0] add-status-column — completed
[1] backfill-status (backfill) — starting (status was "pending")
  batch complete: 500 rows (cursor=500, total this run=500)
  batch complete: 500 rows (cursor=1000, total this run=1000)
  batch complete: 500 rows (cursor=1500, total this run=1500)
  batch complete: 500 rows (cursor=2000, total this run=2000)
  batch complete: 500 rows (cursor=2500, total this run=2500)
  batch complete: 500 rows (cursor=3000, total this run=3000)
  batch complete: 500 rows (cursor=3500, total this run=3500)
  batch complete: 500 rows (cursor=4000, total this run=4000)
  batch complete: 500 rows (cursor=4500, total this run=4500)
  batch complete: 500 rows (cursor=5000, total this run=5000)
[1] backfill-status — completed
[2] enforce-status-not-null (add_constraint) — starting (status was "pending")
  ALTER TABLE users ADD CONSTRAINT status_not_null CHECK (status IS NOT NULL) NOT VALID; ALTER TABLE users VALIDATE CONSTRAINT status_not_null; ALTER TABLE users ALTER COLUMN status SET NOT NULL; ALTER TABLE users DROP CONSTRAINT status_not_null;
[2] enforce-status-not-null — completed
Migration "backfill-user-status-v1" complete.
```

Running it again is a no-op (every step already `completed`):

```
$ smorch run examples/backfill-user-status.json --db-url "$DATABASE_URL"
Migration "backfill-user-status-v1" on table "users" (3 step(s))
[0] add-status-column (add_column) — already completed, skipping
[1] backfill-status (backfill) — already completed, skipping
[2] enforce-status-not-null (add_constraint) — already completed, skipping
Migration "backfill-user-status-v1" complete.
```

Check progress at any time (this reads the same `schema_migration_progress` table the orchestrator
writes to — no separate state to go stale):

```
$ smorch status examples/backfill-user-status.json --db-url "$DATABASE_URL"
┌─────────┬──────┬───────────────────────────┬─────────────┬────────────────┬────────┬────────────────────────────┐
│ (index) │ step │ name                      │ status      │ rows_processed │ cursor │ updated_at                 │
├─────────┼──────┼───────────────────────────┼─────────────┼────────────────┼────────┼────────────────────────────┤
│ 0       │ 0    │ 'add-status-column'       │ 'completed' │ '0'            │ null   │ '2026-08-13T01:25:32.254Z' │
│ 1       │ 1    │ 'backfill-status'         │ 'completed' │ '5000'         │ '5000' │ '2026-08-13T01:25:32.292Z' │
│ 2       │ 2    │ 'enforce-status-not-null' │ 'completed' │ '0'            │ null   │ '2026-08-13T01:25:32.292Z' │
└─────────┴──────┴───────────────────────────┴─────────────┴────────────────┴────────┴────────────────────────────┘
```

If the process is killed mid-run, `smorch resume <spec> --db-url ...` (identical to `run`) picks
up from the last committed batch.

### CLI flags

| Flag | Meaning |
|---|---|
| `--db-url` | Postgres connection string (or `DATABASE_URL` env var) |
| `--lag-threshold-ms` | pause batching once lag exceeds this (default `2000`) |
| `--lag-poll-interval-ms` | how often to re-check lag while paused (default `1000`) |
| `--lag-max-wait-ms` | give up and exit non-zero if lag stays high this long (default: wait forever) |
| `--simulated-lag-ms` | use a fixed value instead of querying `pg_stat_replication` (no-replica setups, demos, local testing) |

## Testing

Test suite (`npm test`, real output):

```
 ✓ test/orchestrator.test.ts (4 tests) 11196ms
   ✓ orchestrator against real Postgres > resumes correctly after the process is killed mid-backfill 11070ms
 ✓ test/lockVerification.test.ts (1 test) 901ms
   ✓ lock behavior during a live migration > never takes an AccessExclusiveLock and never blocks a concurrent writer beyond its lock_timeout 868ms
 ✓ test/throttle.test.ts (4 tests) 35ms

 Test Files  3 passed (3)
      Tests  9 passed (9)
```

- `test/throttle.test.ts` — pure unit tests of the throttle/back-pressure logic against
  `SimulatedLagSource`, no database required.
- `test/orchestrator.test.ts` — against real local Postgres: full-run correctness, idempotent
  re-run, lag-triggered pause-and-resume-within-a-run, and the **kill-and-resume** test (spawns the
  actual built CLI as a child process, `SIGKILL`s it mid-backfill, asserts partial progress, then
  resumes and asserts the final row count/schema is correct with nothing duplicated or lost).
- `test/lockVerification.test.ts` — against real local Postgres: samples `pg_locks` every 25ms for
  the duration of a migration run while a concurrent writer continuously inserts rows with a
  500ms `lock_timeout`; asserts no `AccessExclusiveLock` is ever observed and the writer never
  errors.

## Known limitations

- `backfill.pkColumn` must be a monotonically sortable, effectively append-ordered key (a
  `bigint`/`bigserial` primary key in practice) — the keyset pagination strategy depends on it.
- Migration specs are trusted operator input, akin to a migration file reviewed in a PR — table
  and column names are validated against a strict identifier whitelist before interpolation (raw
  SQL can't be parameter-bound for identifiers in Postgres), but `setClause`/`whereClause`/
  `add_constraint.sql` are executed as written and are not sandboxed.
- `PgReplicationLagSource`'s query against `pg_stat_replication` is implemented and type-checked
  but not exercised against a real streaming replica in this repo's test suite (see
  "What was actually verified vs. documented" above).
