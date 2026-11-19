# causalmesh-engine

Replica and distributed-state components.

Python 3.11 or newer. The primary interfaces are importable component libraries and a local JSON HTTP service.

## Install

```sh
python3 -m venv .venv
./.venv/bin/python -m pip install -r requirements-dev.txt
./.venv/bin/python -m pip install --no-deps -e .
```

## Run

```sh
./.venv/bin/python -m causalmesh demo
./.venv/bin/python -m causalmesh serve --port 8080
./.venv/bin/python -m pytest -q
```

The server binds only to `127.0.0.1`. `GET /health` reports process readiness; `GET /capabilities` lists operations.
Each component has `POST /v1/<operation>` accepting its JSON request object. Responses contain `result` or an explicit error. The service accepts bodies up to 1 MB and at most four concurrent operation handlers. It does not expose a shell, arbitrary file paths or remote URLs.

## Components

| Operation | Behavior |
|---|---|
| `replicas` | Observed-remove string sets, causal dots, merge and JSON state restoration. |
| `transactions` | Conflict-serializability analysis with a concrete cycle or legal order for complete single-version histories. |
| `snapshots` | FIFO marker snapshots with independently recorded in-flight channel messages and overlapping capture sessions. |
| `placement` | Exhaustive bounded replica placement against explicit failure sets, with survivor certificates. |
| `aggregation` | Client-side pairwise X25519-derived additive masks and fixed-roster aggregate verification. |
| `sync` | Incremental dimension-table sync: generational add/remove batches, durable watermark-coupled commits, observed-remove concurrency and independent ledger replay. |

## Request examples

Run `python -m causalmesh.examples` to print valid JSON requests. The output is generated locally and contains no credentials.

```sh
./.venv/bin/python -m causalmesh.examples > requests.json
curl -s http://127.0.0.1:8080/capabilities
```

Use one operation request from that output as the JSON body for its `/v1/<operation>` endpoint. The `demo` command executes all five operations and prints their computed results.

## Supported scope

Replica identities must be distinct and are not recycled; removed-dot metadata is retained. Transaction results are conflict serializability, not a claim about every possible isolation model. Snapshot channels are reliable FIFO and run in a deterministic local message runtime. Aggregation requires all participants and at least three clients; a missing participant aborts. It assumes an honest fixed roster and semi-honest participants, lacks deployment authentication, and is not a dropout-tolerant or formally certified protocol. Client private material stays in memory and is not returned by the service.

Runtime state uses explicit temporary directories or caller-provided library objects. Service examples are per-request computations; use the library interfaces for persistent state and long-lived readers.

## Incremental dimension-table sync (`sync`)

`causalmesh.sync.SyncEngine` builds the recovery entry point on top of the causal
OR-set. It answers "which members are still in the dimension table now" rather
than re-importing members that were previously deleted.

* **Batch input.** Each batch carries a source event identity `(source, seq)`,
  member generations `(name, generation)`, and `adds` / `removes` lists.
  Removes may name the observed add events explicitly; without explicit
  evidence they observe only the add-dots currently visible for that exact
  generational member.
* **Coupled commit and progress.** The dimension-table change, the per-source
  contiguous watermark and the acknowledgement record advance in one durable
  revision. Re-delivered batches return the original result
  (`replayed: true`); a redelivery with different content for the same event
  identity is rejected as `DuplicateBatchError`. Gap batches are held until
  the missing predecessor arrives.
* **Observed-remove concurrency.** A remove never tombstones an add it did not
  observe, so delivery order cannot turn the semantics into last-writer-wins.
* **Generational fencing.** A reused name with an older generation is a
  different, stale writer; `StaleGenerationError` explains the rejected
  generation and the generation already committed.
* **Retained delete evidence.** Unresolved remove evidence (the observed add
  is still partitioned away) is retained across restarts and compaction, and
  tombstones the add as soon as it lands. Tombstones are never garbage
  collected to save space.
* **Durable recovery and independent change model.** Every commit is an
  immutable hash-chained ledger record. `replay()` rebuilds members,
  watermarks, clocks, fences, tombstones and pending evidence from the ledger
  alone; `audit()` cross-checks it against the materialized state. A monotonic
  epoch file outside the state image blocks re-entry of old disk state, and
  `reconcile_dir()` cleans interrupted commit-point artifacts.

The HTTP `sync` operation is stateless: `action=start` returns a state image,
and `apply` / `query` / `audit` / `replay` / `compact` carry that image inside
the request and return the next one. For durable, long-lived readers use
`SyncEngine(target, directory=...)` directly.

## Dependencies

Pinned Python dependencies are listed in `requirements.txt`; the development test runner is pinned in `requirements-dev.txt`. System libraries or a native compiler, when required above, are detected at runtime. No accounts, network connection strings, tokens or private-key files are required.
