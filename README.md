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

## Dependencies

Pinned Python dependencies are listed in `requirements.txt`; the development test runner is pinned in `requirements-dev.txt`. System libraries or a native compiler, when required above, are detected at runtime. No accounts, network connection strings, tokens or private-key files are required.
