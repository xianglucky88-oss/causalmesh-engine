"""Incremental dimension-table synchronization on top of the causal OR-set.

The engine accepts add/remove batches that carry a *source event identity*
``(source, seq)`` and explicit *member generations* ``(name, generation)``.
It publishes queryable target dimension-table versions together with the
confirmed contiguous source progress (watermarks).

Guarantees implemented here:

* Dimension-table commits and source progress advance together in one durable
  step; retries (duplicate batches), lost acknowledgements and re-entry of an
  older on-disk state can never produce a second effective change.
* Adds and removes keep the observed-remove semantics of
  :class:`causalmesh.replicas.ORSet`: a remove only tombstones add-dots it
  explicitly observed, so a concurrent add is never overwritten by a remove
  that arrives later.
* Remove evidence that cannot yet be resolved (the observed add is still
  missing after a partition or compaction restart) is retained as pending
  evidence and applied when the add eventually lands. Tombstone metadata is
  never garbage collected.
* A member name reused with a new generation is a different writer/element.
  Operations carrying a generation older than the one already committed are
  rejected with an explanatory :class:`StaleGenerationError`.
* Every committed batch is recorded in an immutable, hash-chained change
  ledger. :func:`replay` rebuilds the target membership and the source
  progress from that ledger alone (the independent change model), and
  :meth:`SyncEngine.audit` cross-checks it against the materialized state.
"""

import hashlib
import json
import os
import threading
import uuid

from .replicas import ORSet

STATE_VERSION = 1
MAX_BATCH_OPS = 10_000
MAX_PENDING_PER_SOURCE = 10_000
MAX_NAME_LEN = 256
GENESIS_HASH = ""


class SyncError(ValueError):
    """Base class for synchronization errors."""


class StaleGenerationError(SyncError):
    """An operation references a member generation older than the known one."""


class DuplicateBatchError(SyncError):
    """A re-delivered batch whose stored acknowledgement is missing locally."""


class StaleStateError(SyncError):
    """An on-disk state image is older than, or diverges from, the live state."""


class AuditError(SyncError):
    """The independent change model disagrees with the materialized state."""


def _canonical(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def member_key(name, generation):
    """OR-set element encoding for a generational member."""
    return _canonical([name, generation])


def decode_member(key):
    name, generation = json.loads(key)
    return name, generation


def _event_id(value):
    if not (isinstance(value, (list, tuple)) and len(value) == 2):
        raise SyncError("event identity must be [source, seq]")
    source, seq = value
    if not isinstance(source, str) or not source:
        raise SyncError("event source identity required")
    if not isinstance(seq, int) or isinstance(seq, bool) or seq < 1:
        raise SyncError("event sequence must be a positive integer")
    return source, seq


def _member_op(value):
    if not (isinstance(value, (list, tuple)) and len(value) >= 2):
        raise SyncError("member operation must be [name, generation]")
    name, generation = value[0], value[1]
    if not isinstance(name, str) or not name or len(name) > MAX_NAME_LEN:
        raise SyncError("member name must be a non-empty string")
    if not isinstance(generation, int) or isinstance(generation, bool) or generation < 1:
        raise SyncError("member generation must be a positive integer")
    return name, generation


def _normalize_batch(batch):
    """Validate a batch and return a canonical, JSON-ready structure."""
    if not isinstance(batch, dict):
        raise SyncError("batch must be an object")
    source, seq = _event_id([batch.get("source"), batch.get("seq")])
    adds = batch.get("adds", [])
    removes = batch.get("removes", [])
    if not isinstance(adds, list) or not isinstance(removes, list):
        raise SyncError("adds and removes must be lists")
    if len(adds) + len(removes) == 0:
        raise SyncError("batch contains no operations")
    if len(adds) + len(removes) > MAX_BATCH_OPS:
        raise SyncError("batch operation limit exceeded")
    norm_adds = [list(_member_op(x)) for x in adds]
    norm_removes = []
    for row in removes:
        if not isinstance(row, (list, tuple)) or len(row) not in (2, 3):
            raise SyncError("remove operation must be [name, generation] or [name, generation, observed]")
        name, generation = _member_op(row)
        observed = None
        if len(row) == 3:
            observed_raw = row[2]
            if observed_raw is not None:
                if not isinstance(observed_raw, list):
                    raise SyncError("observed evidence must be a list of event ids")
                observed = [list(_event_id(x)) for x in observed_raw]
        norm_removes.append([name, generation, observed])
    return {
        "source": source,
        "seq": seq,
        "event": batch.get("event", "%s:%d" % (source, seq)),
        "adds": norm_adds,
        "removes": norm_removes,
    }


def replay(ledger):
    """Rebuild target state from the change ledger alone.

    This is the independent change model: it trusts only the immutable ledger
    records, never the materialized OR-set, and derives every causal dot,
    tombstone, watermark and generation fence itself.
    """
    if not isinstance(ledger, list):
        raise SyncError("ledger must be a list of records")
    orset = None
    clocks = {}
    watermarks = {}
    fences = {}
    index = {}
    pending_ev = []
    prev_hash = GENESIS_HASH
    for record in ledger:
        if record.get("prev") != prev_hash:
            raise AuditError("ledger hash chain broken at commit %s" % record.get("commit_seq"))
        expected = _record_hash(record, prev_hash)
        if record.get("hash") != expected:
            raise AuditError("ledger record hash mismatch at commit %s" % record.get("commit_seq"))
        prev_hash = expected
        if orset is None:
            orset = ORSet(record["target"])
        source = record["source"]
        # Re-derive dots from the per-source causal clock.
        derived = [(source, clocks.get(source, 0) + i) for i in range(1, len(record["adds"]) + 1)]
        recorded = [tuple(d) for _, _, d in record["adds"]]
        if derived != recorded:
            raise AuditError("recorded dots disagree with causal clock at commit %d" % record["commit_seq"])
        for (name, generation, dot) in record["adds"]:
            if generation < fences.get(name, 0):
                raise AuditError(
                    "ledger violates generation fencing for member %r at commit %d"
                    % (name, record["commit_seq"])
                )
            clocks[source] = clocks.get(source, 0) + 1
            if tuple(dot) != (source, clocks[source]):
                raise AuditError("non-contiguous source dot at commit %d" % record["commit_seq"])
            fences[name] = max(fences.get(name, 0), generation)
            key = member_key(name, generation)
            orset.adds.setdefault(key, set()).add(tuple(dot))
            index.setdefault(_event_id(record["event_id"]), []).append((key, tuple(dot)))
        for name, generation, observed in record["removes"]:
            if generation < fences.get(name, 0):
                raise AuditError(
                    "ledger violates generation fencing on remove for member %r at commit %d"
                    % (name, record["commit_seq"])
                )
            fences[name] = max(fences.get(name, 0), generation)
            key = member_key(name, generation)
            evidence = set()
            if observed is None:
                # Fallback: the remove observes every add-dot currently known
                # for this exact generational member (ORSet.remove semantics).
                evidence.update(orset.adds.get(key, set()))
            else:
                for event_id in observed:
                    dots = index.get(tuple(event_id))
                    if dots is None:
                        pending_ev.append([list(event_id), key])
                    else:
                        evidence.update(dot for dot_key, dot in dots if dot_key == key)
            still_pending = []
            for event_id, pending_key in pending_ev:
                dots = index.get(tuple(event_id))
                if dots is None:
                    still_pending.append([event_id, pending_key])
                else:
                    evidence.update(dot for dot_key, dot in dots if dot_key == pending_key)
            pending_ev = still_pending
            orset.removed.update(evidence)
        # Evidence can also be satisfied by an add landing in a later record
        # that contains no removes: re-scan the full pending set every commit.
        unresolved = []
        for event_id, pending_key in pending_ev:
            dots = index.get(tuple(event_id))
            if dots is None:
                unresolved.append([event_id, pending_key])
            else:
                orset.removed.update(
                    dot for dot_key, dot in dots if dot_key == pending_key
                )
        pending_ev = unresolved
        if record["seq"] != watermarks.get(source, 0) + 1:
            raise AuditError(
                "ledger breaks contiguous source progress for %r at seq %d "
                "(previous confirmed seq %d)"
                % (source, record["seq"], watermarks.get(source, 0))
            )
        watermarks[source] = record["seq"]
    if orset is None:
        return {
            "commit_seq": 0,
            "members": [],
            "watermarks": {},
            "clocks": {},
            "fences": {},
            "adds": {},
            "removed": [],
            "pending_evidence": [],
            "ledger_hash": GENESIS_HASH,
        }
    return {
        "commit_seq": len(ledger),
        "members": [list(decode_member(k)) for k in orset.values()],
        "watermarks": dict(sorted(watermarks.items())),
        "clocks": dict(sorted(clocks.items())),
        "fences": dict(sorted(fences.items())),
        "adds": {k: sorted(dots) for k, dots in orset.adds.items()},
        "removed": sorted(orset.removed),
        "pending_evidence": sorted(pending_ev),
        "ledger_hash": prev_hash,
    }


def _record_hash(record, prev_hash):
    body = {k: v for k, v in record.items() if k != "hash"}
    body["prev"] = prev_hash
    return _sha256(_canonical(body))


class SyncEngine:
    """Durable incremental sync engine for one target dimension table."""

    def __init__(self, target, directory=None):
        if not isinstance(target, str) or not target:
            raise SyncError("target dimension table identity required")
        self.target = target
        self.directory = directory
        self._lock = threading.RLock()
        self._dir_lock = None
        self.orset = ORSet(target)
        self.clocks = {}
        self.watermarks = {}
        self.fences = {}
        self.seen = set()           # committed (source, seq)
        self.pending = {}           # source -> held, gap-buffered batches
        self.held = {}              # (source, seq) -> original apply result
        self.index = {}             # (source, seq) -> [(member_key, dot)]
        self.pending_ev = []        # [(observed event id, member_key)]
        self.ledger = []
        self.rev = 0
        if directory is not None:
            os.makedirs(directory, exist_ok=True)
            self._acquire_dir_lock()
            try:
                if os.path.exists(os.path.join(directory, "manifest.json")):
                    payload = self._read_dir_payload(directory)
                    self._adopt_payload(payload, verify_rev=False)
                    self._guard_epoch(self.rev)
            except BaseException:
                # A refused recovery must not leave its acquired lock behind.
                self.release()
                raise

    def _acquire_dir_lock(self):
        """Best-effort exclusive directory lock against a second live writer."""
        lock_path = os.path.join(self.directory, "engine.lock")
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            raise StaleStateError("directory %s is already owned by another engine" % self.directory)
        os.write(fd, ("%d\n" % os.getpid()).encode())
        self._dir_lock = (lock_path, fd)

    def release(self):
        if self._dir_lock is None:
            return
        lock_path, fd = self._dir_lock
        os.close(fd)
        try:
            os.remove(lock_path)
        except OSError:
            pass
        self._dir_lock = None

    def _guard_epoch(self, rev):
        """The epoch file is a monotonic high-water mark outside the state set.

        Restoring an old on-disk image cannot roll the engine backwards: any
        image whose revision is below the durable epoch is rejected even when
        the manifest itself has been replaced by an older copy.
        """
        epoch_path = os.path.join(self.directory, "epoch")
        if os.path.exists(epoch_path):
            with open(epoch_path, encoding="utf-8") as fh:
                epoch = int(fh.read().strip() or "0")
            if rev < epoch:
                raise StaleStateError(
                    "refused re-entry of old disk state: image revision %d is below "
                    "durable epoch %d" % (rev, epoch)
                )
        else:
            epoch = 0
        if rev > epoch:
            tmp = epoch_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                fh.write(str(rev))
            os.replace(tmp, epoch_path)

    # ------------------------------------------------------------------ views

    def members(self):
        with self._lock:
            return [list(decode_member(k)) for k in self.orset.values()]

    def progress(self):
        with self._lock:
            return [
                {
                    "source": source,
                    "confirmed_seq": self.watermarks.get(source, 0),
                    "next_seq": self.watermarks.get(source, 0) + 1,
                    "held_pending": sorted(b["seq"] for b in self.pending.get(source, ())),
                }
                for source in sorted(set(self.watermarks) | set(self.pending))
            ]

    def query(self):
        with self._lock:
            return {
                "target": self.target,
                "version": self.rev,
                "commit_seq": len(self.ledger),
                "members": self.members(),
                "progress": self.progress(),
                "ledger_events": len(self.ledger),
            }

    def version(self, commit_seq):
        """Materialize the dimension table as of a past commit sequence."""
        with self._lock:
            if not isinstance(commit_seq, int) or isinstance(commit_seq, bool) or commit_seq < 0:
                raise SyncError("commit_seq must be a non-negative integer")
            if commit_seq > len(self.ledger):
                raise SyncError("commit_seq %d is ahead of ledger length %d" % (commit_seq, len(self.ledger)))
            rebuilt = replay(self.ledger[:commit_seq])
            return {
                "target": self.target,
                "commit_seq": commit_seq,
                "members": rebuilt["members"],
                "progress": [
                    {"source": s, "confirmed_seq": w, "next_seq": w + 1}
                    for s, w in rebuilt["watermarks"].items()
                ],
            }

    # ------------------------------------------------------------------ apply

    def apply_batch(self, batch):
        """Apply one add/remove batch; idempotent on (source, seq)."""
        norm = _normalize_batch(batch)
        if norm["source"] == self.target:
            raise SyncError("source identity must not reuse the target identity")
        event_id = (norm["source"], norm["seq"])
        batch_hash = _sha256(_canonical(norm))
        with self._lock:
            if event_id in self.seen:
                original = self.held[event_id]
                if original["batch_hash"] != batch_hash:
                    raise DuplicateBatchError(
                        "re-delivered batch for source event %r carries different content; "
                        "the original commit stands and cannot be applied twice" % (list(event_id),)
                    )
                return dict(original, replayed=True, content_match=True)
            buffered = self.pending.setdefault(norm["source"], [])
            if any(b["seq"] == norm["seq"] for b in buffered):
                # Redelivery while the gap before it is still open.
                for b in buffered:
                    if b["seq"] == norm["seq"]:
                        if _sha256(_canonical(b)) != batch_hash:
                            raise DuplicateBatchError(
                                "re-delivered buffered batch %r carries different content"
                                % (list(event_id),)
                            )
                        return {
                            "accepted": True,
                            "replayed": True,
                            "content_match": True,
                            "commit_seq": len(self.ledger),
                            "version": self.rev,
                            "batch_hash": batch_hash,
                            "members": self.members(),
                            "progress": self.progress(),
                            "buffered": True,
                        }
            if len(buffered) >= MAX_PENDING_PER_SOURCE:
                raise SyncError("held-batch limit reached for source %s" % norm["source"])
            buffered.append(norm)
            buffered.sort(key=lambda b: b["seq"])
            snapshot = self._snapshot_mutable()
            try:
                while self._drain_next(norm["source"]):
                    pass
            except StaleGenerationError:
                # A fenced batch is permanently invalid: drop it from the gap
                # buffer so later valid batches can resume the stream, while
                # leaving all previously committed state untouched.
                self._restore_mutable(snapshot)
                queue = self.pending.get(norm["source"], [])
                self.pending[norm["source"]] = [b for b in queue if b["seq"] != norm["seq"]]
                if not self.pending[norm["source"]]:
                    del self.pending[norm["source"]]
                raise
            except Exception:
                self._restore_mutable(snapshot)
                raise
            # Dimension-table commit, source watermark and the acknowledgement
            # record reach durability in the same single revision.
            result = {
                "accepted": True,
                "replayed": False,
                "event_id": list(event_id),
                "event": norm["event"],
                "commit_seq": len(self.ledger),
                "version": self.rev + 1,
                "batch_hash": batch_hash,
                "members": self.members(),
                "progress": self.progress(),
            }
            self.held[event_id] = result
            self._persist()
            result["version"] = self.rev
            return dict(result)

    def _drain_next(self, source):
        buffered = self.pending.get(source, [])
        expected = self.watermarks.get(source, 0) + 1
        if not buffered or buffered[0]["seq"] != expected:
            return False
        batch = buffered.pop(0)
        if not buffered:
            del self.pending[source]
        self._commit_one(batch)
        return True

    def _commit_one(self, batch):
        source = batch["source"]
        seq = batch["seq"]
        # Generation fencing: every operation must be at least as new as the
        # highest generation already committed for that name.
        for name, generation in batch["adds"]:
            known = self.fences.get(name, 0)
            if generation < known:
                raise StaleGenerationError(
                    "rejected generation %d for member %r from source %r seq %d: "
                    "generation %d was already committed; a reused name with an "
                    "older generation is not the same writer"
                    % (generation, name, source, seq, known)
                )
        for name, generation, _ in batch["removes"]:
            known = self.fences.get(name, 0)
            if generation < known:
                raise StaleGenerationError(
                    "rejected remove generation %d for member %r from source %r "
                    "seq %d: generation %d was already committed"
                    % (generation, name, source, seq, known)
                )
        add_records = []
        produced = []
        for name, generation in batch["adds"]:
            self.clocks[source] = self.clocks.get(source, 0) + 1
            dot = (source, self.clocks[source])
            key = member_key(name, generation)
            self.orset.adds.setdefault(key, set()).add(dot)
            self.fences[name] = max(self.fences.get(name, 0), generation)
            produced.append((key, dot))
            add_records.append([name, generation, list(dot)])
        self.index[(source, seq)] = produced
        remove_records = []
        for name, generation, observed in batch["removes"]:
            self.fences[name] = max(self.fences.get(name, 0), generation)
            key = member_key(name, generation)
            evidence = set()
            if observed is None:
                # Observes only add-dots currently visible for this member:
                # concurrent (not-yet-seen) adds cannot be tombstoned.
                evidence.update(self.orset.adds.get(key, set()))
            else:
                for event_id in observed:
                    dots = self.index.get(tuple(event_id))
                    if dots is None:
                        # Unacknowledged delete evidence must survive until the
                        # observed add becomes known. Never dropped.
                        self.pending_ev.append((tuple(event_id), key))
                    else:
                        evidence.update(dot for dot_key, dot in dots if dot_key == key)
            remove_records.append([name, generation, observed])
            self.orset.removed.update(evidence)
        # Resolve any evidence whose observed add has meanwhile arrived.
        unresolved = []
        for event_id, pending_key in self.pending_ev:
            dots = self.index.get(event_id)
            if dots is None:
                unresolved.append((event_id, pending_key))
            else:
                self.orset.removed.update(
                    dot for dot_key, dot in dots if dot_key == pending_key
                )
        self.pending_ev = unresolved
        self.seen.add((source, seq))
        self.watermarks[source] = seq
        record = {
            "commit_seq": len(self.ledger) + 1,
            "target": self.target,
            "source": source,
            "event_id": [source, seq],
            "event": batch["event"],
            "seq": seq,
            "adds": add_records,
            "removes": remove_records,
            "prev": self._ledger_hash(),
        }
        record["hash"] = _record_hash(record, record["prev"])
        self.ledger.append(record)

    # ------------------------------------------------------------- persistence

    def _ledger_hash(self):
        return self.ledger[-1]["hash"] if self.ledger else GENESIS_HASH

    def _snapshot_mutable(self):
        return (
            json.loads(self.orset.dump()),
            dict(self.clocks),
            dict(self.watermarks),
            dict(self.fences),
            set(self.seen),
            {s: [dict(b) for b in q] for s, q in self.pending.items()},
            {k: dict(v) for k, v in self.held.items()},
            {k: list(v) for k, v in self.index.items()},
            list(self.pending_ev),
            [dict(r) for r in self.ledger],
        )

    def _restore_mutable(self, snap):
        orset_dump, clocks, watermarks, fences, seen, pending, held, index, pending_ev, ledger = snap
        self.orset = ORSet.load(json.dumps(orset_dump))
        self.clocks = clocks
        self.watermarks = watermarks
        self.fences = fences
        self.seen = seen
        self.pending = {s: q for s, q in pending.items()}
        self.held = held
        self.index = {k: list(v) for k, v in index.items()}
        self.pending_ev = pending_ev
        self.ledger = ledger

    def _state_payload(self):
        next_rev = self.rev + 1
        payload = {
            "version": STATE_VERSION,
            "target": self.target,
            "rev": next_rev,
            "commit_seq": len(self.ledger),
            "orset": json.loads(self.orset.dump()),
            "clocks": dict(sorted(self.clocks.items())),
            "watermarks": dict(sorted(self.watermarks.items())),
            "fences": dict(sorted(self.fences.items())),
            "seen": sorted(map(list, self.seen)),
            "pending": {s: q for s, q in sorted(self.pending.items())},
            "held": {json.dumps(k): v for k, v in sorted(self.held.items())},
            "index": {json.dumps(k): [[dk, list(d)] for dk, d in v] for k, v in sorted(self.index.items())},
            "pending_evidence": [[list(e), k] for e, k in sorted(self.pending_ev)],
            "ledger": self.ledger,
        }
        payload["state_hash"] = _sha256(_canonical({k: v for k, v in payload.items() if k != "state_hash"}))
        return payload

    def _persist(self):
        """Advance the monotonically increasing durability revision.

        For directory-backed engines the revision line is also the durable
        image sequence; an interrupted write leaves only a ``.tmp`` file
        behind because the manifest is swapped in last (commit point).
        """
        payload = self._state_payload()
        new_rev = payload["rev"]
        if self.directory is None:
            self.rev = new_rev
            return
        fname = "state-%d-%s.json" % (new_rev, uuid.uuid4().hex[:12])
        tmp_state = os.path.join(self.directory, fname + ".tmp")
        state_path = os.path.join(self.directory, fname)
        with open(tmp_state, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, sort_keys=True)
        os.replace(tmp_state, state_path)
        manifest = {
            "version": STATE_VERSION,
            "target": self.target,
            "rev": new_rev,
            "state_file": fname,
            "state_hash": payload["state_hash"],
        }
        manifest_tmp = os.path.join(self.directory, "manifest.json.tmp")
        with open(manifest_tmp, "w", encoding="utf-8") as fh:
            json.dump(manifest, fh, ensure_ascii=False, sort_keys=True)
        os.replace(manifest_tmp, os.path.join(self.directory, "manifest.json"))
        self.rev = new_rev
        self._guard_epoch(new_rev)
        # Superseded images are garbage now; tombstones live inside the new one.
        for entry in os.listdir(self.directory):
            if entry.startswith("state-") and entry != fname and entry.endswith(".json"):
                try:
                    os.remove(os.path.join(self.directory, entry))
                except OSError:
                    pass

    def snapshot(self):
        """Export a serializable full state image (used by stateless callers)."""
        with self._lock:
            # A snapshot is a read at the current durability revision.
            payload = self._state_payload()
            payload["rev"] = self.rev
            payload["state_hash"] = _sha256(
                _canonical({k: v for k, v in payload.items() if k != "state_hash"})
            )
            return payload

    @classmethod
    def from_snapshot(cls, payload):
        engine = cls(payload["target"])
        engine._adopt_payload(payload, verify_rev=False)
        engine.audit(raise_on_diff=True)
        return engine

    def _adopt_payload(self, payload, verify_rev=True):
        if payload.get("version") != STATE_VERSION:
            raise StaleStateError("unsupported state version %r" % payload.get("version"))
        if payload.get("target") != self.target:
            raise StaleStateError(
                "state image belongs to target %r, refusing to load into %r"
                % (payload.get("target"), self.target)
            )
        incoming_rev = payload.get("rev")
        if not isinstance(incoming_rev, int) or incoming_rev < 0:
            raise StaleStateError("state image carries no valid revision")
        if verify_rev:
            if incoming_rev < self.rev:
                raise StaleStateError(
                    "refused stale state image: image revision %d is older than live "
                    "revision %d; old disk state cannot re-enter the commit stream"
                    % (incoming_rev, self.rev)
                )
            if incoming_rev == self.rev:
                same = payload.get("state_hash") == self._state_hash_quiet()
                if not same:
                    raise StaleStateError(
                        "refused divergent state image at revision %d: same revision "
                        "with different content indicates a forked or tampered image"
                        % incoming_rev
                    )
                return {"adopted": False, "reason": "already at revision %d" % incoming_rev}
        expected_hash = payload.get("state_hash")
        actual_hash = _sha256(
            _canonical({k: v for k, v in payload.items() if k != "state_hash"})
        )
        if expected_hash != actual_hash:
            raise StaleStateError("state image hash mismatch; refusing corrupted image")
        self.orset = ORSet.load(json.dumps(payload["orset"]))
        self.clocks = dict(payload["clocks"])
        self.watermarks = dict(payload["watermarks"])
        self.fences = dict(payload["fences"])
        self.seen = {tuple(x) for x in payload["seen"]}
        self.pending = {s: list(q) for s, q in payload["pending"].items()}
        self.held = {}
        for key, value in payload.get("held", {}).items():
            self.held[tuple(json.loads(key))] = value
        self.index = {}
        for key, dots in payload["index"].items():
            self.index[tuple(json.loads(key))] = [(dk, tuple(d)) for dk, d in dots]
        self.pending_ev = [(tuple(e), k) for e, k in payload["pending_evidence"]]
        self.ledger = list(payload["ledger"])
        self.rev = incoming_rev
        return {"adopted": True, "revision": incoming_rev}

    def _state_hash_quiet(self):
        payload = self._state_payload()
        return _sha256(_canonical({k: v for k, v in payload.items() if k != "state_hash"}))

    def import_state(self, payload):
        """Fenced re-entry of a state image; explains every refusal."""
        with self._lock:
            return self._adopt_payload(payload, verify_rev=True)

    @staticmethod
    def _read_dir_payload(directory):
        manifest_path = os.path.join(directory, "manifest.json")
        if not os.path.exists(manifest_path):
            raise StaleStateError("no manifest in %s" % directory)
        with open(manifest_path, encoding="utf-8") as fh:
            manifest = json.load(fh)
        state_path = os.path.join(directory, manifest["state_file"])
        if not os.path.exists(state_path):
            raise StaleStateError(
                "manifest references missing %s; commit point interrupted" % manifest["state_file"]
            )
        with open(state_path, encoding="utf-8") as fh:
            payload = json.load(fh)
        if payload.get("rev") != manifest["rev"]:
            raise StaleStateError("manifest revision disagrees with state image")
        if payload.get("state_hash") != manifest.get("state_hash"):
            raise StaleStateError("manifest hash disagrees with state image")
        epoch_path = os.path.join(directory, "epoch")
        if os.path.exists(epoch_path):
            with open(epoch_path, encoding="utf-8") as fh:
                epoch = int(fh.read().strip() or "0")
            if payload["rev"] < epoch:
                raise StaleStateError(
                    "refused re-entry of old disk state: image revision %d is below "
                    "durable epoch %d" % (payload["rev"], epoch)
                )
        return payload

    @classmethod
    def load_dir(cls, directory):
        payload = cls._read_dir_payload(directory)
        engine = cls(payload["target"], directory=directory)
        # Constructor already adopted the same payload and audited nothing yet;
        # run the independent change model explicitly as part of recovery.
        report = engine.audit()
        report["adoption"] = {"adopted": True, "revision": payload["rev"]}
        report["directory"] = directory
        return {"engine": engine, "payload": payload, "audit": report}

    def compact(self):
        """Rewrite durable state without dropping any causal metadata."""
        with self._lock:
            before = self.audit(raise_on_diff=True)
            if self.directory is None:
                return {"compacted": False, "reason": "in-memory engine", "audit": before}
            # Compaction is itself a durable revision: metadata is retained in
            # full, only the on-disk image sequence is rewritten.
            self._persist()
            after = self.audit(raise_on_diff=True)
            orphans = self._dir_anomalies()
            if not after["ok"] or not before["ok"]:
                raise AuditError("compaction left an audit discrepancy")
            return {
                "compacted": True,
                "revision": self.rev,
                "retained_tombstones": len(self.orset.removed),
                "retained_pending_evidence": len(self.pending_ev),
                "audit": after,
                "orphans": orphans,
            }

    def _dir_anomalies(self):
        if self.directory is None:
            return {"orphan_files": [], "temp_files": []}
        try:
            with open(os.path.join(self.directory, "manifest.json"), encoding="utf-8") as fh:
                manifest = json.load(fh)
            keep = {manifest["state_file"], "manifest.json", "epoch", "engine.lock"}
        except (OSError, ValueError):
            keep = {"manifest.json", "epoch", "engine.lock"}
        orphan_files, temp_files = [], []
        for entry in sorted(os.listdir(self.directory)):
            if entry.endswith(".tmp"):
                temp_files.append(entry)
            elif entry.startswith("state-") and entry.endswith(".json") and entry not in keep:
                orphan_files.append(entry)
        return {"orphan_files": orphan_files, "temp_files": temp_files}

    # ------------------------------------------------------------------ audit

    def audit(self, raise_on_diff=False):
        """Cross-check materialized state against the independent change model."""
        with self._lock:
            model = replay(self.ledger)
            diffs = []
            actual_members = [list(decode_member(k)) for k in self.orset.values()]
            if actual_members != model["members"]:
                diffs.append({"field": "members", "ledger": model["members"], "materialized": actual_members})
            if self.watermarks != model["watermarks"]:
                diffs.append({"field": "watermarks", "ledger": model["watermarks"], "materialized": self.watermarks})
            if self.clocks != model["clocks"]:
                diffs.append({"field": "clocks", "ledger": model["clocks"], "materialized": self.clocks})
            if self.fences != model["fences"]:
                diffs.append({"field": "fences", "ledger": model["fences"], "materialized": self.fences})
            actual_removed = sorted(self.orset.removed)
            if actual_removed != model["removed"]:
                diffs.append({"field": "removed", "ledger": model["removed"], "materialized": actual_removed})
            actual_pending = sorted([[list(e), k] for e, k in self.pending_ev])
            if actual_pending != model["pending_evidence"]:
                diffs.append({
                    "field": "pending_evidence",
                    "ledger": model["pending_evidence"],
                    "materialized": actual_pending,
                })
            if len(self.ledger) != model["commit_seq"]:
                diffs.append({"field": "commit_seq", "ledger": model["commit_seq"], "materialized": len(self.ledger)})
            anomalies = self._dir_anomalies()
            report = {
                "ok": not diffs,
                "target": self.target,
                "revision": self.rev,
                "commit_seq": len(self.ledger),
                "ledger_hash": model["ledger_hash"],
                "diffs": diffs,
                **anomalies,
            }
            if raise_on_diff and diffs:
                raise AuditError("independent change model mismatch: %s" % _canonical(diffs))
            return report


def reconcile_dir(directory):
    """Clean interrupted commit-point artifacts around a durable engine.

    Also clears a lock file left by a crashed process after the caller has
    verified no live engine owns the directory.
    """
    manifest_path = os.path.join(directory, "manifest.json")
    if not os.path.exists(manifest_path):
        raise StaleStateError("cannot reconcile %s without a manifest" % directory)
    with open(manifest_path, encoding="utf-8") as fh:
        manifest = json.load(fh)
    if not os.path.exists(os.path.join(directory, manifest["state_file"])):
        raise StaleStateError("committed state image missing; manual intervention required")
    removed = []
    lock_path = os.path.join(directory, "engine.lock")
    if os.path.exists(lock_path):
        with open(lock_path, encoding="utf-8") as fh:
            owner = fh.read().strip()
        try:
            os.kill(int(owner.splitlines()[0]), 0)
            alive = True
        except (ValueError, ProcessLookupError, PermissionError):
            alive = False
        if alive:
            raise StaleStateError("lock owner pid %s appears alive; refusing to steal the lock" % owner)
        os.remove(lock_path)
        removed.append("engine.lock")
    for entry in sorted(os.listdir(directory)):
        if entry.endswith(".tmp") or (
            entry.startswith("state-") and entry.endswith(".json") and entry != manifest["state_file"]
        ):
            os.remove(os.path.join(directory, entry))
            removed.append(entry)
    loaded = SyncEngine.load_dir(directory)
    loaded["engine"].release()
    return {"removed": removed, "audit": loaded["audit"]}
