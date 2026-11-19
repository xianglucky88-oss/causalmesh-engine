"""Incremental dimension-table synchronization tests.

Covers idempotent batches, watermark-coupled commits, observed-remove
concurrency, generational fencing, retained delete evidence, durable
recovery, compaction and the independent change model.
"""

import json
import os

import pytest

from causalmesh import sync
from causalmesh.api import sync_call


def _batch(source, seq, adds=(), removes=()):
    out = {"source": source, "seq": seq, "adds": list(adds), "removes": list(removes)}
    return out


def test_adds_publish_members_and_contiguous_source_progress():
    engine = sync.SyncEngine("dim")
    result = engine.apply_batch(_batch("src-a", 1, adds=[["alice", 1], ["bob", 1]]))
    assert result["accepted"] and result["commit_seq"] == 1 and result["version"] == 1
    assert engine.members() == [["alice", 1], ["bob", 1]]
    progress = {p["source"]: p for p in engine.progress()}
    assert progress["src-a"]["confirmed_seq"] == 1
    assert progress["src-a"]["next_seq"] == 2


def test_duplicate_batch_and_lost_ack_never_apply_twice():
    engine = sync.SyncEngine("dim")
    first = engine.apply_batch(_batch("src-a", 1, adds=[["alice", 1]]))
    retry = engine.apply_batch(_batch("src-a", 1, adds=[["alice", 1]]))
    assert retry["replayed"] is True
    assert retry["commit_seq"] == first["commit_seq"] == 1
    assert engine.audit(raise_on_diff=True)["ok"]
    # Same event identity with different content is an explicit rejection.
    with pytest.raises(sync.DuplicateBatchError):
        engine.apply_batch(_batch("src-a", 1, adds=[["alice", 1], ["mallory", 1]]))


def test_gap_buffering_advances_watermark_only_on_contiguous_seq():
    engine = sync.SyncEngine("dim")
    held = engine.apply_batch(_batch("src-a", 2, adds=[["bob", 1]]))
    # seq 2 is held behind missing seq 1: nothing confirmed yet.
    assert held["commit_seq"] == 0
    assert engine.progress()[0]["held_pending"] == [2]
    engine.apply_batch(_batch("src-a", 1, adds=[["alice", 1]]))
    progress = {p["source"]: p for p in engine.progress()}
    assert progress["src-a"]["confirmed_seq"] == 2
    assert engine.members() == [["alice", 1], ["bob", 1]]
    # Redelivery of a still-buffered event is idempotent too.
    again = engine.apply_batch(_batch("src-a", 1, adds=[["alice", 1]]))
    assert again["replayed"] is True and again["commit_seq"] == 2


def test_observed_remove_keeps_concurrent_add_regardless_of_arrival_order():
    # Remove arrives before the concurrent add it never observed.
    engine = sync.SyncEngine("dim")
    engine.apply_batch(_batch("src-a", 1, adds=[["alice", 1]]))
    # src-b's remove observes only src-a:1; a concurrent src-b add is separate.
    engine.apply_batch(
        _batch("src-b", 1, removes=[["alice", 1, [["src-a", 1]]]], adds=[["carol", 1]])
    )
    engine.apply_batch(_batch("src-b", 2, adds=[["alice", 2]]))
    assert ["alice", 2] in engine.members() and ["carol", 1] in engine.members()
    assert ["alice", 1] not in engine.members()
    # Reversed delivery order must reach the identical result (no LWW semantics).
    other = sync.SyncEngine("dim")
    other.apply_batch(_batch("src-a", 1, adds=[["alice", 1]]))
    other.apply_batch(_batch("src-b", 2, adds=[["alice", 2]]))
    other.apply_batch(
        _batch("src-b", 1, removes=[["alice", 1, [["src-a", 1]]]], adds=[["carol", 1]])
    )
    assert other.members() == engine.members()


def test_target_centric_remove_does_not_tombstone_later_concurrent_add():
    engine = sync.SyncEngine("dim")
    engine.apply_batch(_batch("src-a", 1, adds=[["alice", 1]]))
    engine.apply_batch(_batch("src-b", 1, removes=[["alice", 1]]))
    engine.apply_batch(_batch("src-b", 2, adds=[["alice", 1]]))
    # The fallback remove only saw the first dot; the re-add stays alive.
    assert engine.members() == [["alice", 1]]


def test_stale_generation_is_rejected_with_an_explanation():
    engine = sync.SyncEngine("dim")
    engine.apply_batch(_batch("src-a", 1, adds=[["alice", 2]]))
    with pytest.raises(sync.StaleGenerationError) as exc:
        engine.apply_batch(_batch("src-a", 2, adds=[["alice", 1]]))
    message = str(exc.value)
    assert "generation 1" in message and "generation 2" in message and "alice" in message
    # The rejected batch is dropped from the gap buffer, so the next contiguous
    # event is processed; a stale remove is fenced with the same explanation.
    with pytest.raises(sync.StaleGenerationError):
        engine.apply_batch(_batch("src-a", 2, removes=[["alice", 1]]))
    # A reused name at a fresh generation is a brand-new writer/element.
    engine.apply_batch(_batch("src-a", 2, adds=[["alice", 3]]))
    assert engine.members() == [["alice", 2], ["alice", 3]]


def test_unacknowledged_delete_evidence_survives_until_add_arrives():
    engine = sync.SyncEngine("dim")
    # Remove references an add that is still partitioned away.
    engine.apply_batch(_batch("src-b", 1, removes=[["alice", 1, [["src-a", 1]]]]))
    assert engine.audit(raise_on_diff=True)["ok"]
    assert len(engine.pending_ev) == 1
    assert engine.members() == []
    # When the partition heals, the retained evidence tombstones the add.
    engine.apply_batch(_batch("src-a", 1, adds=[["alice", 1]]))
    assert engine.members() == []
    assert engine.pending_ev == []
    assert ("src-a", 1) in engine.orset.removed


def test_independent_change_model_rebuilds_members_and_progress():
    engine = sync.SyncEngine("dim")
    engine.apply_batch(_batch("src-a", 1, adds=[["alice", 1], ["bob", 1]]))
    engine.apply_batch(_batch("src-b", 1, removes=[["alice", 1, [["src-a", 1]]]]))
    engine.apply_batch(_batch("src-a", 2, adds=[["carol", 2]]))
    model = sync.replay(engine.ledger)
    assert model["members"] == engine.members() == [["bob", 1], ["carol", 2]]
    assert model["watermarks"] == {"src-a": 2, "src-b": 1}
    report = engine.audit(raise_on_diff=True)
    assert report["ok"] and report["diffs"] == []
    # Tampering with materialized state is detected by the independent model.
    engine.orset.adds[sync.member_key("mallory", 1)] = {("attacker", 9)}
    assert engine.audit()["ok"] is False
    with pytest.raises(sync.AuditError):
        engine.audit(raise_on_diff=True)


def test_ledger_hash_chain_detects_rewritten_history():
    engine = sync.SyncEngine("dim")
    engine.apply_batch(_batch("src-a", 1, adds=[["alice", 1]]))
    forged = json.loads(json.dumps(engine.ledger))
    forged[0]["adds"][0][0] = "mallory"
    with pytest.raises(sync.AuditError):
        sync.replay(forged)


def test_replay_rejects_forged_source_progress_gap():
    engine = sync.SyncEngine("dim")
    engine.apply_batch(_batch("src-a", 1, adds=[["alice", 1]]))
    engine.apply_batch(_batch("src-a", 2, adds=[["bob", 1]]))
    forged = json.loads(json.dumps(engine.ledger))
    # Re-chain hashes after forging a jump in the confirmed source sequence.
    forged[1]["seq"] = 5
    forged[1]["event_id"] = ["src-a", 5]
    prev = ""
    for record in forged:
        record["prev"] = prev
        record["hash"] = sync._record_hash(record, prev)
        prev = record["hash"]
    with pytest.raises(sync.AuditError, match="contiguous source progress"):
        sync.replay(forged)


def test_version_query_materializes_past_dimension_table_releases():
    engine = sync.SyncEngine("dim")
    engine.apply_batch(_batch("src-a", 1, adds=[["alice", 1]]))
    engine.apply_batch(_batch("src-a", 2, removes=[["alice", 1]]))
    engine.apply_batch(_batch("src-a", 3, adds=[["bob", 2]]))
    assert engine.version(0)["members"] == []
    assert engine.version(1)["members"] == [["alice", 1]]
    assert engine.version(2)["members"] == []
    assert engine.version(3)["members"] == [["bob", 2]]
    with pytest.raises(sync.SyncError):
        engine.version(99)


def test_durable_recovery_reproduces_committed_state(tmp_path):
    directory = str(tmp_path / "engine")
    engine = sync.SyncEngine("dim", directory=directory)
    engine.apply_batch(_batch("src-a", 1, adds=[["alice", 1], ["bob", 1]]))
    engine.apply_batch(_batch("src-b", 1, removes=[["alice", 1, [["src-a", 1]]]]))
    expected_members = engine.members()
    expected_progress = engine.progress()
    engine.release()
    recovered = sync.SyncEngine("dim", directory=directory)
    assert recovered.members() == expected_members
    assert recovered.progress() == expected_progress
    assert recovered.audit(raise_on_diff=True)["ok"]
    # Retries after restart still cannot double-apply.
    again = recovered.apply_batch(_batch("src-a", 1, adds=[["alice", 1], ["bob", 1]]))
    assert again["replayed"] is True
    assert recovered.members() == expected_members


def test_old_disk_state_cannot_reenter_commit_stream(tmp_path):
    directory = str(tmp_path / "engine")
    engine = sync.SyncEngine("dim", directory=directory)
    engine.apply_batch(_batch("src-a", 1, adds=[["alice", 1]]))
    old_image = engine.snapshot()
    engine.apply_batch(_batch("src-a", 2, removes=[["alice", 1]]))
    assert engine.members() == []
    # Snapshot fencing: an older image is refused with a stated reason.
    with pytest.raises(sync.StaleStateError) as exc:
        engine.import_state(old_image)
    assert "older than live revision" in str(exc.value)
    # File-level fencing: restoring an old manifest/state below the epoch fails.
    engine.release()
    state_file = json.load(open(os.path.join(directory, "manifest.json")))["state_file"]
    with open(os.path.join(directory, state_file), encoding="utf-8") as fh:
        newer = json.load(fh)
    old_payload = json.loads(json.dumps(newer))
    old_payload["rev"] = 1
    old_payload["state_hash"] = sync._sha256(
        sync._canonical({k: v for k, v in old_payload.items() if k != "state_hash"})
    )
    with open(os.path.join(directory, "state-1-forged.json"), "w", encoding="utf-8") as fh:
        json.dump(old_payload, fh)
    forged_manifest = {
        "version": 1, "target": "dim", "rev": 1,
        "state_file": "state-1-forged.json", "state_hash": old_payload["state_hash"],
    }
    with open(os.path.join(directory, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(forged_manifest, fh)
    with pytest.raises(sync.StaleStateError, match="durable epoch"):
        sync.SyncEngine("dim", directory=directory)


def test_compaction_keeps_tombstones_and_pending_evidence(tmp_path):
    directory = str(tmp_path / "engine")
    engine = sync.SyncEngine("dim", directory=directory)
    engine.apply_batch(_batch("src-a", 1, adds=[["alice", 1]]))
    engine.apply_batch(_batch("src-a", 2, removes=[["alice", 1]]))
    engine.apply_batch(_batch("src-b", 1, removes=[["zoe", 1, [["src-z", 7]]]]))
    tombstones_before = len(engine.orset.removed)
    pending_before = len(engine.pending_ev)
    report = engine.compact()
    assert report["compacted"] is True
    assert report["retained_tombstones"] == tombstones_before == 1
    assert report["retained_pending_evidence"] == pending_before == 1
    assert engine.audit(raise_on_diff=True)["ok"]
    # Restart after compaction: no deleted member is resurrected, evidence kept.
    engine.release()
    recovered = sync.SyncEngine("dim", directory=directory)
    assert recovered.members() == []
    assert len(recovered.orset.removed) == 1
    assert len(recovered.pending_ev) == 1
    recovered.apply_batch(_batch("src-z", 7, adds=[["zoe", 1]]))
    assert recovered.members() == []


def test_commit_point_interruption_leaves_only_recoverable_artifacts(tmp_path):
    directory = str(tmp_path / "engine")
    engine = sync.SyncEngine("dim", directory=directory)
    engine.apply_batch(_batch("src-a", 1, adds=[["alice", 1]]))
    engine.release()
    # Simulate a crash mid-commit: temp files and an orphaned state image.
    open(os.path.join(directory, "state-9-z.json.tmp"), "w").write("{}")
    with open(os.path.join(directory, "manifest.json"), encoding="utf-8") as fh:
        manifest = json.load(fh)
    orphan = os.path.join(directory, "state-0-orphan.json")
    with open(orphan, "w", encoding="utf-8") as fh:
        json.dump({"version": 1}, fh)
    report = sync.reconcile_dir(directory)
    assert "state-9-z.json.tmp" in report["removed"]
    assert "state-0-orphan.json" in report["removed"]
    assert report["audit"]["ok"]
    recovered = sync.SyncEngine("dim", directory=directory)
    assert recovered.members() == [["alice", 1]]


def test_snapshot_roundtrip_preserves_full_state_and_rejects_tampering():
    engine = sync.SyncEngine("dim")
    engine.apply_batch(_batch("src-a", 1, adds=[["alice", 1]]))
    engine.apply_batch(_batch("src-a", 2, removes=[["alice", 1]]))
    snapshot = engine.snapshot()
    restored = sync.SyncEngine.from_snapshot(snapshot)
    assert restored.members() == []
    assert restored.audit(raise_on_diff=True)["ok"]
    tampered = json.loads(json.dumps(snapshot))
    tampered["watermarks"]["src-a"] = 99
    with pytest.raises(sync.StaleStateError, match="hash mismatch"):
        sync.SyncEngine.from_snapshot(tampered)
    wrong_target = json.loads(json.dumps(snapshot))
    wrong_target["target"] = "other-dim"
    wrong_target["state_hash"] = sync._sha256(
        sync._canonical({k: v for k, v in wrong_target.items() if k != "state_hash"})
    )
    other_engine = sync.SyncEngine("dim")
    with pytest.raises(sync.StaleStateError, match="belongs to target"):
        other_engine.import_state(wrong_target)


def test_stateless_http_style_call_chain_and_replay():
    started = sync_call({"action": "start", "target": "dim"})
    state = started["snapshot"]
    first = sync_call({"action": "apply", "state": state,
                       "batch": _batch("src-a", 1, adds=[["alice", 1]])})
    second = sync_call({"action": "apply", "state": first["snapshot"],
                        "batch": _batch("src-a", 2, removes=[["alice", 1]])})
    retry = sync_call({"action": "apply", "state": second["snapshot"],
                       "batch": _batch("src-a", 2, removes=[["alice", 1]])})
    assert retry["result"]["replayed"] is True
    model = sync_call({"action": "replay", "state": second["snapshot"]})["model"]
    assert model["members"] == [] and model["watermarks"] == {"src-a": 2}
    audit = sync_call({"action": "audit", "state": second["snapshot"]})["audit"]
    assert audit["ok"]
    with pytest.raises(ValueError):
        sync_call({"action": "apply", "batch": _batch("src-a", 1)})


def test_concurrent_threads_observe_single_effective_commit():
    engine = sync.SyncEngine("dim")
    import threading

    errors = []

    def worker():
        try:
            engine.apply_batch(_batch("src-a", 1, adds=[["alice", 1]]))
        except Exception as exc:  # noqa: BLE001 - surfaced via list
            errors.append(exc)
    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert len(engine.ledger) == 1
    assert engine.members() == [["alice", 1]]


def test_validation_rejects_malformed_batches():
    engine = sync.SyncEngine("dim")
    with pytest.raises(sync.SyncError):
        engine.apply_batch({"source": "src-a", "seq": 1, "adds": [], "removes": []})
    with pytest.raises(sync.SyncError):
        engine.apply_batch(_batch("src-a", 0, adds=[["alice", 1]]))
    with pytest.raises(sync.SyncError):
        engine.apply_batch(_batch("dim", 1, adds=[["alice", 1]]))
    with pytest.raises(sync.SyncError):
        engine.apply_batch(_batch("src-a", 1, adds=[["", 1]]))
    with pytest.raises(sync.SyncError):
        engine.apply_batch(_batch("src-a", 1, adds=[["alice", 0]]))
    with pytest.raises(sync.SyncError):
        engine.apply_batch(_batch("src-a", 1, removes=[["alice", 1, "not-a-list"]]))
