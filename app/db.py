"""SQLite persistence layer.

The database is the single source of truth for artifact state. All state
transitions use conditional UPDATEs inside ``BEGIN IMMEDIATE`` transactions so
that redelivery of a job (at-least-once semantics of Redis Streams) can never
produce a second conclusion or overwrite an already confirmed result.
"""
from __future__ import annotations

import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Iterable, Optional

from . import config

# Artifact lifecycle:
#   pending    -> processing -> passed / failed
#   processing -> pending     (reconciler reset when the stream message is gone)
#   pending stays pending while enqueue keeps failing (retried forever)
#   pending / processing -> held     (release engineer pauses verification)
#   held -> pending                  (resume: fresh dispatch cycle, the file
#                                     is re-read; dispatch_epoch is bumped so
#                                     every pre-hold message loses its right
#                                     to claim the row or write a conclusion)
ST_PENDING = "pending"
ST_PROCESSING = "processing"
ST_HELD = "held"
ST_PASSED = "passed"
ST_FAILED = "failed"
TERMINAL_STATES = (ST_PASSED, ST_FAILED)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS artifacts (
    id               TEXT PRIMARY KEY,
    original_name    TEXT NOT NULL,
    stored_path      TEXT NOT NULL,
    expected_sha256  TEXT NOT NULL,
    actual_sha256    TEXT,
    status           TEXT NOT NULL,
    attempts         INTEGER NOT NULL DEFAULT 0,
    last_error       TEXT,
    stream_id        TEXT,
    worker_id        TEXT,
    dispatch_epoch   INTEGER NOT NULL DEFAULT 0,
    next_enqueue_at  REAL NOT NULL DEFAULT 0,
    enqueue_attempts INTEGER NOT NULL DEFAULT 0,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_artifacts_status ON artifacts(status);
"""

_local = threading.local()
_init_lock = threading.Lock()
_initialized = False


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(
        config.DB_PATH,
        timeout=30.0,
        isolation_level=None,  # we manage transactions explicitly
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def get_conn() -> sqlite3.Connection:
    """One connection per thread; the dispatcher and each worker are threads."""
    conn = getattr(_local, "conn", None)
    if conn is None:
        conn = _connect()
        _local.conn = conn
    return conn


def init_db() -> None:
    global _initialized
    with _init_lock:
        if _initialized:
            return
        config.ensure_dirs()
        conn = get_conn()
        conn.executescript(_SCHEMA)
        _migrate(conn)
        _initialized = True


def _migrate(conn: sqlite3.Connection) -> None:
    """Add columns introduced after the first schema version.

    ``dispatch_epoch`` identifies the current dispatch cycle of an artifact;
    it is bumped on every resume so stream messages published before a hold
    can never claim the row or write a conclusion afterwards.
    """
    cols = {row["name"] for row in conn.execute(
        "PRAGMA table_info(artifacts)")}
    if "dispatch_epoch" not in cols:
        conn.execute(
            "ALTER TABLE artifacts "
            "ADD COLUMN dispatch_epoch INTEGER NOT NULL DEFAULT 0")


def create_artifact(original_name: str, stored_path: Path,
                    expected_sha256: str) -> str:
    """Persist an accepted upload in ``pending`` state.

    The row is committed *before* anything touches Redis, so an enqueue
    failure (or a web-process crash) leaves a durable pending record that the
    reconciler keeps dispatching.
    """
    init_db()
    artifact_id = uuid.uuid4().hex
    now = time.time()
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO artifacts (id, original_name, stored_path, expected_sha256,
                               status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (artifact_id, original_name, str(stored_path), expected_sha256,
         ST_PENDING, now, now),
    )
    return artifact_id


def list_by_status(status: str) -> list[sqlite3.Row]:
    init_db()
    conn = get_conn()
    return list(conn.execute(
        "SELECT * FROM artifacts WHERE status = ? ORDER BY created_at",
        (status,),
    ))


def get_artifact(artifact_id: str) -> Optional[sqlite3.Row]:
    init_db()
    conn = get_conn()
    return conn.execute(
        "SELECT * FROM artifacts WHERE id = ?", (artifact_id,)
    ).fetchone()


def fetch_due_pending(limit: int = 100) -> list[sqlite3.Row]:
    """Pending rows that still need a stream message (never enqueued, or a
    previous enqueue attempt failed and the backoff has elapsed)."""
    init_db()
    conn = get_conn()
    now = time.time()
    return list(conn.execute(
        """
        SELECT * FROM artifacts
        WHERE status = ? AND stream_id IS NULL AND next_enqueue_at <= ?
        ORDER BY created_at
        LIMIT ?
        """,
        (ST_PENDING, now, limit),
    ))


def attach_stream_id(artifact_id: str, stream_id: str, epoch: int) -> bool:
    """Atomically bind a freshly produced stream message to one pending row.

    Returns False if the row is no longer eligible (e.g. concurrently held,
    or resumed into a newer dispatch epoch), in which case the caller deletes
    the orphan stream message.
    """
    conn = get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT status, stream_id, dispatch_epoch FROM artifacts "
            "WHERE id = ?",
            (artifact_id,),
        ).fetchone()
        if (row is None or row["status"] != ST_PENDING
                or row["stream_id"] is not None
                or row["dispatch_epoch"] != epoch):
            conn.execute("COMMIT")
            return False
        now = time.time()
        conn.execute(
            """
            UPDATE artifacts
            SET stream_id = ?, enqueue_attempts = enqueue_attempts + 1,
                updated_at = ?
            WHERE id = ?
            """,
            (stream_id, now, artifact_id),
        )
        conn.execute("COMMIT")
        return True
    except Exception:
        conn.execute("ROLLBACK")
        raise


def record_enqueue_failure(artifact_id: str, error: str) -> None:
    """Back off (capped exponential) so a broken Redis is retried forever
    without hot-looping. The row stays pending and unbound."""
    conn = get_conn()
    now = time.time()
    row = conn.execute(
        "SELECT enqueue_attempts FROM artifacts WHERE id = ?", (artifact_id,)
    ).fetchone()
    attempts = (row["enqueue_attempts"] if row else 0) + 1
    backoff = min(2 ** min(attempts, 6), config.READ_FAIL_MAX_BACKOFF)
    conn.execute(
        """
        UPDATE artifacts
        SET enqueue_attempts = ?, last_error = ?, next_enqueue_at = ?,
            updated_at = ?
        WHERE id = ? AND status = ? AND stream_id IS NULL
        """,
        (attempts, error[:500], now + backoff, now, artifact_id, ST_PENDING),
    )


def reset_stuck_processing(ids: Iterable[str]) -> int:
    """Reconciler: rows stuck in ``processing`` whose stream message no
    longer exists go back to ``pending`` with no stream binding, so they are
    re-enqueued and redelivered. Only non-terminal rows are touched."""
    ids = list(ids)
    if not ids:
        return 0
    conn = get_conn()
    now = time.time()
    placeholders = ",".join("?" for _ in ids)
    cur = conn.execute(
        f"""
        UPDATE artifacts
        SET status = ?, stream_id = NULL, worker_id = NULL,
            last_error = 'stream message lost, re-dispatching',
            updated_at = ?
        WHERE id IN ({placeholders}) AND status = ?
        """,
        (ST_PENDING, now, *ids, ST_PROCESSING),
    )
    return cur.rowcount


def claim_for_processing(artifact_id: str, worker_id: str, stream_id: str,
                         epoch: int) -> Optional[sqlite3.Row]:
    """Worker: move one row to ``processing`` iff it is still ``pending``
    *and* the message belongs to the row's current dispatch cycle.

    This is the first idempotency gate: a redelivered message for a row that
    another worker already claimed (or that already has a conclusion) simply
    returns None and the message gets ACKed as a harmless duplicate.

    The epoch/binding checks make hold+resume safe: a message published
    before the artifact was held carries an older ``dispatch_epoch`` than the
    row after resume, so it can never claim the row again; only the message
    freshly published by the reconciler for the resumed row matches.

    A successful claim also binds the message to the row. This matters when
    the worker reads the message in the tiny window between the reconciler's
    XADD and its binding update: the claim then *is* the binding, the
    reconciler's attach fails harmlessly (it deletes its now-redundant stream
    copy), and the later finalize still matches on ``stream_id``.
    """
    conn = get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT * FROM artifacts WHERE id = ?", (artifact_id,)
        ).fetchone()
        if row is None or row["status"] != ST_PENDING:
            conn.execute("COMMIT")
            return None
        if row["dispatch_epoch"] != epoch:
            # Stale message from before a hold/resume cycle (or foreign).
            conn.execute("COMMIT")
            return None
        if row["stream_id"] is not None and row["stream_id"] != stream_id:
            # Bound to a different (newer) message; this one is superseded.
            conn.execute("COMMIT")
            return None
        now = time.time()
        conn.execute(
            """
            UPDATE artifacts
            SET status = ?, worker_id = ?, stream_id = ?,
                attempts = attempts + 1, updated_at = ?
            WHERE id = ? AND status = ?
            """,
            (ST_PROCESSING, worker_id, stream_id, now,
             artifact_id, ST_PENDING),
        )
        conn.execute("COMMIT")
        return conn.execute(
            "SELECT * FROM artifacts WHERE id = ?", (artifact_id,)
        ).fetchone()
    except Exception:
        conn.execute("ROLLBACK")
        raise


def takeover_for_processing(artifact_id: str, worker_id: str,
                            stream_id: str) -> Optional[sqlite3.Row]:
    """Claim a message obtained via XAUTOCLAIM (its original consumer died or
    stalled while the message was still pending-ack).

    Rules:
      * terminal rows            -> None (duplicate; caller ACKs, never rewrites)
      * held rows                -> None (verification paused; caller ACKs and
                                    drops the message — it can never conclude)
      * pending rows             -> normal claim (reconciler may have reset it
                                    and produced a newer message; only accept
                                    when this message is the one bound to it)
      * processing rows          -> accepted only when the row's bound stream
                                    message is exactly the claimed one, proving
                                    this is the same job being continued, not a
                                    stale message from an older dispatch cycle
    """
    conn = get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT * FROM artifacts WHERE id = ?", (artifact_id,)
        ).fetchone()
        if row is None or row["status"] in TERMINAL_STATES:
            conn.execute("COMMIT")
            return None
        if row["status"] == ST_PENDING:
            if row["stream_id"] != stream_id:
                # A newer dispatch superseded this claimed message.
                conn.execute("COMMIT")
                return None
        elif row["status"] == ST_PROCESSING:
            if row["stream_id"] != stream_id:
                # Stale message from a previous dispatch cycle; the row now
                # belongs to another live message. Do not touch it.
                conn.execute("COMMIT")
                return None
        else:
            conn.execute("COMMIT")
            return None
        now = time.time()
        conn.execute(
            """
            UPDATE artifacts
            SET status = ?, worker_id = ?, attempts = attempts + 1,
                updated_at = ?
            WHERE id = ?
            """,
            (ST_PROCESSING, worker_id, now, artifact_id),
        )
        conn.execute("COMMIT")
        return conn.execute(
            "SELECT * FROM artifacts WHERE id = ?", (artifact_id,)
        ).fetchone()
    except Exception:
        conn.execute("ROLLBACK")
        raise


def finalize_artifact(artifact_id: str, worker_id: str, stream_id: str,
                      status: str, actual_sha256: Optional[str],
                      error: Optional[str]) -> bool:
    """Write the conclusion iff the row is still owned by *this* message.

    Second idempotency gate: confirmed ``passed``/``failed`` rows can never be
    rewritten by a duplicate delivery or a takeover, because their status no
    longer matches the WHERE clause. The ``worker_id``/``stream_id`` clauses
    additionally guarantee that a message claimed *before* the artifact was
    held cannot write a conclusion after a resume re-dispatched the job —
    the row then belongs to a different worker/message (or is ``held``), so
    the conditional update matches nothing.
    """
    assert status in TERMINAL_STATES
    conn = get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT status, worker_id, stream_id FROM artifacts WHERE id = ?",
            (artifact_id,),
        ).fetchone()
        if (row is None or row["status"] != ST_PROCESSING
                or row["worker_id"] != worker_id
                or row["stream_id"] != stream_id):
            conn.execute("COMMIT")
            return False
        now = time.time()
        conn.execute(
            """
            UPDATE artifacts
            SET status = ?, actual_sha256 = ?, last_error = ?,
                worker_id = ?, updated_at = ?
            WHERE id = ? AND status = ? AND worker_id = ? AND stream_id = ?
            """,
            (status, actual_sha256, error, worker_id, now,
             artifact_id, ST_PROCESSING, worker_id, stream_id),
        )
        conn.execute("COMMIT")
        return True
    except Exception:
        conn.execute("ROLLBACK")
        raise


def record_read_failure(artifact_id: str, worker_id: str, stream_id: str,
                        error: str) -> bool:
    """A file read/hash failure is *not* a digest mismatch: the row returns to
    ``pending`` with capped exponential backoff and is retried.

    The stream binding is kept until the worker ACKs the old message (see
    :func:`detach_after_ack`); this ordering guarantees there is never a
    window where both the old PEL entry and a freshly published retry message
    exist for the same job.

    Only the message that currently owns the row may push it back to
    ``pending``: a stale (pre-hold) message finding the row re-dispatched to
    someone else must not undo that dispatch.
    """
    conn = get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT status, attempts, worker_id, stream_id FROM artifacts "
            "WHERE id = ?",
            (artifact_id,),
        ).fetchone()
        if (row is None or row["status"] != ST_PROCESSING
                or row["worker_id"] != worker_id
                or row["stream_id"] != stream_id):
            conn.execute("COMMIT")
            return False
        attempts = row["attempts"]
        backoff = min(2 ** min(attempts, 6), config.READ_FAIL_MAX_BACKOFF)
        now = time.time()
        conn.execute(
            """
            UPDATE artifacts
            SET status = ?, worker_id = ?,
                last_error = ?, next_enqueue_at = ?, updated_at = ?
            WHERE id = ? AND status = ?
            """,
            (ST_PENDING, worker_id,
             f"read failure, will retry: {error}"[:500],
             now + backoff, now, artifact_id, ST_PROCESSING),
        )
        conn.execute("COMMIT")
        return True
    except Exception:
        conn.execute("ROLLBACK")
        raise


def detach_after_ack(artifact_id: str, stream_id: str) -> None:
    """Clear the stream binding of a retry-pending row, but only while the
    row is still bound to *this* message. Called after XACK so the reconciler
    can publish a fresh retry message without ever duplicating one."""
    conn = get_conn()
    conn.execute(
        """
        UPDATE artifacts
        SET stream_id = NULL, updated_at = ?
        WHERE id = ? AND status = ? AND stream_id = ?
        """,
        (time.time(), artifact_id, ST_PENDING, stream_id),
    )


# Hold/resume result codes.
HOLD_HELD = "held"            # newly paused by this call
HOLD_ALREADY = "already"      # was already held (repeat click keeps decision)
HOLD_TERMINAL = "terminal"    # a conclusion was committed first -> rejected
HOLD_MISSING = "missing"

RESUME_RESUMED = "resumed"    # held -> pending, fresh dispatch cycle started
RESUME_ACTIVE = "active"      # already back in the verify pipeline (repeat
                              # click keeps the resume decision)
RESUME_TERMINAL = "terminal"  # concluded in the meantime -> rejected
RESUME_MISSING = "missing"


def hold_artifact(artifact_id: str) -> tuple[str, Optional[sqlite3.Row]]:
    """Pause verification of an undecided artifact.

    Atomic with the worker's claim/finalize transitions (``BEGIN
    IMMEDIATE``), so exactly one of "conclusion committed" / "hold committed"
    wins:
      * if a conclusion exists, the hold is rejected and the row (with its
        confirmed status) is returned for display;
      * otherwise the row becomes ``held`` and its stream binding is dropped,
        so any already-dispatched or already-claimed message loses the right
        to write a conclusion (every worker-side write is conditional on
        ``processing`` + the current binding).

    Holding an already-held row is a no-op so repeated clicks keep the
    current decision.
    """
    conn = get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT * FROM artifacts WHERE id = ?", (artifact_id,)
        ).fetchone()
        if row is None:
            conn.execute("COMMIT")
            return HOLD_MISSING, None
        if row["status"] in TERMINAL_STATES:
            conn.execute("COMMIT")
            return HOLD_TERMINAL, row
        if row["status"] == ST_HELD:
            conn.execute("COMMIT")
            return HOLD_ALREADY, row
        now = time.time()
        conn.execute(
            """
            UPDATE artifacts
            SET status = ?, stream_id = NULL, worker_id = NULL,
                updated_at = ?
            WHERE id = ? AND status IN (?, ?)
            """,
            (ST_HELD, now, artifact_id, ST_PENDING, ST_PROCESSING),
        )
        conn.execute("COMMIT")
        # Return the pre-hold row: its ``stream_id`` (now unbound) lets the
        # caller best-effort delete the stale queue message.
        return HOLD_HELD, row
    except Exception:
        conn.execute("ROLLBACK")
        raise


def resume_artifact(artifact_id: str) -> tuple[str, Optional[sqlite3.Row]]:
    """Resume a held artifact: back to ``pending`` in a *new* dispatch epoch.

    The row is immediately due for the reconciler (``next_enqueue_at = 0``),
    which publishes a fresh message; the worker then re-reads the stored file
    from disk and verifies it from scratch. Bumping ``dispatch_epoch``
    invalidates every message published before the hold: they can neither
    claim the row nor write a conclusion, so exactly one conclusion — from
    the post-resume verification — can ever be recorded. The decision is
    durable DB state, so the dispatch still happens after a web/worker
    restart or after the old queue messages were ACKed.
    """
    conn = get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT * FROM artifacts WHERE id = ?", (artifact_id,)
        ).fetchone()
        if row is None:
            conn.execute("COMMIT")
            return RESUME_MISSING, None
        if row["status"] in TERMINAL_STATES:
            conn.execute("COMMIT")
            return RESUME_TERMINAL, row
        if row["status"] in (ST_PENDING, ST_PROCESSING):
            # Already resumed (or never held): keep the current decision.
            conn.execute("COMMIT")
            return RESUME_ACTIVE, row
        now = time.time()
        conn.execute(
            """
            UPDATE artifacts
            SET status = ?, stream_id = NULL, worker_id = NULL,
                dispatch_epoch = dispatch_epoch + 1,
                next_enqueue_at = 0, last_error = NULL, updated_at = ?
            WHERE id = ? AND status = ?
            """,
            (ST_PENDING, now, artifact_id, ST_HELD),
        )
        conn.execute("COMMIT")
        return RESUME_RESUMED, conn.execute(
            "SELECT * FROM artifacts WHERE id = ?", (artifact_id,)
        ).fetchone()
    except Exception:
        conn.execute("ROLLBACK")
        raise
