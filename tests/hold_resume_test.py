"""End-to-end checks for the hold (暂缓) / resume (继续) feature.

Runs the same application modules as the docker-compose deployment (real
redis-server, `app.web`, `app.worker`) and verifies:

  H1  hold a dispatched-but-unclaimed artifact -> held; a surviving pre-hold
      queue message can never write a conclusion; resume re-dispatches,
      re-reads the file and passes; exactly one conclusion.
  H2  repeated hold/resume clicks keep the current decision (dispatch_epoch
      bumps exactly once).
  H3  hold while a worker is mid-verification -> the in-flight conclusion is
      rejected; the row stays held; resume re-verifies and passes.
  H4  a message claimed BEFORE the hold cannot regain write eligibility after
      resume: the stalled old worker's finalize is rejected and the single
      conclusion comes from the post-resume dispatch.
  H5  conclusion committed first -> hold is rejected and the conclusion is
      shown (passed stays downloadable, failed stays failed).
  H6  resume survives a full service restart (web + workers + redis): the
      artifact is still auto-dispatched and verified.

Run:  .venv/bin/python tests/hold_resume_test.py
"""
from __future__ import annotations

import hashlib
import io
import os
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import time
import uuid
from pathlib import Path

import redis
import requests

ROOT = Path(__file__).resolve().parent.parent
RUNTIME = ROOT / "tests" / "runtime-hold"
REDIS_BIN = os.environ.get("REDIS_BIN",
                           "/tmp/redis-build/redis-7.4.2/src/redis-server")
PYTHON = str(ROOT / ".venv" / "bin" / "python")
PORT = 6402
WEB_PORT = 8102
BASE = f"http://127.0.0.1:{WEB_PORT}"

PASS, FAIL = "PASS", "FAIL"
results: list[tuple[str, str, str]] = []


def record(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, PASS if ok else FAIL, detail))
    print(f"[{PASS if ok else FAIL}] {name} {detail}")


def wait_tcp(host: str, port: int, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(0.3)
    return False


def wait_for(url: str, timeout: float = 25.0, expect: int = 200) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=2)
            if r.status_code == expect:
                return True
        except requests.RequestException:
            pass
        time.sleep(0.4)
    return False


def sql(aid: str) -> dict:
    conn = sqlite3.connect(RUNTIME / "data" / "app.db")
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM artifacts WHERE id=?", (aid,)).fetchone()
    conn.close()
    return dict(row) if row else {}


def latest_aid() -> str:
    conn = sqlite3.connect(RUNTIME / "data" / "app.db")
    aid = conn.execute(
        "SELECT id FROM artifacts ORDER BY created_at DESC LIMIT 1"
    ).fetchone()[0]
    conn.close()
    return aid


def wait_row(aid: str, pred, timeout: float = 30.0) -> dict:
    deadline = time.time() + timeout
    row = sql(aid)
    while time.time() < deadline:
        if row and pred(row):
            return row
        time.sleep(0.4)
        row = sql(aid)
    return row


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def upload(content: bytes, expected: str, filename: str = "bin.dat"):
    files = {"file": (filename, io.BytesIO(content),
                      "application/octet-stream")}
    return requests.post(f"{BASE}/upload",
                         data={"expected_sha256": expected},
                         files=files, timeout=15, allow_redirects=False)


def post_action(aid: str, action: str):
    return requests.post(f"{BASE}/artifacts/{aid}/{action}",
                         timeout=15, allow_redirects=False)


def base_env() -> dict:
    env = os.environ.copy()
    env.update({
        "REDIS_HOST": "127.0.0.1",
        "REDIS_PORT": str(PORT),
        "DATA_DIR": str(RUNTIME / "data"),
        "PORT": str(WEB_PORT),
        "CLAIM_MIN_IDLE_MS": "3000",
        "PROCESSING_STALE_SECONDS": "8",
        "DISPATCH_INTERVAL_SECONDS": "1",
        "READ_FAIL_MAX_BACKOFF": "1",
        "LOG_LEVEL": "INFO",
    })
    return env


def start_service(module: str, logname: str,
                  env_extra: dict | None = None) -> subprocess.Popen:
    env = base_env()
    if env_extra:
        env.update(env_extra)
    log = open(RUNTIME / logname, "ab", buffering=0)
    return subprocess.Popen([PYTHON, "-m", module],
                            cwd=str(ROOT), env=env,
                            stdout=log, stderr=subprocess.STDOUT)


def stop(p: subprocess.Popen | None, name: str,
         sig: int = signal.SIGTERM) -> None:
    if p is None or p.poll() is not None:
        return
    p.send_signal(sig)
    try:
        p.wait(timeout=10)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=5)
    print(f"  stopped {name} (rc={p.returncode})")


def start_redis() -> subprocess.Popen:
    (RUNTIME / "redis").mkdir(parents=True, exist_ok=True)
    log = open(RUNTIME / "redis.log", "ab", buffering=0)
    p = subprocess.Popen(
        [REDIS_BIN, "--port", str(PORT), "--dir", str(RUNTIME / "redis"),
         "--appendonly", "yes", "--appendfsync", "everysec",
         "--save", "", "--daemonize", "no"],
        stdout=log, stderr=subprocess.STDOUT)
    if not wait_tcp("127.0.0.1", PORT, 15):
        raise RuntimeError("redis did not start")
    return p


def rclient() -> redis.Redis:
    return redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True)


def pel_ids(r: redis.Redis) -> set[str]:
    try:
        return {p["message_id"] for p in r.xpending_range(
            "verify:jobs", "verifiers", "-", "+", 100)}
    except redis.RedisError:
        return set()


def cleanup_leftovers() -> None:
    me = os.getpid()
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit() or int(proc.name) == me:
            continue
        try:
            raw = (proc / "cmdline").read_bytes().decode(
                "utf-8", "replace").replace("\x00", " ")
        except (OSError, PermissionError):
            continue
        if "runtime-hold" in raw and "app." in raw:
            try:
                os.kill(int(proc.name), signal.SIGKILL)
                print(f"  cleaned leftover pid {proc.name}")
            except OSError:
                pass
    try:
        out = subprocess.run(["pgrep", "-f", f"redis-server.*--port {PORT}"],
                             capture_output=True, text=True)
        for pid in out.stdout.split():
            os.kill(int(pid), signal.SIGKILL)
    except (FileNotFoundError, OSError):
        pass
    deadline = time.time() + 5
    while time.time() < deadline:
        if not wait_tcp("127.0.0.1", PORT, 0.5) and not wait_tcp(
                "127.0.0.1", WEB_PORT, 0.5):
            return
        time.sleep(0.3)
    raise RuntimeError("test ports still occupied after cleanup")


# --------------------------------------------------------------------------- #
def main() -> int:
    cleanup_leftovers()
    if RUNTIME.exists():
        shutil.rmtree(RUNTIME)
    (RUNTIME / "data").mkdir(parents=True)

    redis_p = start_redis()
    r = rclient()
    web = start_service("app.web", "web.log")
    assert wait_for(f"{BASE}/healthz", 25), "web never came up"
    workers: list[subprocess.Popen] = []

    def start_worker(name: str, delay_ms: int = 0) -> subprocess.Popen:
        extra = {"WORKER_NAME": name}
        if delay_ms:
            extra["PRE_FINALIZE_DELAY_MS"] = str(delay_ms)
        p = start_service("app.worker", f"{name}.log", env_extra=extra)
        workers.append(p)
        return p

    try:
        # ------------------------------------------------------------------ #
        print("== H1 hold before claim; stale message cannot conclude; "
              "resume re-verifies ==")
        c1 = b"hold-me-" + uuid.uuid4().bytes
        h1 = sha(c1)
        upload(c1, h1, "h1.bin")
        aid1 = latest_aid()
        row = wait_row(aid1, lambda x: x["stream_id"] is not None, 15)
        record("H1a upload dispatched (stream bound) with no worker running",
               row.get("stream_id") is not None and row["status"] == "pending",
               f"stream={row.get('stream_id')}")

        resp = post_action(aid1, "hold")
        row = sql(aid1)
        page = requests.get(BASE, timeout=5).text
        record("H1b hold accepted -> status held, unbound, shown as 暂缓",
               resp.status_code == 302 and row["status"] == "held"
               and row["stream_id"] is None and row["worker_id"] is None
               and "暂缓" in page,
               f"status={row['status']}")
        dl = requests.get(f"{BASE}/artifacts/{aid1}/download", timeout=10)
        record("H1c held artifact cannot be downloaded (403)",
               dl.status_code == 403, f"dl={dl.status_code}")

        # A pre-hold queue message that survived (e.g. Redis was down when the
        # hold tried to delete it) must never produce a conclusion.
        r.xadd("verify:jobs", {"artifact_id": aid1, "epoch": "0"})
        w1 = start_worker("worker-h1")
        time.sleep(5)
        row = sql(aid1)
        record("H1d stale pre-hold message dropped: still held, untouched",
               row["status"] == "held" and row["attempts"] == 0
               and row["actual_sha256"] is None,
               f"status={row['status']} attempts={row['attempts']}")

        resp = post_action(aid1, "resume")
        row = sql(aid1)
        record("H1e resume -> pending in a new dispatch epoch",
               resp.status_code == 302 and row["status"] == "pending"
               and row["dispatch_epoch"] == 1,
               f"status={row['status']} epoch={row['dispatch_epoch']}")
        row = wait_row(aid1, lambda x: x["status"] == "passed", 30)
        dl = requests.get(f"{BASE}/artifacts/{aid1}/download", timeout=10)
        record("H1f resumed artifact auto-dispatched, file re-read, passed "
               "exactly once",
               row.get("status") == "passed"
               and row.get("actual_sha256") == h1
               and row.get("attempts") == 1
               and dl.status_code == 200 and dl.content == c1,
               f"status={row.get('status')} attempts={row.get('attempts')} "
               f"dl={dl.status_code}")
        record("H1g PEL empty after resume cycle",
               pel_ids(r) == set(), f"pel={pel_ids(r)}")

        # ------------------------------------------------------------------ #
        print("== H2 repeated clicks keep the current decision ==")
        stop(w1, "worker-h1")
        workers.remove(w1)
        c2 = b"double-click-" + uuid.uuid4().bytes
        h2 = sha(c2)
        upload(c2, h2, "h2.bin")
        aid2 = latest_aid()
        wait_row(aid2, lambda x: x["stream_id"] is not None, 15)
        post_action(aid2, "hold")
        post_action(aid2, "hold")  # repeat click
        row = sql(aid2)
        record("H2a hold twice -> still held, epoch unchanged",
               row["status"] == "held" and row["dispatch_epoch"] == 0,
               f"status={row['status']} epoch={row['dispatch_epoch']}")
        post_action(aid2, "resume")
        post_action(aid2, "resume")  # repeat click
        row = sql(aid2)
        record("H2b resume twice -> single new dispatch cycle (epoch == 1)",
               row["status"] == "pending" and row["dispatch_epoch"] == 1,
               f"status={row['status']} epoch={row['dispatch_epoch']}")
        w1 = start_worker("worker-h2")
        row = wait_row(aid2, lambda x: x["status"] == "passed", 30)
        record("H2c doubly-resumed artifact passes once",
               row.get("status") == "passed" and row.get("attempts") == 1
               and row.get("actual_sha256") == h2,
               f"status={row.get('status')} attempts={row.get('attempts')}")
        stop(w1, "worker-h2")
        workers.remove(w1)

        # ------------------------------------------------------------------ #
        print("== H3 hold mid-verification rejects the in-flight conclusion ==")
        wslow = start_worker("worker-slow", delay_ms=5000)
        c3 = b"hold-while-processing-" + uuid.uuid4().bytes
        h3 = sha(c3)
        upload(c3, h3, "h3.bin")
        aid3 = latest_aid()
        row = wait_row(aid3, lambda x: x["status"] == "processing", 20)
        record("H3a worker claimed the job (processing, finalize delayed)",
               row.get("status") == "processing"
               and row.get("worker_id") == "worker-slow",
               f"status={row.get('status')} worker={row.get('worker_id')}")
        post_action(aid3, "hold")
        row = sql(aid3)
        record("H3b hold commits while worker is mid-job -> held",
               row["status"] == "held" and row["worker_id"] is None,
               f"status={row['status']}")
        time.sleep(7)  # let the slow worker wake and try to finalize
        row = sql(aid3)
        record("H3c in-flight conclusion rejected; row stays held",
               row["status"] == "held" and row["actual_sha256"] is None,
               f"status={row['status']} actual={row['actual_sha256']}")
        post_action(aid3, "resume")
        row = wait_row(aid3, lambda x: x["status"] == "passed", 40)
        dl = requests.get(f"{BASE}/artifacts/{aid3}/download", timeout=10)
        record("H3d resume re-reads the file and passes; downloadable",
               row.get("status") == "passed" and row.get("actual_sha256") == h3
               and row.get("attempts") == 2
               and dl.status_code == 200 and dl.content == c3,
               f"status={row.get('status')} attempts={row.get('attempts')}")
        stop(wslow, "worker-slow")
        workers.remove(wslow)

        # ------------------------------------------------------------------ #
        print("== H4 pre-hold claimed message never regains write rights ==")
        wa = start_worker("worker-a", delay_ms=6000)
        c4 = b"stale-claimer-" + uuid.uuid4().bytes
        h4 = sha(c4)
        upload(c4, h4, "h4.bin")
        aid4 = latest_aid()
        row = wait_row(aid4, lambda x: x["status"] == "processing", 20)
        assert row.get("worker_id") == "worker-a", row
        post_action(aid4, "hold")
        post_action(aid4, "resume")
        wb = start_worker("worker-b")  # fast worker takes the new dispatch
        row = wait_row(aid4, lambda x: x["status"] == "passed", 30)
        first_conclusion = dict(row)
        time.sleep(8)  # worker-a wakes now and must fail to overwrite
        row = sql(aid4)
        record("H4 stalled pre-hold worker cannot write; single conclusion "
               "from the post-resume dispatch",
               row["status"] == "passed"
               and row["worker_id"] == "worker-b"
               and row["actual_sha256"] == h4
               and row["attempts"] == 2
               and row["updated_at"] == first_conclusion.get("updated_at"),
               f"worker={row['worker_id']} attempts={row['attempts']}")
        stop(wa, "worker-a")
        workers.remove(wa)
        stop(wb, "worker-b")
        workers.remove(wb)

        # ------------------------------------------------------------------ #
        print("== H5 conclusion first -> hold rejected, conclusion shown ==")
        w = start_worker("worker-h5")
        c5 = b"already-passed-" + uuid.uuid4().bytes
        h5 = sha(c5)
        upload(c5, h5, "h5.bin")
        aid5 = latest_aid()
        wait_row(aid5, lambda x: x["status"] == "passed", 30)
        c5b = b"already-failed-" + uuid.uuid4().bytes
        upload(c5b, "00" * 32, "h5b.bin")
        aid5b = latest_aid()
        wait_row(aid5b, lambda x: x["status"] == "failed", 30)
        sess = requests.Session()  # keep the flash cookie across redirects
        resp = sess.post(f"{BASE}/artifacts/{aid5}/hold", timeout=15)
        resp_b = sess.post(f"{BASE}/artifacts/{aid5b}/hold", timeout=15)
        row5, row5b = sql(aid5), sql(aid5b)
        dl5 = requests.get(f"{BASE}/artifacts/{aid5}/download", timeout=10)
        record("H5 hold rejected after conclusion; conclusion kept & shown",
               row5["status"] == "passed" and row5b["status"] == "failed"
               and "暂缓被拒绝" in resp.text and "passed" in resp.text
               and "暂缓被拒绝" in resp_b.text
               and dl5.status_code == 200,
               f"passed={row5['status']} failed={row5b['status']} "
               f"rejected_msg={'暂缓被拒绝' in resp.text}")
        # resume on a concluded artifact is rejected too
        resp = sess.post(f"{BASE}/artifacts/{aid5}/resume", timeout=15)
        record("H5b resume rejected after conclusion",
               sql(aid5)["status"] == "passed" and "继续被拒绝" in resp.text,
               f"status={sql(aid5)['status']}")
        stop(w, "worker-h5")
        workers.remove(w)

        # ------------------------------------------------------------------ #
        print("== H6 resume survives full restart (web + worker + redis) ==")
        c6 = b"restart-resume-" + uuid.uuid4().bytes
        h6 = sha(c6)
        upload(c6, h6, "h6.bin")
        aid6 = latest_aid()
        wait_row(aid6, lambda x: x["stream_id"] is not None, 15)
        post_action(aid6, "hold")
        post_action(aid6, "resume")
        row = wait_row(aid6, lambda x: x["stream_id"] is not None, 15)
        record("H6a resumed artifact re-dispatched before restart",
               row.get("status") == "pending"
               and row.get("stream_id") is not None,
               f"status={row.get('status')}")
        stop(web, "web")
        stop(redis_p, "redis")
        time.sleep(1)
        redis_p = start_redis()
        r = rclient()
        web = start_service("app.web", "web2.log")
        assert wait_for(f"{BASE}/healthz", 25)
        w6 = start_worker("worker-h6")
        row = wait_row(aid6, lambda x: x["status"] == "passed", 40)
        dl = requests.get(f"{BASE}/artifacts/{aid6}/download", timeout=10)
        record("H6b after full restart the resumed artifact passes; "
               "download works",
               row.get("status") == "passed" and row.get("actual_sha256") == h6
               and dl.status_code == 200 and dl.content == c6,
               f"status={row.get('status')} dl={dl.status_code}")
        stop(w6, "worker-h6")
        workers.remove(w6)

        time.sleep(2)
        record("H7 stream PEL empty at steady state",
               pel_ids(r) == set(), f"pel={pel_ids(r)}")
    finally:
        for p in workers:
            stop(p, "worker")
        stop(web, "web")
        stop(redis_p, "redis")

    print("\n================ SUMMARY ================")
    n_fail = 0
    for name, ok, detail in results:
        print(f"{ok:4} {name}  {detail}")
        if ok == FAIL:
            n_fail += 1
    print(f"\n{len(results) - n_fail}/{len(results)} checks passed")
    return 1 if n_fail else 0


if __name__ == "__main__":
    sys.exit(main())
