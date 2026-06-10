"""Helpers for the HW09 local micro-benchmark of distributed SQL engines.

We spin up three engines in Docker and talk to each over its native protocol:

    Trino       HTTP REST     localhost:8080   (memory connector, schema hw09)
    ClickHouse  HTTP          localhost:8123   (database hw09)
    StarRocks   MySQL wire    localhost:9030   (database hw09, root / no password)

Images are pinned via env vars (see the top of experiments.py). All three are
single-node containers - this is a *micro*-bench to feel the engines on the same
laptop, not a cluster benchmark. For real cluster numbers see report.md, which
leans on the official StarRocks TPC-DS and ClickBench results.
"""
import subprocess
import time

import pymysql
import requests

TRINO_HTTP = "http://localhost:8080"
CLICKHOUSE_HTTP = "http://localhost:8123"
STARROCKS_HOST = "127.0.0.1"
STARROCKS_PORT = 9030


def sh(cmd: list[str], check: bool = True) -> str:
    """Run a command, return stripped stdout."""
    proc = subprocess.run(cmd, check=check, text=True, capture_output=True)
    return proc.stdout.strip()


def docker_rm(name: str) -> None:
    subprocess.run(["docker", "rm", "-f", name], check=False, capture_output=True, text=True)


def docker_pull(image: str) -> None:
    print(f"[pull] {image}")
    subprocess.run(["docker", "pull", image], check=True)


def wait_until(fn, timeout: int, what: str) -> None:
    """Poll fn() until it returns truthy or we run out of time."""
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            if fn():
                return
        except Exception as e:  # noqa: BLE001 - container is still booting
            last_err = e
        time.sleep(2)
    raise RuntimeError(f"{what} did not become ready in {timeout}s. Last error: {last_err}")


def time_action(fun):
    """Measure wall-clock of a single fun() call. Returns (result, seconds)."""
    t0 = time.perf_counter()
    res = fun()
    return res, time.perf_counter() - t0


# --------------------------------------------------------------------------- #
# Per-engine query runners
# --------------------------------------------------------------------------- #
def trino_query(sql: str) -> list[list]:
    """Trino streams results across paginated URIs; follow nextUri to the end."""
    headers = {"X-Trino-User": "bench", "X-Trino-Catalog": "memory", "X-Trino-Schema": "hw09"}
    resp = requests.post(f"{TRINO_HTTP}/v1/statement", data=sql.encode("utf-8"),
                         headers=headers, timeout=120)
    resp.raise_for_status()
    payload = resp.json()
    rows: list[list] = []
    while True:
        if "error" in payload:
            raise RuntimeError(payload["error"])
        rows.extend(payload.get("data", []))
        next_uri = payload.get("nextUri")
        if not next_uri:
            break
        payload = requests.get(next_uri, timeout=120).json()
    return rows


def clickhouse_query(sql: str) -> str:
    resp = requests.post(CLICKHOUSE_HTTP + "/", params={"query": sql}, timeout=120)
    resp.raise_for_status()
    return resp.text.strip()


def _starrocks_conn():
    return pymysql.connect(host=STARROCKS_HOST, port=STARROCKS_PORT, user="root",
                           password="", autocommit=True, cursorclass=pymysql.cursors.Cursor)


def starrocks_query(sql: str) -> list[tuple]:
    with _starrocks_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return list(cur.fetchall()) if cur.description else []


RUNNERS = {
    "trino": trino_query,
    "clickhouse": clickhouse_query,
    "starrocks": starrocks_query,
}


# --------------------------------------------------------------------------- #
# Bulk insert (shared INSERT ... VALUES path for all three engines)
# --------------------------------------------------------------------------- #
def _sql_value(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)


def insert_values(engine: str, table: str, columns: list[str],
                  rows: list[tuple], batch: int) -> None:
    run = RUNNERS[engine]
    cols = ", ".join(columns)
    for i in range(0, len(rows), batch):
        chunk = rows[i:i + batch]
        values = ", ".join("(" + ", ".join(_sql_value(v) for v in row) + ")" for row in chunk)
        run(f"INSERT INTO {table} ({cols}) VALUES {values}")
