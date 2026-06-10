"""HW09 - local micro-benchmark of distributed SQL engines: Trino, StarRocks,
ClickHouse.

We run the *same* tiny star schema (fact_sales + dim_customers + dim_products)
and the *same* 4 analytical queries on all three engines, in single-node Docker
containers, and measure load time and warm query latency. This is a feel-test on
one laptop - the headline cluster numbers in report.md come from the official
StarRocks TPC-DS 1 TB benchmark and from ClickBench (see clickbench_metrics.py).

Queries:
    q1  daily aggregation (SUM + COUNT, GROUP BY date)
    q2  filter + join + aggregation by region
    q3  two joins + GROUP BY + LIMIT 10 (top regions/categories)
    q4  COUNT(DISTINCT) + SUM by customer segment

Usage:
    python experiments.py                      # all engines, default 100k facts
    python experiments.py --engine trino       # one engine
    python experiments.py --fact-rows 50000 --json results.json

Requires a running Docker daemon. Tune images via the HW09_* env vars below.
"""
import argparse
import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path

import bench_helpers as bh

ROOT = Path(__file__).resolve().parent
TRINO_CATALOG = ROOT / "docker" / "trino" / "catalog" / "memory.properties"
RESULTS_PATH = ROOT / "results.json"

IMAGES = {
    "clickhouse": os.environ.get("HW09_CLICKHOUSE_IMAGE", "clickhouse/clickhouse-server:24.8"),
    "trino": os.environ.get("HW09_TRINO_IMAGE", "trinodb/trino:475"),
    "starrocks": os.environ.get("HW09_STARROCKS_IMAGE", "starrocks/allin1-ubuntu:3.3.5"),
}
CONTAINERS = {k: f"hw09-{k}" for k in IMAGES}

FACT_ROWS = int(os.environ.get("HW09_FACT_ROWS", "100000"))
CUSTOMER_ROWS = int(os.environ.get("HW09_CUSTOMER_ROWS", "10000"))
PRODUCT_ROWS = int(os.environ.get("HW09_PRODUCT_ROWS", "1000"))
INSERT_BATCH = int(os.environ.get("HW09_INSERT_BATCH", "2000"))
MEASURE_RUNS = 3

REGIONS = ["north", "south", "west", "east", "center"]
SEGMENTS = ["retail", "small_business", "enterprise"]
CATEGORIES = ["books", "tech", "home", "sport", "food"]


@dataclass
class EngineResult:
    engine: str
    version: str
    load_time_s: float
    query_times_s: dict[str, list[float]] = field(default_factory=dict)


# --------------------------------------------------------------------------- #
# Deterministic synthetic dataset (no RNG -> same data for every engine/run)
# --------------------------------------------------------------------------- #
def generate_data(fact_rows: int) -> dict[str, list[tuple]]:
    customers = [
        (cid, REGIONS[(cid - 1) % len(REGIONS)], SEGMENTS[(cid - 1) % len(SEGMENTS)])
        for cid in range(1, CUSTOMER_ROWS + 1)
    ]
    products = [
        (pid, CATEGORIES[(pid - 1) % len(CATEGORIES)], round(10 + ((pid * 13) % 4000) / 100, 2))
        for pid in range(1, PRODUCT_ROWS + 1)
    ]
    sales = []
    for sid in range(1, fact_rows + 1):
        customer_id = ((sid * 17) % CUSTOMER_ROWS) + 1
        product_id = ((sid * 13) % PRODUCT_ROWS) + 1
        month = ((sid * 7) % 12) + 1
        day = ((sid * 11) % 28) + 1
        qty = (sid % 5) + 1
        discount = round((sid % 10) / 100, 2)
        unit_price = round(10 + ((product_id * 13) % 4000) / 100, 2)
        amount = round(qty * unit_price * (1 - discount), 2)
        sales.append((sid, customer_id, product_id, f"2024-{month:02d}-{day:02d}",
                      qty, amount, discount))
    return {"customers": customers, "products": products, "sales": sales}


# --------------------------------------------------------------------------- #
# Container lifecycle
# --------------------------------------------------------------------------- #
def start_clickhouse() -> None:
    bh.docker_rm(CONTAINERS["clickhouse"])
    bh.sh(["docker", "run", "-d", "--name", CONTAINERS["clickhouse"],
           "-p", "8123:8123", "-p", "9000:9000", IMAGES["clickhouse"]])
    bh.wait_until(lambda: bh.clickhouse_query("SELECT 1") == "1", 180, "ClickHouse")


def start_trino() -> None:
    bh.docker_rm(CONTAINERS["trino"])
    bh.sh(["docker", "run", "-d", "--name", CONTAINERS["trino"], "-p", "8080:8080",
           "-v", f"{TRINO_CATALOG}:/etc/trino/catalog/memory.properties:ro",
           IMAGES["trino"]])
    bh.wait_until(lambda: len(bh.trino_query("SHOW CATALOGS")) > 0, 240, "Trino")


def start_starrocks() -> None:
    bh.docker_rm(CONTAINERS["starrocks"])
    bh.sh(["docker", "run", "-d", "--name", CONTAINERS["starrocks"],
           "-p", "8030:8030", "-p", "9030:9030", IMAGES["starrocks"]])
    bh.wait_until(lambda: len(bh.starrocks_query("SELECT 1")) > 0, 360, "StarRocks")


# --------------------------------------------------------------------------- #
# Schema + load (one setup per engine). Returns load time in seconds.
# --------------------------------------------------------------------------- #
def setup_clickhouse(data) -> EngineResult:
    q = bh.clickhouse_query
    q("DROP DATABASE IF EXISTS hw09")
    q("CREATE DATABASE hw09")
    q("CREATE TABLE hw09.dim_customers (customer_id Int32, region String, segment String)"
      " ENGINE = MergeTree ORDER BY customer_id")
    q("CREATE TABLE hw09.dim_products (product_id Int32, category String, base_price Float64)"
      " ENGINE = MergeTree ORDER BY product_id")
    q("CREATE TABLE hw09.fact_sales (sale_id Int32, customer_id Int32, product_id Int32,"
      " sale_date Date, qty Int32, amount Float64, discount Float64)"
      " ENGINE = MergeTree ORDER BY sale_id")
    load = _load("clickhouse", data, "hw09")
    return EngineResult("ClickHouse", q("SELECT version()"), load)


def setup_trino(data) -> EngineResult:
    q = bh.trino_query
    try:
        q("DROP SCHEMA memory.hw09 CASCADE")
    except Exception:  # noqa: BLE001 - schema may not exist yet
        pass
    q("CREATE SCHEMA memory.hw09")
    q("CREATE TABLE memory.hw09.dim_customers (customer_id INTEGER, region VARCHAR, segment VARCHAR)")
    q("CREATE TABLE memory.hw09.dim_products (product_id INTEGER, category VARCHAR, base_price DOUBLE)")
    q("CREATE TABLE memory.hw09.fact_sales (sale_id INTEGER, customer_id INTEGER, product_id INTEGER,"
      " sale_date DATE, qty INTEGER, amount DOUBLE, discount DOUBLE)")
    load = _load("trino", data, "memory.hw09")
    return EngineResult("Trino", q("SELECT version()")[0][0], load)


def setup_starrocks(data) -> EngineResult:
    q = bh.starrocks_query
    q("DROP DATABASE IF EXISTS hw09")
    q("CREATE DATABASE hw09")
    q("CREATE TABLE hw09.dim_customers (customer_id INT, region STRING, segment STRING)"
      " DUPLICATE KEY(customer_id) DISTRIBUTED BY HASH(customer_id) BUCKETS 1"
      " PROPERTIES('replication_num'='1')")
    q("CREATE TABLE hw09.dim_products (product_id INT, category STRING, base_price DOUBLE)"
      " DUPLICATE KEY(product_id) DISTRIBUTED BY HASH(product_id) BUCKETS 1"
      " PROPERTIES('replication_num'='1')")
    q("CREATE TABLE hw09.fact_sales (sale_id INT, customer_id INT, product_id INT,"
      " sale_date DATE, qty INT, amount DOUBLE, discount DOUBLE)"
      " DUPLICATE KEY(sale_id) DISTRIBUTED BY HASH(sale_id) BUCKETS 1"
      " PROPERTIES('replication_num'='1')")
    load = _load("starrocks", data, "hw09")
    return EngineResult("StarRocks", str(q("SELECT version()")[0][0]), load)


def _load(engine: str, data, schema: str) -> float:
    t0 = time.perf_counter()
    bh.insert_values(engine, f"{schema}.dim_customers",
                     ["customer_id", "region", "segment"], data["customers"], INSERT_BATCH)
    bh.insert_values(engine, f"{schema}.dim_products",
                     ["product_id", "category", "base_price"], data["products"], INSERT_BATCH)
    bh.insert_values(engine, f"{schema}.fact_sales",
                     ["sale_id", "customer_id", "product_id", "sale_date", "qty", "amount", "discount"],
                     data["sales"], INSERT_BATCH)
    return time.perf_counter() - t0


# --------------------------------------------------------------------------- #
# The 4 analytical queries, per engine dialect (schema prefix differs).
# --------------------------------------------------------------------------- #
def _queries(schema: str, date_fn) -> dict[str, str]:
    d1, d2 = date_fn("2024-06-01"), date_fn("2024-08-31")
    return {
        "q1_daily_agg": f"""
            SELECT sale_date, SUM(amount) AS revenue, COUNT(*) AS orders_cnt
            FROM {schema}.fact_sales
            GROUP BY sale_date ORDER BY sale_date""",
        "q2_filter_region": f"""
            SELECT c.region, SUM(f.amount) AS revenue, AVG(f.qty) AS avg_qty
            FROM {schema}.fact_sales f
            JOIN {schema}.dim_customers c ON f.customer_id = c.customer_id
            WHERE f.sale_date BETWEEN {d1} AND {d2}
            GROUP BY c.region ORDER BY revenue DESC""",
        "q3_join_top10": f"""
            SELECT c.region, p.category, SUM(f.amount) AS revenue, COUNT(*) AS orders_cnt
            FROM {schema}.fact_sales f
            JOIN {schema}.dim_customers c ON f.customer_id = c.customer_id
            JOIN {schema}.dim_products p ON f.product_id = p.product_id
            GROUP BY c.region, p.category ORDER BY revenue DESC LIMIT 10""",
        "q4_distinct_segment": f"""
            SELECT c.segment, COUNT(DISTINCT f.customer_id) AS buyers, SUM(f.amount) AS revenue
            FROM {schema}.fact_sales f
            JOIN {schema}.dim_customers c ON f.customer_id = c.customer_id
            WHERE f.discount >= 0.05
            GROUP BY c.segment ORDER BY revenue DESC""",
    }


QUERY_SETS = {
    "trino": lambda: _queries("memory.hw09", lambda d: f"DATE '{d}'"),
    "starrocks": lambda: _queries("hw09", lambda d: f"DATE('{d}')"),
    "clickhouse": lambda: _queries("hw09", lambda d: f"toDate('{d}')"),
}


def measure(engine: str) -> dict[str, list[float]]:
    run = bh.RUNNERS[engine]
    results: dict[str, list[float]] = {}
    for name, sql in QUERY_SETS[engine]().items():
        run(sql)  # warm-up, not measured
        results[name] = [round(bh.time_action(lambda: run(sql))[1], 4) for _ in range(MEASURE_RUNS)]
    return results


# --------------------------------------------------------------------------- #
SETUP = {"trino": setup_trino, "starrocks": setup_starrocks, "clickhouse": setup_clickhouse}
START = {"trino": start_trino, "starrocks": start_starrocks, "clickhouse": start_clickhouse}
QUERY_NAMES = ["q1_daily_agg", "q2_filter_region", "q3_join_top10", "q4_distinct_segment"]


def avg(values: list[float]) -> float:
    return round(sum(values) / len(values), 4)


def run_engine(engine: str, data) -> EngineResult:
    print(f"\n########## {engine.upper()} ##########")
    bh.docker_pull(IMAGES[engine])
    START[engine]()
    res = SETUP[engine](data)
    res.query_times_s = measure(engine)
    print(f"[{res.engine}] version={res.version} load={res.load_time_s:.2f}s")
    for name in QUERY_NAMES:
        print(f"  {name}: avg {avg(res.query_times_s[name]):.4f}s -> {res.query_times_s[name]}")
    return res


def write_results(results: list[EngineResult], fact_rows: int, path: Path) -> None:
    payload = {
        "dataset": {"fact_rows": fact_rows, "customer_rows": CUSTOMER_ROWS,
                    "product_rows": PRODUCT_ROWS},
        "engines": [
            {"engine": r.engine, "version": r.version, "load_time_s": round(r.load_time_s, 3),
             "query_times_s": r.query_times_s,
             "query_avg_s": {k: avg(v) for k, v in r.query_times_s.items()}}
            for r in results
        ],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[done] results saved to {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="HW09 Trino vs StarRocks vs ClickHouse micro-bench")
    parser.add_argument("--engine", default="all", choices=["all", *IMAGES.keys()])
    parser.add_argument("--fact-rows", type=int, default=FACT_ROWS)
    parser.add_argument("--json", default=str(RESULTS_PATH))
    args = parser.parse_args()

    data = generate_data(args.fact_rows)
    print(f"[data] fact={len(data['sales'])} customers={len(data['customers'])} "
          f"products={len(data['products'])}")

    engines = list(IMAGES) if args.engine == "all" else [args.engine]
    results = [run_engine(e, data) for e in engines]
    write_results(results, args.fact_rows, Path(args.json))


if __name__ == "__main__":
    main()
