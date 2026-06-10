"""HW09 - reproducible ClickBench metrics: StarRocks vs ClickHouse.

ClickBench (https://benchmark.clickhouse.com) publishes raw JSON results: for
every system and machine it stores load time, on-disk data size and 43 queries,
each run 3 times. We take the official c6a.4xlarge snapshots for ClickHouse and
StarRocks and compute a few honest, comparable numbers instead of eyeballing the
public leaderboard:

  cold run  = first execution of a query (cache miss);
  hot  run  = min(second, third execution) (cache warm);
  ratio     = StarRocks_time / ClickHouse_time  (>1 => ClickHouse is faster).

We aggregate per-query ratios with a geometric mean (the ClickBench-style metric:
robust to a single very slow/fast query) and fold load time and data size into a
single weighted "combined ratio". Queries where either engine reports null
(failed/unsupported) are skipped so we only compare what both engines ran.

Snapshots live in data/ so the script is fully offline/reproducible. Pass
--refresh to re-download the latest raw JSON from the ClickBench repo.

Usage:
    python clickbench_metrics.py
    python clickbench_metrics.py --refresh        # pull fresh raw JSON
    python clickbench_metrics.py --json out.json  # also dump the summary
"""
import argparse
import json
import math
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"

# Official ClickBench raw snapshots (same machine -> fair comparison).
SNAPSHOTS = {
    "clickhouse": {
        "file": "clickbench_clickhouse_c6a.4xlarge.json",
        "url": "https://raw.githubusercontent.com/ClickHouse/ClickBench/main/clickhouse/results/20260327/c6a.4xlarge.json",
    },
    "starrocks": {
        "file": "clickbench_starrocks_c6a.4xlarge.json",
        "url": "https://raw.githubusercontent.com/ClickHouse/ClickBench/main/starrocks/results/20251230/c6a.4xlarge.json",
    },
}

# combined_ratio weights (hot latency dominates an interactive analytics engine).
WEIGHTS = {"load": 0.1, "size": 0.1, "cold": 0.2, "hot": 0.6}


def load_snapshot(engine: str, refresh: bool) -> dict:
    spec = SNAPSHOTS[engine]
    path = DATA_DIR / spec["file"]
    if refresh or not path.exists():
        DATA_DIR.mkdir(exist_ok=True)
        req = urllib.request.Request(spec["url"], headers={"User-Agent": "hw09"})
        payload = json.load(urllib.request.urlopen(req, timeout=30))
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"[refresh] {engine}: saved {path.name}")
    return json.loads(path.read_text(encoding="utf-8"))


def gmean(values: list[float]) -> float:
    return math.exp(sum(math.log(v) for v in values) / len(values))


def cold_time(timings: list[float]) -> float:
    return timings[0]


def hot_time(timings: list[float]) -> float:
    return min(timings[1], timings[2])


def usable(timings) -> bool:
    """A query is comparable only if all 3 runs are real positive numbers."""
    return (
        isinstance(timings, list)
        and len(timings) >= 3
        and all(isinstance(t, (int, float)) and t > 0 for t in timings[:3])
    )


def build_summary(clickhouse: dict, starrocks: dict) -> dict:
    ch_results = clickhouse["result"]
    sr_results = starrocks["result"]
    if len(ch_results) != len(sr_results):
        raise ValueError("query count mismatch between snapshots")

    cold_ratios: list[float] = []
    hot_ratios: list[float] = []
    sr_hot_faster = 0
    sr_cold_faster = 0
    compared = 0

    for ch, sr in zip(ch_results, sr_results):
        if not (usable(ch) and usable(sr)):
            continue
        compared += 1
        ch_cold, sr_cold = cold_time(ch), cold_time(sr)
        ch_hot, sr_hot = hot_time(ch), hot_time(sr)
        cold_ratios.append(sr_cold / ch_cold)
        hot_ratios.append(sr_hot / ch_hot)
        sr_hot_faster += sr_hot < ch_hot
        sr_cold_faster += sr_cold < ch_cold

    cold_ratio = gmean(cold_ratios)
    hot_ratio = gmean(hot_ratios)
    load_ratio = starrocks["load_time"] / clickhouse["load_time"]
    size_ratio = starrocks["data_size"] / clickhouse["data_size"]
    combined_ratio = math.exp(
        WEIGHTS["load"] * math.log(load_ratio)
        + WEIGHTS["size"] * math.log(size_ratio)
        + WEIGHTS["cold"] * math.log(cold_ratio)
        + WEIGHTS["hot"] * math.log(hot_ratio)
    )

    return {
        "snapshots": {
            "clickhouse": {
                "date": clickhouse["date"],
                "machine": clickhouse["machine"],
                "load_time_s": clickhouse["load_time"],
                "data_size_bytes": clickhouse["data_size"],
                "queries": len(ch_results),
            },
            "starrocks": {
                "date": starrocks["date"],
                "machine": starrocks["machine"],
                "load_time_s": starrocks["load_time"],
                "data_size_bytes": starrocks["data_size"],
                "queries": len(sr_results),
            },
        },
        "compared_queries": compared,
        "ratios_starrocks_vs_clickhouse": {
            "load_ratio": round(load_ratio, 6),
            "size_ratio": round(size_ratio, 6),
            "cold_gmean_ratio": round(cold_ratio, 6),
            "hot_gmean_ratio": round(hot_ratio, 6),
            "combined_ratio": round(combined_ratio, 6),
        },
        "starrocks_wins": {
            "hot_queries_faster": sr_hot_faster,
            "cold_queries_faster": sr_cold_faster,
            "of_total": compared,
        },
        "method": {
            "cold_run": "first execution of each query",
            "hot_run": "min(second, third execution)",
            "ratio": "StarRocks_time / ClickHouse_time (>1 => ClickHouse faster)",
            "combined_weights": WEIGHTS,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="ClickBench StarRocks vs ClickHouse")
    parser.add_argument("--refresh", action="store_true", help="re-download raw JSON")
    parser.add_argument("--json", default=None, help="where to dump the summary")
    args = parser.parse_args()

    clickhouse = load_snapshot("clickhouse", args.refresh)
    starrocks = load_snapshot("starrocks", args.refresh)
    summary = build_summary(clickhouse, starrocks)
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    if args.json:
        Path(args.json).write_text(
            json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"\n[done] summary saved to {args.json}")


if __name__ == "__main__":
    main()
