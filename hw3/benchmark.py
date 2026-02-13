from __future__ import annotations

import argparse
import gzip
import io
import json
import os
import random
import time
from collections import Counter, deque
from concurrent.futures import ProcessPoolExecutor
from datetime import date, timedelta
from typing import Iterable, Iterator

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq

EVENT_TYPES = ["click", "view", "purchase", "signup", "scroll", "hover"]

# Единая схема (Parquet) с вложенной структурой metrics.
PARQUET_SCHEMA = pa.schema(
    [
        ("date", pa.string()),
        ("user_id", pa.int64()),
        ("event_type", pa.string()),
        ("url", pa.string()),
        ("user_agent", pa.string()),
        ("value", pa.float64()),
        (
            "metrics",
            pa.struct(
                [
                    ("clicks", pa.int64()),
                    ("impressions", pa.int64()),
                    ("revenue", pa.float64()),
                ]
            ),
        ),
    ]
)

# Та же схема в нотации Avro.
AVRO_SCHEMA = {
    "type": "record",
    "name": "Event",
    "fields": [
        {"name": "date", "type": "string"},
        {"name": "user_id", "type": "long"},
        {"name": "event_type", "type": "string"},
        {"name": "url", "type": "string"},
        {"name": "user_agent", "type": "string"},
        {"name": "value", "type": "double"},
        {
            "name": "metrics",
            "type": {
                "type": "record",
                "name": "Metrics",
                "fields": [
                    {"name": "clicks", "type": "long"},
                    {"name": "impressions", "type": "long"},
                    {"name": "revenue", "type": "double"},
                ],
            },
        },
    ],
}

_BASE_DATE = date.today() - timedelta(days=365)


def gen_batch(n: int) -> list[dict]:
    """Сгенерировать n случайных событий за последний год."""
    rnd = random.random
    randint = random.randint
    choice = random.choice
    out = []
    for _ in range(n):
        d = (_BASE_DATE + timedelta(days=randint(0, 364))).isoformat()
        out.append(
            {
                "date": d,
                "user_id": randint(1, 10_000_000),
                "event_type": choice(EVENT_TYPES),
                "url": f"https://example.com/page/{randint(0, 9999)}",
                "user_agent": f"PyBench/{randint(1, 3)}.{randint(0, 9)}",
                "value": rnd() * 100.0,
                "metrics": {
                    "clicks": randint(0, 9),
                    "impressions": 100 + randint(0, 999),
                    "revenue": rnd() * 10.0,
                },
            }
        )
    return out


def _gen_batch_task(seed: int, n: int) -> list[dict]:
    """Worker для пула процессов: своя инициализация RNG на каждый батч."""
    random.seed(seed)
    return gen_batch(n)


def iter_batches_gen(args) -> Iterator[list[dict]]:
    """Поток батчей для записи.

    Если задан пул процессов (args._executor), генерация распараллеливается
    по ядрам, но батчи отдаются строго по порядку, а количество батчей,
    одновременно держащихся в памяти, ограничено окном max_inflight —
    запись остаётся последовательной и память не растёт линейно.
    """
    executor: ProcessPoolExecutor | None = getattr(args, "_executor", None)
    if executor is None:
        for _ in range(args.batches):
            yield gen_batch(args.rows_per_batch)
        return

    max_inflight = max(1, args.workers)
    base_seed = random.randrange(1 << 30)
    inflight: deque = deque()
    submitted = 0
    completed = 0
    while completed < args.batches:
        while submitted < args.batches and len(inflight) < max_inflight:
            inflight.append(
                executor.submit(_gen_batch_task, base_seed + submitted, args.rows_per_batch)
            )
            submitted += 1
        yield inflight.popleft().result()
        completed += 1


def human_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    f = float(n)
    for u in units:
        if f < 1024 or u == units[-1]:
            return f"{f:.2f} {u}"
        f /= 1024
    return f"{n} B"


def file_size(path: str) -> int:
    try:
        return os.stat(path).st_size
    except OSError:
        return 0


# --------------------------------------------------------------------------- #
# PARQUET
# --------------------------------------------------------------------------- #
def write_parquet(path: str, args) -> None:
    print(f"Writing Parquet to {path} (comp={args.parquet_comp})")
    writer = pq.ParquetWriter(
        path,
        PARQUET_SCHEMA,
        compression=args.parquet_comp.lower(),
    )
    total = 0
    try:
        for batch in iter_batches_gen(args):
            table = pa.Table.from_pylist(batch, schema=PARQUET_SCHEMA)
            writer.write_table(table)
            total += len(batch)
    finally:
        writer.close()
    print(f"Parquet: wrote {total} rows")


def read_parquet(path: str, args) -> dict:
    res = {}
    pf = pq.ParquetFile(path)
    num = pf.metadata.num_rows
    print(f"Parquet reader: {num} rows")

    # FULL SCAN — читаем все колонки.
    start = time.perf_counter()
    rows = 0
    for batch in pf.iter_batches(batch_size=50_000):
        rows += batch.num_rows
    res["full_scan_s"] = time.perf_counter() - start
    print(f"Parquet full-scan rows={rows} time={res['full_scan_s']:.3f}s")

    # FILTER — колоночному формату нужна только колонка date.
    pf = pq.ParquetFile(path)
    start = time.perf_counter()
    found = 0
    for batch in pf.iter_batches(batch_size=50_000, columns=["date"]):
        col = batch.column(0)
        for v in col:
            if v.as_py() == args.filter_date:
                found += 1
    res["filter_s"] = time.perf_counter() - start
    res["found"] = found
    print(f"Parquet filter({args.filter_date}): found={found} time={res['filter_s']:.3f}s")

    # AGGREGATION — нужна только колонка event_type.
    pf = pq.ParquetFile(path)
    start = time.perf_counter()
    counts: Counter[str] = Counter()
    for batch in pf.iter_batches(batch_size=50_000, columns=["event_type"]):
        counts.update(batch.column(0).to_pylist())
    res["agg_s"] = time.perf_counter() - start
    res["counts"] = dict(counts)
    print(f"Parquet aggregation: time={res['agg_s']:.3f}s counts={dict(counts)}")
    return res


# --------------------------------------------------------------------------- #
# AVRO
# --------------------------------------------------------------------------- #
def write_avro(path: str, args) -> None:
    print(f"Writing Avro OCF to {path} (codec={args.avro_codec})")
    parsed = fastavro.parse_schema(AVRO_SCHEMA)
    total = 0
    with open(path, "wb") as f:
        # Инкрементальный OCF-писатель: пишем батчами, не держим весь набор в памяти.
        writer = fastavro.write.Writer(f, parsed, codec=args.avro_codec)
        for batch in iter_batches_gen(args):
            for rec in batch:
                writer.write(rec)
            total += len(batch)
        writer.flush()
    print(f"Avro: wrote {total} rows")


def _avro_records(path: str) -> Iterable[dict]:
    with open(path, "rb") as f:
        yield from fastavro.reader(f)


def read_avro(path: str, args) -> dict:
    res = {}

    # FULL SCAN
    start = time.perf_counter()
    rows = 0
    for _ in _avro_records(path):
        rows += 1
    res["full_scan_s"] = time.perf_counter() - start
    print(f"Avro full-scan rows={rows} time={res['full_scan_s']:.3f}s")

    # FILTER
    start = time.perf_counter()
    found = 0
    for rec in _avro_records(path):
        if rec["date"] == args.filter_date:
            found += 1
    res["filter_s"] = time.perf_counter() - start
    res["found"] = found
    print(f"Avro filter({args.filter_date}): found={found} time={res['filter_s']:.3f}s")

    # AGGREGATION
    start = time.perf_counter()
    counts: Counter[str] = Counter()
    for rec in _avro_records(path):
        counts[rec["event_type"]] += 1
    res["agg_s"] = time.perf_counter() - start
    res["counts"] = dict(counts)
    print(f"Avro aggregation: time={res['agg_s']:.3f}s counts={dict(counts)}")
    return res


# --------------------------------------------------------------------------- #
# JSON (NDJSON)
# --------------------------------------------------------------------------- #
def _json_open_write(path: str, gzipped: bool):
    if gzipped:
        return io.TextIOWrapper(gzip.open(path, "wb"), encoding="utf-8")
    return open(path, "w", encoding="utf-8")


def _json_open_read(path: str, gzipped: bool):
    if gzipped:
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8")
    return open(path, "r", encoding="utf-8")


def write_json(path: str, args) -> None:
    print(f"Writing JSON NDJSON to {path} (gzip={args.json_gzip})")
    total = 0
    dumps = json.dumps
    with _json_open_write(path, args.json_gzip) as w:
        for batch in iter_batches_gen(args):
            w.write("\n".join(dumps(e, separators=(",", ":")) for e in batch))
            w.write("\n")
            total += len(batch)
    print(f"JSON: wrote {total} rows")


def read_json(path: str, args) -> dict:
    res = {}
    loads = json.loads

    # FULL SCAN
    start = time.perf_counter()
    rows = 0
    with _json_open_read(path, args.json_gzip) as r:
        for line in r:
            loads(line)
            rows += 1
    res["full_scan_s"] = time.perf_counter() - start
    print(f"JSON full-scan rows={rows} time={res['full_scan_s']:.3f}s")

    # FILTER
    start = time.perf_counter()
    found = 0
    with _json_open_read(path, args.json_gzip) as r:
        for line in r:
            if loads(line)["date"] == args.filter_date:
                found += 1
    res["filter_s"] = time.perf_counter() - start
    res["found"] = found
    print(f"JSON filter({args.filter_date}): found={found} time={res['filter_s']:.3f}s")

    # AGGREGATION
    start = time.perf_counter()
    counts: Counter[str] = Counter()
    with _json_open_read(path, args.json_gzip) as r:
        for line in r:
            counts[loads(line)["event_type"]] += 1
    res["agg_s"] = time.perf_counter() - start
    res["counts"] = dict(counts)
    print(f"JSON aggregation: time={res['agg_s']:.3f}s counts={dict(counts)}")
    return res


# --------------------------------------------------------------------------- #
# Оркестрация
# --------------------------------------------------------------------------- #
FORMATS = {
    "parquet": ("data.parquet", write_parquet, read_parquet),
    "avro": ("data.avro", write_avro, read_avro),
    "json": ("data.ndjson", write_json, read_json),
}


def resolve_path(args, fmt: str) -> str:
    fname, _, _ = FORMATS[fmt]
    if fmt == "json" and args.json_gzip:
        fname += ".gz"
    return os.path.join(args.outdir, fname)


def selected_formats(args) -> list[str]:
    if args.format == "all":
        return ["parquet", "avro", "json"]
    return [args.format]


def main() -> None:
    p = argparse.ArgumentParser(description="Format comparison benchmark")
    p.add_argument("--mode", default="all", choices=["write", "read", "all"])
    p.add_argument("--format", default="all", choices=["parquet", "avro", "json", "all"])
    p.add_argument("--outdir", default="./out")
    p.add_argument("--batches", type=int, default=10)
    p.add_argument("--rows-per-batch", type=int, default=200_000)
    p.add_argument("--parquet-comp", default="SNAPPY", choices=["SNAPPY", "GZIP", "NONE"])
    p.add_argument("--avro-codec", default="snappy", choices=["snappy", "deflate", "null"])
    p.add_argument("--json-gzip", action="store_true")
    p.add_argument("--filter-date", default="")
    p.add_argument("--results", default="results.json", help="куда сохранить метрики")
    p.add_argument(
        "--workers",
        type=int,
        default=min(4, os.cpu_count() or 1),
        help="процессов для генерации данных (1 = последовательно)",
    )
    args = p.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    if not args.filter_date:
        args.filter_date = (date.today() - timedelta(days=random.randint(0, 364))).isoformat()
    # pyarrow называет отсутствие сжатия "none".
    if args.parquet_comp == "NONE":
        args.parquet_comp = "none"

    # Многопоточность PyArrow (C++) для чтения/декодирования Parquet.
    pa.set_cpu_count(max(1, args.workers))

    print(
        f"Mode={args.mode} format={args.format} outdir={args.outdir} "
        f"batches={args.batches} rows/batch={args.rows_per_batch} "
        f"parquetComp={args.parquet_comp} avroCodec={args.avro_codec} "
        f"jsonGzip={args.json_gzip} filterDate={args.filter_date} workers={args.workers}"
    )

    metrics: dict[str, dict] = {}
    total_rows = args.batches * args.rows_per_batch

    # Пул процессов для параллельной генерации батчей при записи.
    args._executor = None
    need_write = args.mode in ("write", "all")
    if need_write and args.workers > 1:
        args._executor = ProcessPoolExecutor(max_workers=args.workers)

    try:
        for fmt in selected_formats(args):
            path = resolve_path(args, fmt)
            _, writer, reader = FORMATS[fmt]
            metrics.setdefault(fmt, {})

            if args.mode in ("write", "all"):
                print(f"=== Write: {fmt} ===")
                start = time.perf_counter()
                writer(path, args)
                write_s = time.perf_counter() - start
                size = file_size(path)
                metrics[fmt]["write_s"] = write_s
                metrics[fmt]["size_bytes"] = size
                print(f"{fmt} written in {write_s:.3f}s; {human_bytes(size)} ({size} bytes)")

            if args.mode in ("read", "all"):
                print(f"=== Read: {fmt} ===")
                start = time.perf_counter()
                metrics[fmt].update(reader(path, args))
                metrics[fmt]["read_total_s"] = time.perf_counter() - start
                print(f"{fmt} read finished in {metrics[fmt]['read_total_s']:.3f}s")
    finally:
        if args._executor is not None:
            args._executor.shutdown(wait=True)

    metrics["_meta"] = {
        "total_rows": total_rows,
        "filter_date": args.filter_date,
        "parquet_comp": args.parquet_comp,
        "avro_codec": args.avro_codec,
        "json_gzip": args.json_gzip,
        "workers": args.workers,
    }
    print_summary(metrics)
    with open(args.results, "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    print(f"\nMetrics saved to {args.results}")


def print_summary(metrics: dict) -> None:
    order = [f for f in ("json", "avro", "parquet") if f in metrics]
    if not order:
        return
    print("\n==================== SUMMARY ====================")
    header = f"{'Format':<8} {'Size':>10} {'Write':>9} {'Full-scan':>10} {'Filter':>9} {'Aggreg.':>9} {'Read tot':>9}"
    print(header)
    for fmt in order:
        m = metrics[fmt]
        print(
            f"{fmt:<8} "
            f"{human_bytes(m.get('size_bytes', 0)):>10} "
            f"{m.get('write_s', 0):>8.2f}s "
            f"{m.get('full_scan_s', 0):>9.2f}s "
            f"{m.get('filter_s', 0):>8.2f}s "
            f"{m.get('agg_s', 0):>8.2f}s "
            f"{m.get('read_total_s', 0):>8.2f}s"
        )


if __name__ == "__main__":
    main()
