"""HW07 - comparison of Lakehouse table formats: Delta Lake, Apache Iceberg, Apache Hudi.

What we measure (per the assignment):
  1. Concurrent writes: which problems arise and how each format handles them.
  2. Performance: on-disk size, read speed, write/update speed at different
     volumes (10%, 20%, 50%, 100%).

Usage:
    python experiments.py --format all
    python experiments.py --format delta              # single format
    python experiments.py --format iceberg --rows 1000000 --json results.json

Each format runs in its own SparkSession (the formats use incompatible
extensions/catalog), so --format all starts Spark three times sequentially.
"""
import argparse
import concurrent.futures
import json
import os
import shutil
import subprocess
import sys
import time

from pyspark.sql import functions as F

import bench_helpers as bh

ROOT = "/tmp/lakehouse_hw07"
UPDATE_PCTS = [10, 20, 50]


def _gen_df(spark, rows):
    """Synthetic dataset: id + amount + updated_at + partition_key."""
    return spark.range(0, rows).select(
        F.col("id"),
        (F.rand() * 1000).cast("int").alias("amount"),
        F.current_timestamp().alias("updated_at"),
        (F.col("id") % 10).cast("string").alias("partition_key"),
    )


def _print_metrics(name, m):
    print(f"\n=== {name}: metrics ===")
    print(f"  write_100%  : {m['write_100']:.2f} s")
    print(f"  size        : {m['size_mb']:.2f} MB")
    print(f"  read        : {m['read']:.2f} s")
    for pct in UPDATE_PCTS:
        print(f"  update_{pct:>3}% : {m['updates'][str(pct)]:.2f} s")
    print(f"  concurrency : {m['concurrency']}")


# --------------------------------------------------------------------------- #
# Delta Lake
# --------------------------------------------------------------------------- #
def run_delta(rows):
    table_path = f"{ROOT}/delta_table"
    shutil.rmtree(table_path, ignore_errors=True)
    spark = bh.mk_delta()
    spark.sparkContext.setLogLevel("ERROR")
    print("\n########## DELTA LAKE ##########")

    df = _gen_df(spark, rows)
    _, t_write = bh.time_action(
        lambda: df.write.format("delta").mode("overwrite").save(table_path))

    size_mb = bh.get_size_mb(table_path)

    _, t_read = bh.time_action(
        lambda: spark.read.format("delta").load(table_path)
        .agg(F.sum("amount")).collect())

    spark.read.format("delta").load(table_path).createOrReplaceTempView("target_table")
    updates = {}
    for pct in UPDATE_PCTS:
        base = spark.read.format("delta").load(table_path)
        src = (base.sample(fraction=pct / 100.0)
               .selectExpr("id", "amount + 100 as amount",
                           "current_timestamp() as updated_at", "partition_key"))
        src.createOrReplaceTempView("source_table")
        _, t = bh.time_action(lambda: spark.sql("""
            MERGE INTO target_table t USING source_table s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET t.amount = s.amount, t.updated_at = s.updated_at
        """))
        updates[str(pct)] = t

    def one_update(thread_id):
        spark.sql(f"UPDATE target_table SET amount = amount + {thread_id} WHERE id < 1000")

    conc = _run_concurrent(one_update)

    metrics = {"write_100": t_write, "size_mb": size_mb, "read": t_read,
               "updates": updates, "concurrency": conc}
    _print_metrics("Delta", metrics)
    spark.stop()
    return metrics


# --------------------------------------------------------------------------- #
# Apache Iceberg
# --------------------------------------------------------------------------- #
def run_iceberg(rows):
    warehouse = f"{ROOT}/iceberg_wh"
    shutil.rmtree(warehouse, ignore_errors=True)
    spark = bh.mk_iceberg(warehouse=warehouse)
    spark.sparkContext.setLogLevel("ERROR")
    print("\n########## APACHE ICEBERG ##########")

    df = _gen_df(spark, rows)
    _, t_write = bh.time_action(
        lambda: df.sortWithinPartitions("id")
        .writeTo("local.db.iceberg_table")
        .tableProperty("format-version", "2").createOrReplace())

    table_dir = f"{warehouse}/db/iceberg_table"
    size_mb = bh.get_size_mb(table_dir)

    _, t_read = bh.time_action(
        lambda: spark.read.table("local.db.iceberg_table").agg(F.sum("amount")).collect())

    updates = {}
    for pct in UPDATE_PCTS:
        base = spark.read.table("local.db.iceberg_table")
        src = (base.sample(fraction=pct / 100.0)
               .selectExpr("id", "amount + 100 as amount",
                           "current_timestamp() as updated_at", "partition_key"))
        src.createOrReplaceTempView("source_table")
        _, t = bh.time_action(lambda: spark.sql("""
            MERGE INTO local.db.iceberg_table t USING source_table s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET t.amount = s.amount, t.updated_at = s.updated_at
        """))
        updates[str(pct)] = t

    def one_update(thread_id):
        spark.sql(f"UPDATE local.db.iceberg_table SET amount = amount + {thread_id} WHERE id < 1000")

    conc = _run_concurrent(one_update)

    metrics = {"write_100": t_write, "size_mb": size_mb, "read": t_read,
               "updates": updates, "concurrency": conc}
    _print_metrics("Iceberg", metrics)
    spark.stop()
    return metrics


# --------------------------------------------------------------------------- #
# Apache Hudi
# --------------------------------------------------------------------------- #
def run_hudi(rows):
    table_path = f"{ROOT}/hudi_table"
    shutil.rmtree(table_path, ignore_errors=True)
    spark = bh.mk_hudi()
    spark.sparkContext.setLogLevel("ERROR")
    print("\n########## APACHE HUDI ##########")

    opts = {
        "hoodie.table.name": "hudi_table",
        "hoodie.datasource.write.recordkey.field": "id",
        "hoodie.datasource.write.partitionpath.field": "partition_key",
        "hoodie.datasource.write.precombine.field": "updated_at",
        "hoodie.datasource.write.table.type": "MERGE_ON_READ",
        "hoodie.datasource.write.operation": "bulk_insert",
    }

    df = _gen_df(spark, rows)
    _, t_write = bh.time_action(
        lambda: df.write.format("hudi").options(**opts).mode("overwrite").save(table_path))

    size_mb = bh.get_size_mb(table_path)

    _, t_read = bh.time_action(
        lambda: spark.read.format("hudi").load(table_path).agg(F.sum("amount")).collect())

    upsert_opts = dict(opts, **{"hoodie.datasource.write.operation": "upsert"})
    updates = {}
    for pct in UPDATE_PCTS:
        base = spark.read.format("hudi").load(table_path)
        src = (base.sample(fraction=pct / 100.0)
               .selectExpr("id", "amount + 100 as amount",
                           "current_timestamp() as updated_at", "partition_key"))
        _, t = bh.time_action(
            lambda s=src: s.write.format("hudi").options(**upsert_opts)
            .mode("append").save(table_path))
        updates[str(pct)] = t

    conc_opts = dict(upsert_opts, **{
        "hoodie.write.concurrency.mode": "optimistic_concurrency_control",
        "hoodie.write.lock.provider":
            "org.apache.hudi.client.transaction.lock.InProcessLockProvider",
    })

    def one_update(thread_id):
        base = spark.read.format("hudi").load(table_path)
        upd = base.limit(1000).selectExpr(
            "id", f"amount + {thread_id} as amount",
            "current_timestamp() as updated_at", "partition_key")
        upd.write.format("hudi").options(**conc_opts).mode("append").save(table_path)

    conc = _run_concurrent(one_update, sleep_s=2.0)

    metrics = {"write_100": t_write, "size_mb": size_mb, "read": t_read,
               "updates": updates, "concurrency": conc}
    _print_metrics("Hudi", metrics)
    spark.stop()
    return metrics


# --------------------------------------------------------------------------- #
# Concurrent write test: two threads update the same rows (id < 1000).
# All three formats use Optimistic Concurrency Control -> conflict.
# We handle it with a client-side retry (bench_helpers.retry).
# --------------------------------------------------------------------------- #
def _run_concurrent(one_update, sleep_s=1.0):
    print("--- Concurrent Writes Test ---")
    results = []

    def worker(thread_id):
        ok, attempt, err = bh.retry(lambda: one_update(thread_id), sleep_s=sleep_s)
        msg = (f"Thread {thread_id} SUCCESS on attempt {attempt}" if ok
               else f"Thread {thread_id} FAILED: {type(err).__name__}")
        print(msg)
        return {"thread": thread_id, "ok": ok, "attempt": attempt}

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        futures = [ex.submit(worker, i) for i in range(2)]
        for fut in concurrent.futures.as_completed(futures):
            results.append(fut.result())
    return results


RUNNERS = {"delta": run_delta, "iceberg": run_iceberg, "hudi": run_hudi}


def main():
    parser = argparse.ArgumentParser(description="HW07 Delta vs Iceberg vs Hudi")
    parser.add_argument("--format", default="all",
                        choices=["all", *RUNNERS.keys()])
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--json", default=None, help="where to save metrics")
    args = parser.parse_args()

    if args.format == "all":
        # Each format needs its own JVM: spark.jars.packages is fixed at JVM
        # launch, so a single process cannot load Delta + Iceberg + Hudi jars
        # at once. Run each format in a fresh subprocess and merge the metrics.
        results = {}
        for fmt in RUNNERS:
            tmp = f"/tmp/hw07_{fmt}.json"
            cmd = [sys.executable, os.path.abspath(__file__),
                   "--format", fmt, "--rows", str(args.rows), "--json", tmp]
            t0 = time.perf_counter()
            subprocess.run(cmd, check=True)
            print(f"[{fmt}] full run took {time.perf_counter() - t0:.1f} s")
            with open(tmp) as f:
                results[fmt] = json.load(f)[fmt]
    else:
        results = {args.format: RUNNERS[args.format](args.rows)}

    if args.json:
        with open(args.json, "w") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"\nMetrics saved to {args.json}")


if __name__ == "__main__":
    main()
