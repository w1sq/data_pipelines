"""Helpers for benchmarking Lakehouse table formats (Delta / Iceberg / Hudi).

Jar bundles are pinned for Spark 3.5 / Scala 2.12 (see requirements.txt):
    Delta   io.delta:delta-spark_2.12:3.2.0
    Iceberg org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2
    Hudi    org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0
"""
import os
import time

from pyspark.sql import SparkSession

DELTA_PKG = "io.delta:delta-spark_2.12:3.2.0"
ICEBERG_PKG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"
HUDI_PKG = "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0"

DRIVER_MEM = "4g"


def get_size_mb(path):
    """Total size of a table directory in MB (recursive, ignoring symlinks)."""
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for name in filenames:
            fp = os.path.join(dirpath, name)
            if not os.path.islink(fp):
                total += os.path.getsize(fp)
    return total / (1024 * 1024)


def time_action(fun):
    """Measure wall-clock of a single fun() call. Returns (result, seconds)."""
    t0 = time.perf_counter()
    res = fun()
    return res, time.perf_counter() - t0


def _base_builder(app):
    return (
        SparkSession.builder
        .appName(app)
        .master("local[*]")
        .config("spark.driver.memory", DRIVER_MEM)
        .config("spark.ui.showConsoleProgress", "false")
        # force loopback so the local driver does not bind to a LAN/public IP
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
    )


def mk_delta(app="hw07-delta"):
    return (
        _base_builder(app)
        .config("spark.jars.packages", DELTA_PKG)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def mk_iceberg(app="hw07-iceberg", warehouse="/tmp/lakehouse_iceberg"):
    return (
        _base_builder(app)
        .config("spark.jars.packages", ICEBERG_PKG)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse)
        .getOrCreate()
    )


def mk_hudi(app="hw07-hudi"):
    return (
        _base_builder(app)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.jars.packages", HUDI_PKG)
        .config("spark.sql.extensions",
                "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .getOrCreate()
    )


def retry(fun, max_attempts=3, sleep_s=1.0):
    """Client-side retry for Optimistic Concurrency Control (OCC) conflicts.

    Returns (success: bool, attempt: int, error: Exception | None).
    Between attempts we sleep and let fun re-read a fresh table snapshot.
    """
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            fun()
            return True, attempt, None
        except Exception as e:  # noqa: BLE001 - intentionally catch any write conflict
            last_err = e
            time.sleep(sleep_s)
    return False, max_attempts, last_err
