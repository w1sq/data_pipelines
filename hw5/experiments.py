"""
Data Pipelines | HW 05 — эксперименты с оптимизацией Spark-приложений.

Скрипт генерирует синтетические данные, для каждого кейса:
  1. строит проблемный (baseline) сценарий,
  2. применяет оптимизацию,
  3. замеряет wall-clock и метрики стадий (shuffle / spill / executorRunTime).

Базовые кейсы (как в эталоне hw05):
  - broadcast    : broadcast join vs shuffle join
  - skew         : skew & salting (горячий ключ в агрегации)
  - repartition  : выбор числа партиций, repartition vs coalesce

Доп. кейсы (звёздочка — "решить проблему, не описанную выше"):
  - udf          : Python UDF vs нативные функции Spark SQL
  - cache        : переиспользование ветки cache() vs пересчёт DAG
  - aqe          : Adaptive Query Execution против skew без ручного salting
  - pushdown     : column pruning + predicate pushdown на Parquet

Запуск:
    python experiments.py --experiment all
    python experiments.py --experiment udf
"""

from __future__ import annotations

import argparse
import json
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.functions import broadcast
    from pyspark.sql.types import IntegerType, LongType, StringType
except Exception:
    print("Не найден pyspark. Установите зависимости: python -m pip install -r requirements.txt")
    raise

try:
    import requests
except Exception:
    requests = None  # метрики стадий просто не соберутся, но эксперименты отработают


# ----------------------------------------------------------------------------
# SPARK SESSION
# ----------------------------------------------------------------------------
def build_spark(app_name: str, extra_conf: Optional[Dict[str, str]] = None) -> SparkSession:
    """Создаёт локальную SparkSession. extra_conf позволяет отдельным кейсам
    менять конфигурацию (например, включать/выключать AQE)."""
    builder = SparkSession.builder.master("local[*]").appName(app_name)
    base_conf = {
        "spark.ui.showConsoleProgress": "false",
        "spark.sql.shuffle.partitions": "200",
        # По умолчанию выключаем AQE, чтобы baseline-кейсы (skew, repartition)
        # демонстрировали "сырое" поведение Spark. Отдельный эксперимент включает его сам.
        "spark.sql.adaptive.enabled": "false",
    }
    if extra_conf:
        base_conf.update(extra_conf)
    for k, v in base_conf.items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ----------------------------------------------------------------------------
# Метрики стадий (delta-based, чтобы не суммировать предыдущие стадии)
# ----------------------------------------------------------------------------
_METRIC_KEYS = [
    "shuffleReadBytes",
    "shuffleWriteBytes",
    "memoryBytesSpilled",
    "diskBytesSpilled",
    "executorRunTime",
    "inputBytes",
]


def _ui_url(spark: SparkSession) -> Optional[str]:
    # uiWebUrl() часто возвращает LAN-IP (например http://172.x.x.x:4040), к которому
    # соединение сбрасывается. Берём только порт и обращаемся к localhost.
    port = 4040
    try:
        url = spark.sparkContext._jsc.sc().uiWebUrl().get()
        if url and ":" in url:
            port = int(url.rsplit(":", 1)[1])
    except Exception:
        pass
    return f"http://localhost:{port}"


def _fetch_stages(spark: SparkSession) -> List[Dict[str, Any]]:
    if requests is None:
        return []
    url = _ui_url(spark)
    if not url:
        return []
    try:
        app_id = spark.sparkContext.applicationId
        resp = requests.get(f"{url.rstrip('/')}/api/v1/applications/{app_id}/stages", timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return []


class StageMetrics:
    """Снимает метрики стадий по дельте: считает только новые (не виденные ранее)
    стадии. Это устраняет ключевую проблему наивного подхода, при котором REST
    суммирует все стадии за всё время приложения."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._seen_before: set = set()
        self.result: Dict[str, Any] = {}

    def __enter__(self) -> "StageMetrics":
        self._seen_before = {s.get("stageId") for s in _fetch_stages(self.spark)}
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        stages = _fetch_stages(self.spark)
        summary = {k: 0 for k in _METRIC_KEYS}
        summary["numTasks"] = 0
        for s in stages:
            if s.get("stageId") in self._seen_before:
                continue
            tm = s.get("taskMetrics") or {}
            for key in _METRIC_KEYS:
                val = s.get(key, tm.get(key, 0))
                if isinstance(val, (int, float)):
                    summary[key] += val
            summary["numTasks"] += s.get("numTasks", 0) or 0
        self.result = summary
        return False


def timed(action: Callable[[], Any]) -> Tuple[Any, float]:
    t0 = time.perf_counter()
    res = action()
    return res, time.perf_counter() - t0


def measure(spark: SparkSession, label: str, action: Callable[[], Any]) -> Dict[str, Any]:
    """Выполняет action, замеряет и возвращает time_s + метрики стадий."""
    with StageMetrics(spark) as sm:
        _, dt = timed(action)
    metrics = sm.result
    print(f"  [{label}] time={dt:.3f}s  metrics={metrics}")
    return {"time_s": round(dt, 4), "metrics": metrics}


def speedup_line(baseline: float, optimized: float) -> str:
    if not optimized:
        return "n/a"
    ratio = baseline / optimized
    saved = (1 - optimized / baseline) * 100 if baseline else 0
    return f"speedup x{ratio:.2f}  (-{saved:.0f}% wall-clock)"


# ----------------------------------------------------------------------------
# Генерация данных
# ----------------------------------------------------------------------------
def gen_orders_countries(spark: SparkSession, n_orders: int, n_countries: int) -> Tuple[DataFrame, DataFrame]:
    orders = (
        spark.range(0, n_orders)
        .withColumnRenamed("id", "order_id")
        .withColumn("country_id", (F.col("order_id") % n_countries).cast(IntegerType()))
        .withColumn("amount", (F.rand(seed=7) * 10000).cast(IntegerType()))
        .withColumn("ts", (F.col("order_id") % 1_000_000).cast(LongType()))
    )
    countries = (
        spark.range(0, n_countries)
        .withColumnRenamed("id", "country_id")
        .withColumn("country_name", F.concat(F.lit("country_"), F.col("country_id").cast(StringType())))
    )
    return orders, countries


def gen_skewed(spark: SparkSession, n_rows: int, skew_fraction: float) -> DataFrame:
    df = (
        spark.range(0, n_rows)
        .withColumnRenamed("id", "row_id")
        .withColumn("rand_mod", (F.col("row_id") % 10000).cast(IntegerType()))
    )
    threshold = int(skew_fraction * 10000)
    df = df.withColumn(
        "key",
        F.when(F.col("rand_mod") < threshold, F.lit("HOT_KEY")).otherwise(
            F.concat(F.lit("K_"), (F.col("rand_mod") % 1000).cast(StringType()))
        ),
    )
    df = df.withColumn("value", (F.col("row_id") % 1000).cast(IntegerType())).drop("rand_mod")
    return df


# ----------------------------------------------------------------------------
# Базовые эксперименты
# ----------------------------------------------------------------------------
def exp_broadcast(spark: SparkSession, n_orders: int = 4_000_000, n_countries: int = 200) -> Dict[str, Any]:
    print("\n=== [1] Broadcast join vs Shuffle join ===")
    print("Проблема: join большой таблицы с маленьким справочником вызывает лишний shuffle большой таблицы.")
    orders, countries = gen_orders_countries(spark, n_orders, n_countries)
    print(f"orders={n_orders:,}  countries={n_countries}")

    res = {}
    res["baseline_shuffle"] = measure(
        spark, "shuffle join",
        lambda: orders.join(countries, on="country_id").agg(F.count("*").alias("c")).collect(),
    )
    res["optimized_broadcast"] = measure(
        spark, "broadcast join",
        lambda: orders.join(broadcast(countries), on="country_id").agg(F.count("*").alias("c")).collect(),
    )
    print("  ->", speedup_line(res["baseline_shuffle"]["time_s"], res["optimized_broadcast"]["time_s"]))
    return res


def exp_skew(spark: SparkSession, n_rows: int = 6_000_000, skew_fraction: float = 0.9, salts: int = 16) -> Dict[str, Any]:
    print("\n=== [2] Skew & Salting ===")
    print("Проблема: один 'горячий' ключ содержит 90% строк -> один task-straggler тормозит весь job.")
    df = gen_skewed(spark, n_rows, skew_fraction)
    print(f"rows={n_rows:,}  skew_fraction={skew_fraction}  salts={salts}")

    res = {}
    res["baseline"] = measure(
        spark, "groupBy (skewed)",
        lambda: df.groupBy("key").agg(F.count("*").alias("c")).count(),
    )

    def salted():
        salted_df = (
            df.withColumn("salt", (F.hash(F.col("row_id")) % salts).cast(IntegerType()))
            .withColumn("skey", F.concat(F.col("key"), F.lit("_"), F.col("salt").cast(StringType())))
        )
        partial = salted_df.groupBy("skey", "key").agg(F.count("*").alias("p"))
        return partial.groupBy("key").agg(F.sum("p").alias("c")).count()

    res["optimized_salted"] = measure(spark, "salting", salted)
    print("  ->", speedup_line(res["baseline"]["time_s"], res["optimized_salted"]["time_s"]))
    return res


def exp_repartition(spark: SparkSession, n_rows: int = 5_000_000, group_count: int = 100) -> Dict[str, Any]:
    print("\n=== [3] repartition / coalesce: выбор числа партиций ===")
    print("Проблема: слишком мало партиций -> простой CPU; слишком много -> overhead планирования и мелкие shuffle-файлы.")
    df = (
        spark.range(n_rows)
        .withColumnRenamed("id", "row_id")
        .withColumn("group_id", (F.col("row_id") % group_count).cast(IntegerType()))
        .withColumn("value", F.rand(seed=42))
    )
    df.limit(1000).count()  # warm-up

    def job(d: DataFrame):
        return d.groupBy("group_id").agg(F.sum("value").alias("t")).count()

    base_parts = df.rdd.getNumPartitions()
    res: Dict[str, Any] = {"baseline": measure(spark, f"baseline ({base_parts} parts)", lambda: job(df))}
    res["baseline"]["num_partitions"] = base_parts

    res["repartition"] = {}
    for p in [2, 8, 16, 50, 200, 500, 1000]:
        r = measure(spark, f"repartition({p})", lambda p=p: job(df.repartition(p)))
        r["num_partitions"] = p
        res["repartition"][str(p)] = r

    res["coalesce"] = {}
    for p in [p for p in [1, 2, 4] if p < base_parts] or [1, 2]:
        r = measure(spark, f"coalesce({p})", lambda p=p: job(df.coalesce(p)))
        r["num_partitions"] = p
        res["coalesce"][str(p)] = r
    return res


# ----------------------------------------------------------------------------
# Доп. эксперименты (*"решить проблему, не описанную выше"*)
# ----------------------------------------------------------------------------
def exp_udf(spark: SparkSession, n_rows: int = 8_000_000) -> Dict[str, Any]:
    print("\n=== [4*] Python UDF vs нативные функции Spark SQL ===")
    print("Проблема: Python UDF гоняет данные через сериализацию JVM<->Python и непрозрачна для Catalyst.")
    df = spark.range(n_rows).withColumnRenamed("id", "x").withColumn("s", F.col("x").cast(StringType()))

    from pyspark.sql.functions import udf

    @udf(returnType=StringType())
    def categorize_udf(x: int) -> str:
        if x % 15 == 0:
            return "fizzbuzz"
        if x % 3 == 0:
            return "fizz"
        if x % 5 == 0:
            return "buzz"
        return "num"

    res = {}
    res["baseline_python_udf"] = measure(
        spark, "python udf",
        lambda: df.withColumn("cat", categorize_udf(F.col("x"))).groupBy("cat").count().collect(),
    )

    cat_native = (
        F.when(F.col("x") % 15 == 0, F.lit("fizzbuzz"))
        .when(F.col("x") % 3 == 0, F.lit("fizz"))
        .when(F.col("x") % 5 == 0, F.lit("buzz"))
        .otherwise(F.lit("num"))
    )
    res["optimized_native"] = measure(
        spark, "native when/otherwise",
        lambda: df.withColumn("cat", cat_native).groupBy("cat").count().collect(),
    )
    print("  ->", speedup_line(res["baseline_python_udf"]["time_s"], res["optimized_native"]["time_s"]))
    return res


def exp_cache(spark: SparkSession, n_rows: int = 4_000_000) -> Dict[str, Any]:
    print("\n=== [5*] cache() vs пересчёт DAG при повторном использовании ===")
    print("Проблема: дорогой DataFrame, используемый в нескольких action, пересчитывается с нуля каждый раз.")

    def expensive_base() -> DataFrame:
        # имитируем дорогую цепочку преобразований
        d = spark.range(n_rows).withColumnRenamed("id", "x")
        for i in range(1, 6):
            d = d.withColumn(f"f{i}", F.sqrt(F.abs(F.sin(F.col("x") + i)) + 1))
        return d.withColumn("bucket", (F.col("x") % 50).cast(IntegerType()))

    def without_cache():
        d = expensive_base()
        a = d.groupBy("bucket").agg(F.avg("f1").alias("m")).count()
        b = d.groupBy("bucket").agg(F.sum("f2").alias("s")).count()
        c = d.filter(F.col("f3") > 1.0).count()
        return a + b + c

    def with_cache():
        d = expensive_base().cache()
        d.count()  # материализуем кэш один раз
        a = d.groupBy("bucket").agg(F.avg("f1").alias("m")).count()
        b = d.groupBy("bucket").agg(F.sum("f2").alias("s")).count()
        c = d.filter(F.col("f3") > 1.0).count()
        d.unpersist()
        return a + b + c

    res = {}
    res["baseline_recompute"] = measure(spark, "no cache (3x recompute)", without_cache)
    res["optimized_cache"] = measure(spark, "cache + reuse", with_cache)
    print("  ->", speedup_line(res["baseline_recompute"]["time_s"], res["optimized_cache"]["time_s"]))
    return res


def exp_aqe(_spark_unused: Optional[SparkSession], n_rows: int = 6_000_000, skew_fraction: float = 0.9) -> Dict[str, Any]:
    print("\n=== [6*] Adaptive Query Execution против skew без ручного salting ===")
    print("Проблема: skew join обычно лечат руками (salting). AQE умеет дробить перекошенные партиции автоматически.")
    res = {}

    big_n, dim_n = n_rows, 5000

    def skew_join(spark: SparkSession):
        big = gen_skewed(spark, big_n, skew_fraction).select("key", "value")
        dim = (
            spark.range(0, dim_n)
            .withColumn("key", F.concat(F.lit("K_"), (F.col("id") % 1000).cast(StringType())))
            .select("key", F.col("id").alias("dim_val"))
            .union(spark.createDataFrame([("HOT_KEY", -1)], ["key", "dim_val"]))
        )
        return big.join(dim, on="key").agg(F.count("*").alias("c")).collect()

    # AQE OFF
    spark_off = build_spark("hw05_aqe_off", {"spark.sql.adaptive.enabled": "false"})
    res["baseline_aqe_off"] = measure(spark_off, "AQE off", lambda: skew_join(spark_off))
    spark_off.stop()

    # AQE ON + skewJoin
    spark_on = build_spark(
        "hw05_aqe_on",
        {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            # понижаем пороги, чтобы skewJoin сработал на маленьких данных
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "1m",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "4m",
            "spark.sql.autoBroadcastJoinThreshold": "-1",  # форсим sort-merge join
        },
    )
    res["optimized_aqe_on"] = measure(spark_on, "AQE on (skewJoin)", lambda: skew_join(spark_on))
    spark_on.stop()

    print("  ->", speedup_line(res["baseline_aqe_off"]["time_s"], res["optimized_aqe_on"]["time_s"]))
    return res


def exp_pushdown(spark: SparkSession, n_rows: int = 8_000_000, tmp_dir: str = "/tmp/hw05_parquet") -> Dict[str, Any]:
    print("\n=== [7*] Column pruning + predicate pushdown на Parquet ===")
    print("Проблема: чтение всех колонок и фильтрация в Spark вместо того, чтобы отдать работу формату.")
    df = (
        spark.range(n_rows)
        .withColumnRenamed("id", "x")
        .withColumn("country_id", (F.col("x") % 200).cast(IntegerType()))
        .withColumn("amount", (F.rand(seed=1) * 1000).cast(IntegerType()))
        .withColumn("payload", F.sha2(F.col("x").cast(StringType()), 256))  # тяжёлая "широкая" колонка
    )
    df.write.mode("overwrite").parquet(tmp_dir)
    print(f"Parquet записан в {tmp_dir}")

    res = {}

    def naive():
        d = spark.read.parquet(tmp_dir)
        # читаем все колонки (включая тяжёлый payload) и фильтруем уже в Spark
        return d.select("*").filter(F.col("country_id") == 7).agg(F.sum("amount").alias("s")).collect()

    def optimized():
        d = spark.read.parquet(tmp_dir)
        # берём только нужные колонки + ранний фильтр -> column pruning + predicate pushdown
        return d.select("country_id", "amount").filter(F.col("country_id") == 7).agg(F.sum("amount").alias("s")).collect()

    res["baseline_full_read"] = measure(spark, "read all cols", naive)
    res["optimized_pruned"] = measure(spark, "pruned + pushdown", optimized)
    print("  ->", speedup_line(res["baseline_full_read"]["time_s"], res["optimized_pruned"]["time_s"]))
    return res


# ----------------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------------
# Эксперименты, которым нужна общая сессия (AQE сам создаёт свои сессии)
_SHARED_SESSION_EXPERIMENTS = {
    "broadcast": exp_broadcast,
    "skew": exp_skew,
    "repartition": exp_repartition,
    "udf": exp_udf,
    "cache": exp_cache,
    "pushdown": exp_pushdown,
}
_OWN_SESSION_EXPERIMENTS = {
    "aqe": exp_aqe,  # создаёт собственные сессии с разной настройкой AQE
}
ALL_EXPERIMENTS = list(_SHARED_SESSION_EXPERIMENTS) + list(_OWN_SESSION_EXPERIMENTS)


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Spark optimization experiments (HW05).")
    parser.add_argument("--experiment", choices=ALL_EXPERIMENTS + ["all"], default="all")
    parser.add_argument("--out", default="results.json")
    args = parser.parse_args(argv)

    chosen = ALL_EXPERIMENTS if args.experiment == "all" else [args.experiment]
    summary: Dict[str, Any] = {}

    shared = [e for e in chosen if e in _SHARED_SESSION_EXPERIMENTS]
    if shared:
        spark = build_spark("hw05_experiments")
        try:
            for name in shared:
                summary[name] = _SHARED_SESSION_EXPERIMENTS[name](spark)
        finally:
            spark.stop()

    for name in [e for e in chosen if e in _OWN_SESSION_EXPERIMENTS]:
        summary[name] = _OWN_SESSION_EXPERIMENTS[name](None)

    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)
    print(f"\nРезультаты записаны в {args.out}")


if __name__ == "__main__":
    main()
