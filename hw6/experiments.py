"""HW06 - DataFrame vs RDD vs SQL в Apache Spark.

Три основных кейса, где DataFrame гарантированно быстрее RDD за счёт
оптимизаций Catalyst/Tungsten, плюс два SQL-кейса, где SQL выигрывает у
"наивного" DataFrame API за счёт hint'ов выбора стратегии join.

Запуск:
    python experiments.py --case all
    python experiments.py --case a            # один кейс
    python experiments.py --case all --json results.json

Каждый кейс печатает таблицу времени и план выполнения (explain extended).
"""
import argparse
import json
import time

from pyspark.sql import functions as F, Window

from bench_helpers import mk_spark, time_action

N = 5_000_000
KEYS = 1000


def explain_str(df):
    """Возвращает extended-план (parsed/analyzed/optimized/physical) как строку."""
    return df._jdf.queryExecution().toString()


def make_flat_df(spark):
    """Плоский датасет: key (1000 значений) + value."""
    return spark.range(0, N).select(
        (F.col("id") % KEYS).alias("key"),
        (F.col("id") % 1000).alias("value"),
    )


def make_nested_df(spark):
    """Датасет с вложенными типами: struct meta и array values."""
    return spark.range(0, N).select(
        (F.col("id") % KEYS).alias("key"),
        F.struct(
            (F.col("id") % 100).alias("subid"),
            (F.col("id") % 5).alias("flag"),
        ).alias("meta"),
        F.array(*((F.col("id") % 10 + i) for i in range(3))).alias("values"),
    )


def print_table(rows):
    print("| Метод     | Время, с |")
    print("|-----------|----------|")
    for name, t in rows:
        print(f"| {name:<9} | {t:.4f} |")


# Кейс A: множественные агрегации (sum/avg/min) в один проход vs 3 reduceByKey
def case_a(spark):
    df = make_flat_df(spark)

    df_aggs = df.groupBy("key").agg(
        F.sum("value").alias("sum"),
        F.avg("value").alias("avg"),
        F.min("value").alias("min"),
    )

    def df_action():
        df_aggs.count()

    rdd = df.rdd.map(lambda row: (row["key"], row["value"]))

    def rdd_action():
        rdd.reduceByKey(lambda a, b: a + b).count()
        (rdd.mapValues(lambda x: (x, 1))
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
            .mapValues(lambda sc: sc[0] / sc[1])
            .count())
        rdd.reduceByKey(lambda a, b: a if a < b else b).count()

    df_avg, _ = time_action(df_action)
    rdd_avg, _ = time_action(rdd_action)
    print_table([("DataFrame", df_avg), ("RDD", rdd_avg)])
    plan = explain_str(df_aggs)
    print(plan)
    return {"DataFrame": df_avg, "RDD": rdd_avg, "plan": plan}


# Кейс B: оконные функции - Top-N в группе vs groupByKey+sort на RDD
def case_b(spark):
    df = make_flat_df(spark)

    w = Window.partitionBy("key").orderBy(F.col("value").desc())
    df_topk = (df.withColumn("rn", F.row_number().over(w))
                 .filter(F.col("rn") < 5)
                 .select("key", "value"))

    def df_action():
        df_topk.count()

    rdd = df.rdd.map(lambda row: (row["key"], row["value"]))

    def rdd_action():
        (rdd.groupByKey()
            .mapValues(lambda x: sorted(x, reverse=True)[:4])
            .count())

    df_avg, _ = time_action(df_action)
    rdd_avg, _ = time_action(rdd_action)
    print_table([("DataFrame", df_avg), ("RDD", rdd_avg)])
    plan = explain_str(df_topk)
    print(plan)
    return {"DataFrame": df_avg, "RDD": rdd_avg, "plan": plan}


# Кейс C: вложенные типы - struct/array+explode vs ручной парсинг JSON на RDD
def case_c(spark):
    df = make_nested_df(spark)

    df2 = df.select("key", "meta.subid", F.explode("values").alias("value"))

    def df_action():
        df2.count()

    rdd = df.rdd.map(lambda row: json.dumps({
        "key": row["key"],
        "meta": {"subid": row["meta"]["subid"]},
        "values": list(row["values"]),
    }))

    def rdd_action():
        (rdd.map(lambda s: json.loads(s))
            .flatMap(lambda d: [(d["key"], d["meta"]["subid"], v) for v in d["values"]])
            .count())

    df_avg, _ = time_action(df_action)
    rdd_avg, _ = time_action(rdd_action)
    print_table([("DataFrame", df_avg), ("RDD", rdd_avg)])
    plan = explain_str(df2)
    print(plan)
    return {"DataFrame": df_avg, "RDD": rdd_avg, "plan": plan}


# SQL-кейс 1: BROADCAST hint vs наивный DataFrame join (broadcast выключен)
# Hint ссылается на алиас 's' (не на имя вью), иначе Spark 4 его игнорирует.
def case_sql_broadcast(spark):
    # autoBroadcast выключаем: без hint'а наивный DataFrame обязан делать SortMergeJoin
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    big = spark.range(0, N).select(
        (F.col("id") % 100000).alias("key"), F.col("id").alias("value"))
    small = spark.range(0, 1000).select(
        (F.col("id") % 100000).alias("key"), (F.col("id") % 10).alias("s"))
    big.createOrReplaceTempView("big")
    small.createOrReplaceTempView("small")

    sql_q = """
        SELECT /*+ BROADCAST(s) */ b.key, count(*) AS cnt
        FROM big b
        JOIN small s ON b.key = s.key
        GROUP BY b.key
    """
    df_sql = spark.sql(sql_q)
    df_naive = big.join(small, on="key").groupBy("key").count()

    sql_avg, _ = time_action(lambda: df_sql.count())
    naive_avg, _ = time_action(lambda: df_naive.count())

    print_table([("SQL+hint", sql_avg), ("DF naive", naive_avg)])
    sql_plan = explain_str(df_sql)
    naive_plan = explain_str(df_naive)
    print("=== SQL (BROADCAST hint) ===")
    print(sql_plan)
    print("=== DataFrame (naive) ===")
    print(naive_plan)
    spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
    return {"SQL+hint": sql_avg, "DF naive": naive_avg,
            "sql_plan": sql_plan, "naive_plan": naive_plan}


# SQL-кейс 2: SHUFFLE_HASH hint vs наивный DataFrame join (дефолтный SortMergeJoin)
# Обе таблицы слишком большие для broadcast; hint форсит ShuffledHashJoin без сортировки.
def case_sql_shuffle_hash(spark):
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    n_big = 20_000_000
    n_med = 2_000_000

    big = spark.range(0, n_big).select(
        (F.col("id") % n_med).alias("key"), F.col("id").alias("value"))
    med = spark.range(0, n_med).select(
        F.col("id").alias("key"), (F.col("id") % 7).alias("s"))
    big.createOrReplaceTempView("big2")
    med.createOrReplaceTempView("med")

    sql_q = """
        SELECT /*+ SHUFFLE_HASH(m) */ b.key, b.value, m.s
        FROM big2 b
        JOIN med m ON b.key = m.key
    """
    df_sql = spark.sql(sql_q)
    df_naive = big.join(med, on="key")

    sql_avg, _ = time_action(lambda: df_sql.count())
    naive_avg, _ = time_action(lambda: df_naive.count())

    print_table([("SQL+hint", sql_avg), ("DF naive", naive_avg)])
    sql_plan = explain_str(df_sql)
    naive_plan = explain_str(df_naive)
    print("=== SQL (SHUFFLE_HASH hint) ===")
    print(sql_plan)
    print("=== DataFrame (naive) ===")
    print(naive_plan)
    spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
    return {"SQL+hint": sql_avg, "DF naive": naive_avg,
            "sql_plan": sql_plan, "naive_plan": naive_plan}


CASES = {
    "a": ("Кейс A: множественные агрегации", case_a),
    "b": ("Кейс B: оконные функции (Top-N)", case_b),
    "c": ("Кейс C: вложенные типы (struct/array)", case_c),
    "sql_broadcast": ("SQL-кейс 1: BROADCAST join", case_sql_broadcast),
    "sql_shuffle_hash": ("SQL-кейс 2: SHUFFLE_HASH join", case_sql_shuffle_hash),
}


def main():
    parser = argparse.ArgumentParser(description="HW06 DataFrame vs RDD vs SQL")
    parser.add_argument(
        "--case", default="all",
        choices=["all", *CASES.keys()],
        help="какой кейс прогнать")
    parser.add_argument("--json", default=None, help="куда сохранить результаты")
    args = parser.parse_args()

    spark = mk_spark()
    spark.sparkContext.setLogLevel("ERROR")

    to_run = CASES.keys() if args.case == "all" else [args.case]
    results = {}
    for key in to_run:
        title, fn = CASES[key]
        print("\n" + "=" * 70)
        print(title)
        print("=" * 70)
        t0 = time.perf_counter()
        results[key] = fn(spark)
        print(f"\n[{key}] выполнено за {time.perf_counter() - t0:.1f} с")

    if args.json:
        # планы в json не пишем - они большие; сохраняем только времена
        slim = {k: {kk: vv for kk, vv in v.items() if not kk.endswith("plan")}
                for k, v in results.items()}
        with open(args.json, "w") as f:
            json.dump(slim, f, ensure_ascii=False, indent=2)
        print(f"\nРезультаты (времена) сохранены в {args.json}")

    spark.stop()


if __name__ == "__main__":
    main()
