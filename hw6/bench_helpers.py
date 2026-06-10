"""Вспомогательные функции для бенчмарков DataFrame vs RDD."""
import time

from pyspark.sql import SparkSession


def mk_spark(app="hw06", shuffle_partitions=8):
    """Создаёт локальную SparkSession.

    shuffle_partitions намеренно небольшой (8 = число ядер),
    чтобы на синтетике с 1000 ключей не плодить пустые партиции.
    """
    return (
        SparkSession.builder
        .appName(app)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def time_action(fun, n_warm=1, n_runs=3):
    """Измеряет время выполнения fun: один прогрев + n_runs замеров.

    Возвращает (среднее, список_замеров).
    """
    for _ in range(n_warm):
        fun()
    times = []
    for _ in range(n_runs):
        t0 = time.perf_counter()
        fun()
        times.append(time.perf_counter() - t0)
    return sum(times) / len(times), times
