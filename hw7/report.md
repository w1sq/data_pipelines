# Data Pipelines | HW 07 — сравнение форматов Lakehouse (Delta, Iceberg, Hudi)

## Задача

Провести эксперименты с тремя открытыми форматами таблиц:

1. **Одновременная запись** — какие проблемы возникают и как форматы их обрабатывают.
2. **Производительность** — вес на диске, скорость чтения, скорость записи/обновления (10%, 20%, 50%, 100%).
3. **Кейсы использования** — когда оправдано брать Hudi или Delta вместо Iceberg.

Код: `experiments.py` + `bench_helpers.py` (PySpark 3.5, `local[*]`).

## Окружение прогона

| Параметр | Значение |
|----------|----------|
| Spark / PySpark | 3.5.1 |
| Java | OpenJDK 17 (`brew install openjdk@17`) |
| Master | `local[*]` |
| Driver memory | 4 GB |
| Delta | `io.delta:delta-spark_2.12:3.2.0` |
| Iceberg | `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2` |
| Hudi | `org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0` |
| Датасет | 1 000 000 строк (`id`, `amount`, `updated_at`, `partition_key`) |

Числа ниже — **реальный прогон** на этой машине (`python experiments.py --format all --json results.json`).

### Запуск

```bash
cd hw7
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export SPARK_LOCAL_IP=127.0.0.1

python experiments.py --format all --json results.json   # все три формата
python experiments.py --format delta                     # один формат
```

> **Почему `--format all` запускает три subprocess'а?**  
> `spark.jars.packages` фиксируется при старте JVM. В одном процессе нельзя одновременно
> подгрузить jar'ы Delta, Iceberg и Hudi — второй и третий формат упадут с `ClassNotFoundException`.
> Оркестратор в `experiments.py` гоняет каждый формат в отдельном процессе и сливает метрики.

---

## 1. Одновременная запись (Concurrency)

### Проблема: Optimistic Concurrency Control (OCC)

Все три формата используют **оптимистичную блокировку**: транзакция читает снапшот таблицы,
готовит изменения и коммитит. Если за это время другой писатель изменил те же файлы —
коммит отклоняется.

| Формат | Типичная ошибка при конфликте |
|--------|-------------------------------|
| Delta Lake | `ConcurrentModificationException` |
| Iceberg | `ReplaceDataExec aborting` / `aborted` |
| Hudi | конфликт OCC (при `hoodie.write.concurrency.mode=optimistic_concurrency_control`) |

### Сценарий эксперимента

Два потока (`ThreadPoolExecutor`, `max_workers=2`) одновременно обновляют строки `id < 1000`.
В коде реализован **клиентский Retry** (`bench_helpers.retry`): при ошибке ждём 1–2 с,
перечитываем свежий снапшот и повторяем.

### Результаты

| Формат | Thread 0 | Thread 1 |
|--------|----------|----------|
| Delta | SUCCESS, attempt **2** | SUCCESS, attempt **1** |
| Iceberg | SUCCESS, attempt **2** | SUCCESS, attempt **1** |
| Hudi | SUCCESS, attempt **2** | SUCCESS, attempt **1** |

Один поток коммитит с первой попытки, второй перехватывает конфликт и успешно дожимает со
второй. Это стандартный production-паттерн для OCC.

**Как форматы обрабатывают конфликт:**

- **Delta** — отклоняет транзакцию, если изменился набор файлов, на которые ссылается commit.
- **Iceberg** — atomic swap файлов через метаданные; при гонке `ReplaceDataExec` откатывает запись.
- **Hudi** — OCC + `InProcessLockProvider` (для локального теста); в кластере — ZooKeeper / DynamoDB / Hive Metastore lock.

---

## 2. Производительность

### Сводная таблица

| Метрика | Delta | Iceberg | Hudi |
|---------|-------|---------|------|
| **Размер на диске** | 5.13 MB | **2.25 MB** | 41.53 MB |
| **Чтение (sum)** | 2.41 s | **0.52 s** | 1.09 s |
| **Запись 100%** | 4.31 s | **2.20 s** | 6.29 s |
| **Update 10%** | 2.83 s | **1.54 s** | 12.05 s |
| **Update 20%** | 2.33 s | **1.05 s** | 8.94 s |
| **Update 50%** | 2.07 s | **1.22 s** | 9.88 s |

### a. Кто меньше весит?

1. **Iceberg (2.25 MB)** — лучший результат. `sortWithinPartitions("id")` перед записью даёт
   плотное сжатие Parquet.
2. **Delta (5.13 MB)** — чуть больше из-за дефолтного партиционирования Spark и transaction log.
3. **Hudi (41.53 MB)** — самый тяжёлый: 5 системных колонок на каждую строку
   (`_hoodie_commit_time`, `_hoodie_record_key`, …) + директория `.hoodie` с индексами и
   timeline. На микро-датасете оверхед выглядит огромным, на терабайтах — пропорционально меньше.

### b. Кого быстрее читать?

1. **Iceberg (0.52 s)** — эффективные метаданные, нет merge-on-read.
2. **Hudi (1.09 s)** — MoR: при чтении нужно склеивать base-файлы с log-файлами.
3. **Delta (2.41 s)** — в этом прогоне медленнее ожидаемого; на маленьком локальном датасете
   разброс в 2-3x между форматами сильно зависит от JVM warmup и кэша ОС.

### c. Скорость записи и обновления

**Запись 100%:** Iceberg (2.20 s) > Delta (4.31 s) > Hudi (6.29 s).

**Обновления (MERGE / upsert):**

| % данных | Delta | Iceberg | Hudi |
|----------|-------|---------|------|
| 10% | 2.83 s | 1.54 s | 12.05 s |
| 20% | 2.33 s | 1.05 s | 8.94 s |
| 50% | 2.07 s | 1.22 s | 9.88 s |

- **Iceberg** — стабильно быстрый MERGE (~1–1.5 s).
- **Delta** — 2–3 s; время уменьшается от 10% к 50% (эффект JVM warmup: первый запрос
  тратит время на компиляцию плана и инициализацию каталога).
- **Hudi** — 9–12 s на все апдейты. MoR-архитектура рассчитана на кластеры: Write Client,
  Bloom-фильтры и индексы дают большой фиксированный overhead на таблице в ~5 MB.

---

## 3. Кейсы: когда Hudi или Delta вместо Iceberg

Локальный микро-тест выиграл Iceberg, но в production выбор зависит от архитектуры:

### Apache Hudi — стриминг CDC из высоконагруженных систем

**Сценарий:** Postgres -> Kafka -> Lakehouse, таблица 10+ TB, тысячи upsert'ов в секунду.

Iceberg/Delta в режиме Copy-on-Write перепишут гигабайты файлов при каждом MERGE.
Hudi найдёт строки по индексу и точечно допишет в log-файлы (MoR). Медленные 12 s на
локальном тесте — цена за индексы, которые окупаются на больших объёмах.

### Delta Lake — аналитика в экосистеме Databricks / Spark

**Сценарий:** дата-инженеры пишут Structured Streaming + batch в одну Delta-таблицу.

Delta даёт из коробки `Auto Optimize`, `OPTIMIZE ZORDER`, нативную интеграцию со Spark
и Databricks Unity Catalog. Меньше ручной настройки метаданных, чем у Iceberg.

### Iceberg — универсальный формат без vendor lock-in

Iceberg остаётся лучшим выбором, когда к одному хранилищу обращаются **разные движки**
(Spark, Trino, Flink, Snowflake) и нужен открытый vendor-neutral стандарт.

---

## Выводы

1. **Concurrency:** все форматы используют OCC; конфликты решаются клиентским Retry.
2. **Размер:** Iceberg < Delta << Hudi (на 1M строк).
3. **Скорость:** Iceberg лидирует в записи/чтении/апдейтах на локальном тесте; Hudi
   проигрывает из-за MoR-overhead на маленьких данных.
4. **Выбор формата** — не по микро-бенчмарку, а по паттерну нагрузки: CDC -> Hudi,
   Databricks/Spark -> Delta, мульти-движковый lakehouse -> Iceberg.
