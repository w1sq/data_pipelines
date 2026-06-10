# Data Pipelines | HW 06 - DataFrame vs RDD vs SQL

## Задача

Реализовать 3 кейса, где DataFrame гарантированно быстрее RDD, и 2 бонусных кейса, где SQL быстрее "наивного" DataFrame API. Для каждого кейса - код, замеры и объяснение оптимизаций Catalyst/Tungsten по плану `explain`.

Все эксперименты в одном скрипте `experiments.py` (PySpark 4.0, `local[*]`).

## Окружение прогона

| Параметр | Значение |
|----------|----------|
| Spark / PySpark | 4.0.2 |
| Java | OpenJDK 17 (Homebrew, `openjdk@17`) |
| Master | `local[*]` |
| `spark.sql.shuffle.partitions` | 8 |
| Датасет (кейсы A-C) | 5 000 000 строк, 1000 ключей |
| Датасет (SQL-кейс 2) | 20 000 000 + 2 000 000 строк |

Числа ниже - **реальный прогон** (`python experiments.py --case all`), сырые времена в `results.json`, полные планы в `run.log`.

## Сводная таблица

| Кейс | DataFrame / SQL | RDD / DF naive | Ускорение |
|------|-----------------|----------------|-----------|
| A: множественные агрегации | 0.09 с | 12.50 с | ~144x |
| B: Top-N в группе | 0.40 с | 4.09 с | ~10x |
| C: struct/array + explode | 0.07 с | 13.78 с | ~204x |
| SQL 1: BROADCAST join | 0.11 с | 0.38 с | ~3.4x |
| SQL 2: SHUFFLE_HASH join | 1.18 с | 2.00 с | ~1.7x |

---

## Кейс A: множественные агрегации

### Задача

Для 1000 ключей посчитать `sum`, `avg`, `min` по полю `value`.

### Код

**DataFrame:**
```python
df.groupBy("key").agg(
    F.sum("value").alias("sum"),
    F.avg("value").alias("avg"),
    F.min("value").alias("min"),
)
```

**RDD:**
```python
rdd.reduceByKey(lambda a, b: a + b).count()                    # sum
rdd.mapValues(lambda x: (x, 1)).reduceByKey(...).mapValues(...) # avg
rdd.reduceByKey(lambda a, b: a if a < b else b).count()         # min
```

### Результаты

| Метод | Время |
|-------|-------|
| DataFrame | 0.09 с |
| RDD | 12.50 с |

### Оптимизация Catalyst/Tungsten

В физическом плане DataFrame - один `HashAggregate` с тремя функциями и **один** shuffle:

```
HashAggregate(keys=[key], functions=[sum, avg, min])
  +- Exchange hashpartitioning(key, 8)
     +- HashAggregate(..., functions=[partial_sum, partial_avg, partial_min])
```

Catalyst объединяет три агрегата в **один проход** (partial + final aggregation). Tungsten генерирует JVM-байткод, обновляющий все три аккумулятора за один цикл.

RDD делает **три** отдельных `reduceByKey` = три shuffle + три прохода. Плюс Python-RDD сериализует данные через pickle на каждой границе JVM/Python.

**Итого:** DataFrame - 1 shuffle, 1 проход. RDD - 3 shuffle, 3 прохода + сериализация.

---

## Кейс B: оконные функции (Top-N в группе)

### Задача

Найти топ-4 записи по `value` в каждой группе `key`.

### Код

**DataFrame:**
```python
w = Window.partitionBy("key").orderBy(F.col("value").desc())
df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") < 5)
```

**RDD:**
```python
rdd.groupByKey().mapValues(lambda x: sorted(x, reverse=True)[:4]).count()
```

### Результаты

| Метод | Время |
|-------|-------|
| DataFrame | 0.40 с |
| RDD | 4.09 с |

### Оптимизация Catalyst/Tungsten

В Optimized Logical Plan появляется специальный узел:

```
WindowGroupLimit [key], [value DESC], row_number(), 4
```

Catalyst не сортирует все строки группы целиком. Сначала на каждом executor отбирается не более 4 строк (Partial), затем один shuffle, затем финальная фильтрация (Final). Это сокращает объём данных, пересылаемых по сети.

RDD через `groupByKey()` собирает **все** значения группы на один узел, сортирует весь список в Python и только потом берёт первые 4.

---

## Кейс C: вложенные типы (struct/array)

### Задача

Извлечь `meta.subid` и развернуть массив `values` в отдельные строки.

### Код

**DataFrame:**
```python
df.select("key", "meta.subid", F.explode("values").alias("value"))
```

**RDD:**
```python
rdd = df.rdd.map(lambda row: json.dumps({...}))
parsed = rdd.map(json.loads).flatMap(lambda d: [...]).count()
```

### Результаты

| Метод | Время |
|-------|-------|
| DataFrame | 0.07 с |
| RDD | 13.78 с |

### Оптимизация Catalyst/Tungsten

Catalyst применяет projection pushdown - доступ к `meta.subid` упрощается до прямого вычисления `(id % 100)` без промежуточного struct. Физический план - одна стадия без shuffle, whole-stage codegen:

```
*(1) Generate explode(values)
  +- *(1) Project [key, subid, values]
     +- *(1) Range
```

Префикс `*(1)` - весь пайплайн скомпилирован в один блок JVM-байткода. Данные в бинарном формате Tungsten.

RDD сериализует каждую строку в JSON (`json.dumps`) и обратно (`json.loads`) - на 5 млн строк это доминирующие накладные расходы.

---

## SQL-кейс 1: BROADCAST join (*)

### Задача

Join большой таблицы (5 млн) с маленькой (1000 строк). Сравнить SQL с hint `BROADCAST` и наивный DataFrame join при выключенном auto-broadcast.

### Код

**SQL:**
```sql
SELECT /*+ BROADCAST(s) */ b.key, count(*) AS cnt
FROM big b JOIN small s ON b.key = s.key
GROUP BY b.key
```

**DataFrame (naive):**
```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
big.join(small, on="key").groupBy("key").count()
```

> Важно: hint должен ссылаться на **алиас** `s`, а не на имя вью `small`. Иначе Spark 4 игнорирует hint.

### Результаты

| Метод | Время |
|-------|-------|
| SQL + hint | 0.11 с |
| DataFrame naive | 0.38 с |

### Планы

**SQL (hint):** `BroadcastHashJoin ... BuildRight` + `BroadcastExchange` - маленькая таблица разослана на все executors, большая не перемещается для join.

**DataFrame (naive):** `SortMergeJoin` - обе стороны сортируются и перемешиваются, хотя одна из них крошечная.

SQL-hint даёт **явный контроль** над физическим планом, когда оптимизатор по конфигурации выбирает неоптимальную стратегию.

---

## SQL-кейс 2: SHUFFLE_HASH join (*)

### Задача

Join двух больших таблиц (20 млн + 2 млн), где broadcast невозможен. Сравнить SQL с hint `SHUFFLE_HASH` и наивный DataFrame join (дефолтный SortMergeJoin).

### Код

**SQL:**
```sql
SELECT /*+ SHUFFLE_HASH(m) */ b.key, b.value, m.s
FROM big2 b JOIN med m ON b.key = m.key
```

**DataFrame (naive):**
```python
big.join(med, on="key")
```

### Результаты

| Метод | Время |
|-------|-------|
| SQL + hint | 1.18 с |
| DataFrame naive | 2.00 с |

### Планы

**SQL (hint):** `ShuffledHashJoin ... BuildRight` - join через hash-таблицу без предварительной сортировки обеих сторон.

**DataFrame (naive):** `SortMergeJoin` - дополнительная сортировка обеих сторон перед merge.

Для равномерно распределённых ключей ShuffledHashJoin часто быстрее SortMergeJoin, потому что убирает дорогую фазу Sort. SQL-hint позволяет форсировать эту стратегию без `broadcast()` / ручных настроек в DataFrame API.

---

## Общие выводы

### Когда выбирать DataFrame вместо RDD

| Причина | DataFrame | RDD (Python) |
|---------|-----------|--------------|
| Shuffles | Минимум (Catalyst объединяет операции) | По одному на каждую операцию |
| Проходы по данным | 1 | По 1 на каждую метрику/трансформацию |
| Сериализация | Нет (Tungsten binary) | Pickle на каждой границе JVM/Python |
| Codegen | Whole-stage JVM bytecode | Интерпретация Python |
| Спец. оптимизации | partial agg, WindowGroupLimit, pushdown | Нет |

### Когда SQL выигрывает у "наивного" DataFrame

SQL hint'ы (`BROADCAST`, `SHUFFLE_HASH`, `COALESCE`, ...) дают декларативный контроль над физическим планом. DataFrame API для того же эффекта требует `broadcast()`, `hint()` или настройки конфигурации - и без них оптимизатор может выбрать SortMergeJoin даже когда есть лучший вариант.

### Практическое правило

1. Начинайте с **DataFrame / SQL** - Catalyst оптимизирует большинство задач.
2. К **RDD** - только для нетабличных данных или низкоуровневого контроля.
3. Всегда проверяйте `df.explain(True)` - это показывает, какие оптимизации реально сработали.
4. Для join'ов с известным размером сторон используйте hint'ы или `broadcast()` явно.

## Запуск

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
cd hw6
python -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python experiments.py --case all --json results.json
```

Отдельный кейс: `.venv/bin/python experiments.py --case b`
