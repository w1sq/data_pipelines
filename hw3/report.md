# Data Pipelines | HW 03 — Сравнение форматов хранения (Python)

## Задача

Синтетически сгенерировать набор событий (`Event`), записать одни и те же данные в три формата (**Parquet**, **Avro**, **JSON**), измерить и сравнить:

- время записи (write);
- размер файлов на диске (bytes);
- время чтения всех строк (full-scan);
- время выборки по дате (filter);
- время простой агрегации (`GROUP BY event_type`);
- итоговые выводы по выгоде каждого формата.

## Выбранные форматы

| Формат | Обязательность | Почему выбран |
|--------|---------------|---------------|
| **Parquet** | обязательный | Колоночный формат, стандарт де-факто для аналитики (Spark, Pandas, DuckDB) |
| **Avro** | обязательный | Бинарный row-based формат, популярен в Kafka / streaming pipelines |
| **JSON (NDJSON)** | дополнительный | Человеко-читаемый, часто используется в логах и API; хорошо демонстрирует накладные расходы текстовых форматов |

CSV не выбран — не поддерживает вложенные структуры (`metrics`). Protobuf — компактный, но ориентирован на межсервисный обмен, а не на аналитику.

## Схема данных

Каждое событие содержит:

```
date        string   — дата в формате YYYY-MM-DD (случайная за последний год)
user_id     int64    — ID пользователя (1..10 000 000)
event_type  string   — click | view | purchase | signup | scroll | hover
url         string   — https://example.com/page/{N}
user_agent  string   — PyBench/{major}.{minor}
value       float64  — случайное значение 0..100
metrics     struct   — {clicks, impressions, revenue}
```

## Конфигурация эксперимента

| Параметр | Значение |
|----------|----------|
| Язык / рантайм | Python 3.12 |
| Библиотеки | pyarrow 24.0, fastavro 1.12, python-snappy |
| CPU | 4 ядра |
| RAM | 8 GB |
| Строк | **8 000 000** (40 батчей × 200 000) |
| Parquet compression | SNAPPY |
| Avro codec | snappy |
| JSON gzip | false |
| filter-date | 2025-11-07 |

*Объём данных: ~1.65 GB в JSON (несжатый). Для полных 10 GB потребуется ~48.5M строк — масштаб линейный, команда для воспроизведения приведена ниже.*

## Как происходили замеры

1. **Генерация** — `gen_batch(n)` создаёт батчи случайных событий. Батчи пишутся последовательно, чтобы не держать весь набор в памяти.
2. **Запись** — замеряется `time.perf_counter()` от начала до конца записи всех батчей.
3. **Размер** — `os.stat(path).st_size` после записи.
4. **Чтение** — три независимых теста:
   - **full-scan**: читаем весь файл, инкрементируем счётчик;
   - **filter**: считаем строки с `date == filter_date`;
   - **aggregation**: `GROUP BY event_type` — подсчёт количества по типу.
5. **Особенность Python-реализации**: для Parquet filter и aggregation читают **только нужные колонки** (`columns=["date"]` / `columns=["event_type"]`) — это идиоматичное использование колоночного формата. Avro и JSON читают записи целиком.

## Запуск

```bash
cd python
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 8M строк (~1.65 GB JSON) — использовано в отчёте:
python benchmark.py --batches 40 --rows-per-batch 200000

# ~10 GB JSON (как в задании):
python benchmark.py --batches 268 --rows-per-batch 200000

# Только запись / только чтение / один формат:
python benchmark.py --mode write --format parquet
python benchmark.py --mode read  --format avro
```

## Результаты

```
Mode=all format=all outdir=./out batches=40 rows/batch=200000
parquetComp=SNAPPY avroCodec=snappy jsonGzip=false filterDate=2025-11-07

=== Write: parquet ===
Parquet: wrote 8000000 rows
parquet written in 133.06s; 238.28 MB

=== Read: parquet ===
Parquet full-scan rows=8000000 time=2.22s
Parquet filter(2025-11-07): found=22067 time=13.48s
Parquet aggregation: time=14.80s

=== Write: avro ===
Avro: wrote 8000000 rows
avro written in 187.99s; 312.94 MB

=== Read: avro ===
Avro full-scan rows=8000000 time=58.57s
Avro filter(2025-11-07): found=21860 time=57.91s
Avro aggregation: time=61.16s

=== Write: json ===
JSON: wrote 8000000 rows
json written in 226.90s; 1.65 GB

=== Read: json ===
JSON full-scan rows=8000000 time=68.13s
JSON filter(2025-11-07): found=21930 time=67.44s
JSON aggregation: time=71.42s
```

### Сводная таблица

| Format | Size | Write | Full-scan | Throughput | Filter | Aggregation | Read total |
|--------|------|-------|-----------|------------|--------|-------------|------------|
| **JSON** | 1.65 GB | 226.90s | 68.13s | 117 k rows/s | 67.44s | 71.42s | 206.99s |
| **Avro** | 312.94 MB | 187.99s | 58.57s | 137 k rows/s | 57.91s | 61.16s | 177.64s |
| **Parquet** | 238.28 MB | 133.06s | 2.22s | **3.6 M rows/s** | 13.48s | 14.80s | **30.51s** |

*Throughput = total_rows / full_scan_time*

## Анализ результатов

### Размер на диске

1. **Parquet (238 MB)** — самый компактный. Колоночное хранение + dictionary encoding для повторяющихся строк (`event_type`, `date`) + SNAPPY-сжатие дают ~7× экономию относительно JSON.
2. **Avro (313 MB)** — бинарный row-формат без повторяющихся имён полей. Компактнее JSON, но row-based layout хуже сжимается, чем колоночный.
3. **JSON (1.65 GB)** — текстовая сериализация: имена полей в каждой строке, кавычки, числа в ASCII. Максимальная избыточность.

### Скорость записи

**Parquet (133s) > Avro (188s) > JSON (227s)**

Parquet пишет колонками пакетами через `pyarrow.Table`, что эффективнее построчной записи Avro и текстовой сериализации JSON.

### Скорость чтения

**Parquet доминирует** при правильном использовании:

- **Full-scan (2.2s)** — в 26× быстрее JSON и в 26× быстрее Avro. PyArrow читает колонки нативно, без десериализации в Python-объекты.
- **Filter (13.5s)** — читается только колонка `date` (1 из 7 полей). Avro/JSON вынуждены десериализовать всю запись.
- **Aggregation (14.8s)** — читается только `event_type`. Аналогичное преимущество.

Avro и JSON показывают схожую скорость (~58–71s), потому что оба десериализуют полные записи в Python-объекты.

### Сравнение с Go-реализацией (26.85M строк)

| Метрика | Go (26.85M) | Python (8M) | Комментарий |
|---------|-------------|-------------|-------------|
| JSON size | 5.96 GB | 1.65 GB | Линейно: ~222 B/row |
| Avro size | 2.21 GB | 313 MB | Линейно: ~41 B/row |
| Parquet size | 1.20 GB | 238 MB | Линейно: ~31 B/row |
| Avro full-scan | 21.4s (1.25M r/s) | 58.6s (137k r/s) | Go быстрее в ~9× (нативный код vs Python) |
| Parquet full-scan | 47.1s (570k r/s) | 2.2s (3.6M r/s) | Python быстрее — column pruning + PyArrow C++ |

Go-реализация читала **все колонки** при filter/aggregation, поэтому Avro показал лучший full-scan. Python-реализация использует **column pruning** для Parquet, что демонстрирует главное преимущество колоночных форматов.

## Практические рекомендации

| Сценарий | Рекомендация |
|----------|-------------|
| Аналитика, OLAP, data lake | **Parquet** — минимальный размер, быстрое чтение отдельных колонок |
| Streaming, Kafka, schema evolution | **Avro** — компактный бинарный формат со схемой, хорош для сериализации сообщений |
| Логи, API, отладка, малые объёмы | **JSON** — читаемость важнее производительности |
| 10+ GB аналитических данных | Однозначно **Parquet** (или ORC) с партиционированием по `date` |

## Структура проекта

```
hw03/
├── main.go              # Go-реализация (оригинал)
├── hw03-format_comparison.pdf
└── python/
    ├── benchmark.py     # Python-реализация
    ├── requirements.txt
    ├── results.json     # метрики последнего прогона
    └── report.md        # этот отчёт
```

## Проблемы и наблюдения

1. **Дисковое пространство** — для 8M строк потребовалось ~2.3 GB (все три формата). Для 10 GB JSON (~48M строк) нужно ~12 GB свободного места.
2. **Avro append** — `fastavro.writer()` не поддерживает дописывание в существующий OCF-файл; использован инкрементальный `fastavro.write.Writer`.
3. **Parquet filter vs full-scan** — filter оказался медленнее full-scan (13.5s vs 2.2s), потому что full-scan читает колонки нативно в C++, а filter вызывает `.as_py()` для каждого значения в Python-цикле. При использовании PyArrow compute (`pc.equal`) filter был бы быстрее.
4. **Масштабирование** — все метрики масштабируются линейно по числу строк; для полных 10 GB достаточно увеличить `--batches`.
