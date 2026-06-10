# Spark (Scala) для задания DataPipelines

Проект считает 2 метрики по логам пользовательских сессий:

1. Количество карточных поисков (`CARD_SEARCH_*`), где искали документ `ACC_45616`.
2. Количество открытий каждого документа, найденного через быстрый поиск (`QS`), по дням.

## Быстрый старт (Docker Compose)

Нужен только Docker и Docker Compose. Java/Maven/Spark на хосте ставить не нужно.

```bash
cd /root/Sync/HSE/DataPipelines/final/spark-scala
docker compose up --build
```

Что делает `docker compose up --build`:

1. Собирает JAR через Maven внутри `Dockerfile` (multi-stage build).
2. Запускает Spark job в контейнере на образе `apache/spark:3.5.1` от `root` (так контейнер может писать в смонтированный `./output`).
3. Читает данные из `../sessions` (read-only).
4. Пишет результат в `./output`.

Повторный запуск без пересборки:

```bash
docker compose up
```

Очистить старый результат перед новым прогоном:

```bash
rm -rf output
docker compose up --build
```

## Где смотреть результат

После выполнения появятся:

- `output/metric1_card_search_target_count` — текстовый файл с итогом по `ACC_45616`.
- `output/metric2_daily_qs_document_opens` — CSV с колонками:
  - `day`
  - `documentId`
  - `openCount`

```bash
ls -la output
find output -type f
```

## Структура Docker-файлов

- `Dockerfile` — multi-stage: Maven собирает JAR, Spark-образ запускает job, рантайм идёт от `root`.
- `docker-compose.yml` — volumes (`../sessions` → `/data/sessions`, `./output` → `/data/output`) и команда `spark-submit`.

## Альтернатива: локальный запуск без Docker

### Установить Java, Maven и Spark

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk maven
java -version
mvn -version
```

Spark можно распаковать в `/opt/spark` и добавить `SPARK_HOME=/opt/spark` в `~/.bashrc`.

### Сборка

```bash
cd /root/Sync/HSE/DataPipelines/final/spark-scala
mvn -DskipTests clean package
ls -la target/spark-session-metrics-1.0.0-jar-with-dependencies.jar
```

### Запуск

```bash
spark-submit \
  --class datapipelines.SessionMetricsApp \
  --master "local[*]" \
  target/spark-session-metrics-1.0.0-jar-with-dependencies.jar \
  --input /root/Sync/HSE/DataPipelines/final/sessions \
  --output /root/Sync/HSE/DataPipelines/final/spark-scala/output
```

## Важно про устойчивость к грязным данным

Код специально:

- не падает на неполных/битых строках;
- корректно обрабатывает многострочные события `QS` и `CARD_SEARCH_*`;
- учитывает открытие документа из `DOC_OPEN` только если документ действительно входил в выдачу соответствующего `QS` (по `queryId`).
