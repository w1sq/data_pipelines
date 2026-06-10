# Reusable Data Pipeline on Argo Workflows

## О проекте
Проект показывает, как из переиспользуемых `WorkflowTemplate` собрать осмысленный data pipeline в `Argo Workflows`. Вместо одного большого workflow с жёстко зашитой логикой каждая частая задача оркестрации вынесена в отдельный универсальный шаблон, который параметризуется через `inputs.parameters` и обменивается данными через `outputs.parameters` и `outputs.artifacts`.

На вход пайплайн получает URL датасета. Дальше он скачивает данные, проверяет их качество, агрегирует и формирует итоговый markdown-отчёт.

## Что делает пайплайн
1. Скачивает датасет (CSV) по URL.
2. Проверяет качество данных: наличие обязательных колонок и минимальное число строк.
3. Агрегирует данные с группировкой по колонке (`sum` / `avg` / `count`).
4. Формирует итоговый markdown-отчёт.

## Почему шаблоны универсальные и переиспользуемые
- Каждая частая задача оркестрации (скачать, проверить, агрегировать, отчитаться) вынесена в отдельный `WorkflowTemplate`.
- Все шаблоны принимают параметры через `inputs.parameters`, поэтому их можно применять к любым датасетам, а не только к демо.
- Данные между шагами передаются через `outputs.artifacts` (файлы) и `outputs.parameters` (метаданные), без жёстко прошитых путей.
- Основной `Workflow` связывает шаблоны через `templateRef`, поэтому каждый блок легко переиспользовать в других пайплайнах.

## Состав WorkflowTemplate
- `01-http-fetch-template.yaml` — универсальный шаблон скачивания файла по URL.
  - inputs.parameters: `source-url`, `output-filename`
  - outputs.parameters: `content-size`
  - outputs.artifacts: `fetched-data`
- `02-csv-quality-template.yaml` — проверка качества CSV.
  - inputs.parameters: `required-columns`, `min-rows`; inputs.artifacts: `dataset`
  - outputs.parameters: `quality-status`, `row-count`, `missing-columns`
  - outputs.artifacts: `quality-report`
- `03-csv-aggregate-template.yaml` — агрегация CSV с группировкой.
  - inputs.parameters: `group-by`, `value-column`, `agg`; inputs.artifacts: `dataset`
  - outputs.parameters: `groups-count`
  - outputs.artifacts: `aggregated`
- `04-report-template.yaml` — сборка финального markdown-отчёта.
  - inputs.parameters: `pipeline-name`, `quality-status`, `row-count`, `missing-columns`, `groups-count`, `agg`
  - inputs.artifacts: `quality-report`, `aggregated`
  - outputs.artifacts: `final-report`

## Входные параметры основного Workflow
- `source-url` — URL датасета.
- `dataset-filename` — имя файла внутри контейнера, например `sales-ok.csv`.
- `pipeline-name` — логическое имя пайплайна.
- `required-columns` — обязательные колонки через запятую, например `region,product,amount`.
- `min-rows` — минимальное число строк.
- `group-by` — колонка группировки.
- `value-column` — числовая колонка для агрегации.
- `agg` — функция агрегации: `sum`, `avg` или `count`.

## Схема пайплайна
```text
            fetch-data
            /         \
           v           v
   check-quality   aggregate-data
            \         /
             v       v
          generate-report
```
`check-quality` и `aggregate-data` зависят только от `fetch-data`, поэтому выполняются параллельно. `generate-report` ждёт оба шага.

## Структура проекта
```text
hw11/
??? demo/
?   ??? data-demo-server.yaml
??? scripts/
?   ??? common.sh
?   ??? bootstrap-local-demo.sh
?   ??? run-demo-success.sh
?   ??? run-demo-failure.sh
?   ??? destroy-local-demo.sh
??? templates/
?   ??? 01-http-fetch-template.yaml
?   ??? 02-csv-quality-template.yaml
?   ??? 03-csv-aggregate-template.yaml
?   ??? 04-report-template.yaml
??? workflows/
?   ??? data-pipeline-workflow.yaml
??? speach_text.md
??? README.md
```

## Базовые команды запуска
```bash
kubectl apply -n argo -f hw11/templates/
argo submit -n argo hw11/workflows/data-pipeline-workflow.yaml
```

## Пример запуска через Argo
```bash
argo submit -n argo hw11/workflows/data-pipeline-workflow.yaml \
  -p source-url=https://example.com/sales.csv \
  -p dataset-filename=sales.csv \
  -p pipeline-name=SalesAnalytics \
  -p required-columns="region,product,amount" \
  -p min-rows=5 \
  -p group-by=region \
  -p value-column=amount \
  -p agg=sum
```

Workflow использует `generateName: data-pipeline-`, поэтому каждый запуск создаёт уникальный объект — удобно для демонстрации нескольких сценариев подряд.

## Воспроизводимый локальный demo-стенд
Проект можно показать полностью локально на `kind`:

- поднимается кластер `kind`;
- устанавливается `Argo Workflows` из официального `quick-start-minimal.yaml`;
- вместе с Argo поднимается встроенный `MinIO`, поэтому `outputs.artifacts` реально сохраняются;
- разворачивается `data-demo-server`, который внутри кластера отдаёт тестовые датасеты:
  - `sales-ok.csv` (корректный)
  - `sales-broken.csv` (без колонки `amount` и со слишком малым числом строк)

### Что нужно локально
- `docker`
- `kubectl`
- `curl`

Скрипты сами докачают `kind` и `argo` в каталог `hw11/bin`, если этих утилит нет в системе.

### Поднять стенд
```bash
./hw11/scripts/bootstrap-local-demo.sh
```
Первый запуск может занять несколько минут — Docker качает образ `kindest/node` и образы Argo. При медленном интернете:
```bash
ARGO_WAIT_TIMEOUT=900s DEMO_WAIT_TIMEOUT=600s ./hw11/scripts/bootstrap-local-demo.sh
```

### Показать успешный прогон
```bash
./hw11/scripts/run-demo-success.sh
```
Использует `sales-ok.csv`. Ожидаемый результат:
- `quality-status = OK`
- агрегированный CSV с суммой `amount` по регионам
- в логах `generate-report` выводится итоговый markdown-отчёт
- workflow завершается в статусе `Succeeded`

### Показать прогон с ошибкой данных
```bash
./hw11/scripts/run-demo-failure.sh
```
Использует `sales-broken.csv`. Ожидаемый результат:
- `quality-status = FAILED`
- `missing-columns = amount`
- workflow всё равно завершается успешно: отчёт собирается даже при плохом качестве данных (агрегация устойчива к отсутствующей колонке)

### Удалить локальный стенд
```bash
./hw11/scripts/destroy-local-demo.sh
```

## Просмотр статуса и логов
```bash
argo list -n argo
argo get -n argo <workflow-name>
argo logs -n argo <workflow-name>
```

## Ожидаемый результат
- скачанный датасет как artifact после `fetch-data`;
- параметры качества и текстовый отчёт после `check-quality`;
- агрегированный CSV после `aggregate-data`;
- финальный markdown-отчёт как artifact после `generate-report`.
