# Платформа потоковой обработки логов микросервисов

Проект реализует pipeline обработки потоковых данных:

```text
Python generator -> Kafka -> Spark Structured Streaming -> HDFS + Cassandra -> Grafana
```

Система генерирует события микросервисного приложения, передает их в Kafka, обрабатывает поток в Spark, сохраняет сырые события в HDFS в формате Parquet и записывает агрегированные метрики в Cassandra. Grafana используется для просмотра графиков по ошибкам, времени ответа и нагрузке.

## Стек

- Apache Kafka — прием потоковых событий.
- Apache Spark Structured Streaming — обработка потока.
- HDFS — хранение сырых событий в Parquet.
- Cassandra — хранение агрегированных метрик.
- Python — генератор событий и PySpark-приложение.
- Docker Compose — запуск инфраструктуры.
- Grafana — визуализация метрик.

## Структура проекта

```text
.
├── cassandra/init.cql
├── docker-compose.yml
├── Makefile
├── generator/
│   ├── Dockerfile
│   ├── log_generator.py
│   └── requirements.txt
├── grafana/
│   ├── dashboards/microservice-metrics.json
│   └── provisioning/
├── scripts/
│   └── check_pipeline.sh
└── spark/
    └── jobs/
        └── streaming_app.py
```

## Запуск

Перед запуском должен быть включен Docker Desktop или Docker daemon.

```bash
docker compose up --build
```

Первый запуск может занять несколько минут, потому что Spark скачивает зависимости для Kafka и Cassandra.

Можно запускать через Makefile:

```bash
make up
```

## Веб-интерфейсы

- Grafana: http://localhost:13000
- Spark Master: http://localhost:18080
- HDFS NameNode: http://localhost:9870
- Kafka UI: http://localhost:18090

Данные для входа в Grafana:

```text
login: admin
password: admin
```

## Проверка Kafka

Посмотреть сообщения из topic:

```bash
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic microservice_logs \
  --from-beginning
```

Или открыть Kafka UI:

```text
http://localhost:18090
```

В интерфейсе должен быть cluster `bigdata-kafka`, topic `microservice_logs` и новые сообщения.

## Проверка Cassandra

Посмотреть агрегированные метрики:

```bash
docker compose exec cassandra cqlsh -e \
  "SELECT service, window_start, request_count, error_count, avg_latency_ms, requests_per_second FROM microservice_metrics.service_metrics LIMIT 20;"
```

Через Makefile:

```bash
make check-cassandra
```

## Проверка HDFS

Посмотреть структуру сохраненных Parquet-файлов:

```bash
docker compose exec namenode hdfs dfs -ls -R /hdfs/logs
```

Через Makefile:

```bash
make check-hdfs
```

Ожидаемая структура:

```text
/hdfs/logs/year=2026/month=6/day=14/
```

## Что делает Spark Streaming

Spark-приложение выполняет две операции:

1. Сохраняет сырые события из Kafka в HDFS в формате Parquet с партиционированием по `year`, `month`, `day`.
2. Считает агрегаты по окнам 30 секунд и сервисам:
   - количество запросов;
   - количество ошибок;
   - долю ошибок;
   - среднее, минимальное и максимальное время ответа;
   - интенсивность запросов;
   - средний размер запроса и ответа.

## Автоматическая проверка

Smoke-тест проверяет, что основные части pipeline работают:

```bash
make test
```

Проверяются:

- доступность Grafana;
- доступность Kafka UI;
- наличие Kafka topic `microservice_logs`;
- наличие Parquet-файлов в HDFS;
- наличие агрегатов в Cassandra.

## Полезные команды

```bash
make up              # запустить проект
make down            # остановить проект
make ps              # посмотреть контейнеры
make logs            # смотреть логи
make test            # проверить pipeline
make check-kafka     # прочитать 5 сообщений из Kafka
make check-hdfs      # проверить файлы в HDFS
make check-cassandra # проверить агрегаты в Cassandra
make check-grafana   # проверить Grafana
make clean           # остановить проект и удалить docker volumes
```

## Перенос на VPS

Для переноса достаточно скопировать проект на сервер с Docker и Docker Compose:

```bash
docker compose up --build -d
```
