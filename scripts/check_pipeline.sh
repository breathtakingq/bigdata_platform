#!/usr/bin/env bash
set -euo pipefail

echo "Проверка контейнеров"
docker compose ps

echo "Проверка Grafana"
curl -fsS http://localhost:13000/login >/dev/null

echo "Проверка Kafka UI"
curl -fsS http://localhost:18090 >/dev/null

echo "Проверка Kafka topic"
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list | grep -q '^microservice_logs$'

echo "Проверка HDFS Parquet"
set +o pipefail
parquet_count=$(docker compose exec namenode hdfs dfs -ls -R /hdfs/logs | grep -c '.parquet')
set -o pipefail

if [[ "$parquet_count" -eq 0 ]]; then
  echo "В HDFS не найдены Parquet-файлы"
  exit 1
fi

echo "Проверка Cassandra"
rows=$(docker compose exec cassandra cqlsh -e "SELECT COUNT(*) FROM microservice_metrics.service_metrics;" | awk '/^[[:space:]]*[0-9]+[[:space:]]*$/ {print $1; exit}')

if [[ -z "${rows:-}" || "$rows" -eq 0 ]]; then
  echo "В Cassandra пока нет агрегатов"
  exit 1
fi

echo "Проверка завершена успешно"
