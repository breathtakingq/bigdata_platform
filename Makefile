.PHONY: up down restart ps logs test check-hdfs check-cassandra check-kafka check-grafana clean

up:
	docker compose up --build -d

down:
	docker compose down

restart:
	docker compose restart

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=120

test:
	./scripts/check_pipeline.sh

check-hdfs:
	docker compose exec namenode hdfs dfs -ls -R /hdfs/logs

check-cassandra:
	docker compose exec cassandra cqlsh -e "SELECT service, window_start, request_count, error_count, avg_latency_ms, requests_per_second FROM microservice_metrics.service_metrics LIMIT 20;"

check-kafka:
	docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic microservice_logs --from-beginning --max-messages 5

check-grafana:
	curl -I http://localhost:13000/login

clean:
	docker compose down -v
