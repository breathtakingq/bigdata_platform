CREATE KEYSPACE IF NOT EXISTS microservice_metrics
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

CREATE TABLE IF NOT EXISTS microservice_metrics.service_metrics (
  service text,
  window_start timestamp,
  window_end timestamp,
  window_start_text text,
  window_end_text text,
  request_count bigint,
  error_count bigint,
  error_rate double,
  avg_latency_ms double,
  min_latency_ms int,
  max_latency_ms int,
  avg_request_size_bytes double,
  avg_response_size_bytes double,
  requests_per_second double,
  batch_id bigint,
  PRIMARY KEY ((service), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);
