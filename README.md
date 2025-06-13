# airflow_spark

docker compose up airflow-init
docker compose up -d


## Criando conexões no airflow
### Spark:
- Connection ID: spark_standalone
- Connection Type: spark
- Port 7077
- extras.Deploy mode: client
- extras.Spark binary: spark-submit


### minio:
- Connection ID: minio_conn
- Connection Type: http
- extra:
{
  "s3a.access.key": "minio",
  "s3a.secret.key": "minio123",
  "s3a.endpoint": "http://minio:9000"
}
