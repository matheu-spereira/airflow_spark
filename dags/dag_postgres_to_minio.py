
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.base import BaseHook
from datetime import datetime

# Recupera credenciais do MinIO e PostgreSQL via Airflow Connections
minio_conn = BaseHook.get_connection("minio_conn")
minio_extras = minio_conn.extra_dejson

postgres_db = "jdbc:postgresql://db:5432/postgres"
postgres_user = "postgres"
postgres_pwd = "postgres"

postgres_driver_jar = "/opt/spark/jars/postgresql-42.6.0.jar"
hadoop_driver_jar = "/opt/spark/jars/hadoop-aws-3.3.4.jar"
aws_driver_jar = "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
delta_driver_jar = "/opt/spark/jars/delta-spark_2.12-3.2.0.jar"


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
}

with DAG(
    dag_id='postgres_to_minio_dag',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['spark', 'minio', 'postgres'],
) as dag:

    submit_spark_job = SparkSubmitOperator(
        task_id='submit_pg_to_minio',
        application='/opt/airflow/scripts/pg_to_minio.py',
        conn_id='spark_standalone',
        conf={
            'spark.hadoop.fs.s3a.access.key': minio_extras.get('s3a.access.key'),
            'spark.hadoop.fs.s3a.secret.key': minio_extras.get('s3a.secret.key'),
            'spark.hadoop.fs.s3a.endpoint': minio_extras.get('s3a.endpoint'),
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.com.amazonaws.services.s3.enableV4': 'true',
            'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
            'spark.hadoop.fs.AbstractFileSystem.s3a.impl': 'org.apache.hadoop.fs.s3a.S3A',
        },
        application_args=[postgres_db, postgres_user, postgres_pwd],
        jars=",".join([
        postgres_driver_jar,
        hadoop_driver_jar,
        aws_driver_jar,
        delta_driver_jar
        ]),
        verbose=True,
        deploy_mode='client',
        total_executor_cores=4,
        executor_cores=2
    )

    submit_spark_job
