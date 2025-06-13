from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from airflow.hooks.base import BaseHook
minio_conn = BaseHook.get_connection("minio_conn")
minio_extras = minio_conn.extra_dejson

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
}

with DAG(
    dag_id='spark_pi_cluster_job',
    default_args=default_args,
    schedule=None,  # execução manual
    catchup=False,
    tags=['example', 'spark'],
) as dag:

    submit_spark_job = SparkSubmitOperator(
        task_id='submit_spark_pi',
        application='/opt/airflow/scripts/teste_minio.py',
        conn_id='spark_standalone',
        application_args=[],
        conf={
            'spark.driver.extraJavaOptions': '-Dlog4j.configuration=log4j.properties',
            'spark.eventLog.enabled': 'true',
            'spark.eventLog.dir': 'file:/opt/spark/events',

            # Configs do acesso ao MinIO
            'spark.hadoop.fs.s3a.access.key': minio_extras.get('s3a.access.key'),
            'spark.hadoop.fs.s3a.secret.key': minio_extras.get('s3a.secret.key'),
            'spark.hadoop.fs.s3a.endpoint': minio_extras.get('s3a.endpoint'),
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.com.amazonaws.services.s3.enableV4': 'true',
            'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
            'spark.hadoop.fs.AbstractFileSystem.s3a.impl': 'org.apache.hadoop.fs.s3a.S3A'
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,io.delta:delta-spark_2.12:3.2.0,io.delta:delta-storage:3.2.0",
        verbose=True,
        deploy_mode='client',
        total_executor_cores=4,
        executor_cores=2
    )

    submit_spark_job
