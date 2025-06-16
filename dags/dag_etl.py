from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from airflow.hooks.base import BaseHook

# Conexão com MinIO
minio_conn = BaseHook.get_connection("minio_conn")
minio_extras = minio_conn.extra_dejson


# Conexão com PostgreSQL
postgres_conn = BaseHook.get_connection("postgres_conn")
postgres_host = postgres_conn.host
postgres_port = postgres_conn.port
postgres_user = postgres_conn.login
postgres_pwd = postgres_conn.password
postgres_schema = postgres_conn.schema

postgres_db = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_schema}"


# Caminhos dos JARs
postgres_driver_jar = "/opt/spark/jars/postgresql-42.6.0.jar"
hadoop_driver_jar = "/opt/spark/jars/hadoop-aws-3.3.4.jar"
aws_driver_jar = "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
delta_driver_jar = "/opt/spark/jars/delta-spark_2.12-3.2.0.jar"

# Configurações padrão da DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
}


# Configuração padrão do Spark + MinIO
common_spark_conf = {
    'spark.eventLog.enabled': 'true',
    'spark.eventLog.dir': 'file:/opt/spark/events',
    'spark.hadoop.fs.s3a.access.key': minio_extras.get('s3a.access.key'),
    'spark.hadoop.fs.s3a.secret.key': minio_extras.get('s3a.secret.key'),
    'spark.hadoop.fs.s3a.endpoint': minio_extras.get('s3a.endpoint'),
    'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'spark.hadoop.fs.s3a.path.style.access': 'true',
    'spark.hadoop.com.amazonaws.services.s3.enableV4': 'true',
    'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
    'spark.hadoop.fs.AbstractFileSystem.s3a.impl': 'org.apache.hadoop.fs.s3a.S3A',
    'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
    'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog'
}

# Lista de JARs
common_jars = ",".join([
    postgres_driver_jar,
    hadoop_driver_jar,
    aws_driver_jar,
    delta_driver_jar
])


with DAG(
    dag_id='dag_etl',
    default_args=default_args,
    schedule=None,  # execução manual
    catchup=False,
    tags=['etl', 'spark'],
) as dag:

    # Task 1 - bronze
    spark_bronze = SparkSubmitOperator(
        task_id='submit_spark_bronze',
        application='/opt/airflow/scripts/01_bronze.py',
        conn_id='spark_standalone',
        application_args=[],
        conf=common_spark_conf,
        jars=common_jars,
        verbose=True,
        deploy_mode='client',
        total_executor_cores=4,
        executor_cores=2
    )

    # Task 2 - silver
    spark_silver = SparkSubmitOperator(
        task_id='submit_spark_silver',
        application='/opt/airflow/scripts/02_silver.py',
        conn_id='spark_standalone',
        application_args=[],
        conf=common_spark_conf,
        jars=common_jars,
        verbose=True,
        deploy_mode='client',
        total_executor_cores=4,
        executor_cores=2
    )

    # Task 3 - consume
    spark_gold = SparkSubmitOperator(
        task_id='submit_spark_consume',
        application='/opt/airflow/scripts/03_consume.py',
        conn_id='spark_standalone',
        conf=common_spark_conf,
        application_args=[postgres_db, postgres_user, postgres_pwd],
        jars=common_jars,
        verbose=True,
        deploy_mode='client',
        total_executor_cores=4,
        executor_cores=2
    )

    # Encadeamento das tarefas
    spark_bronze >> spark_silver >> spark_gold
