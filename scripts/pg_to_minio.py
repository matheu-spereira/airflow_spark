import sys
from pyspark.sql import SparkSession

postgres_db = sys.argv[1]
postgres_user = sys.argv[2]
postgres_pwd = sys.argv[3]

def main():
    spark = SparkSession.builder \
        .appName("PostgresToMinIO") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Leitura do PostgreSQL
    df = spark.read \
        .format("jdbc") \
        .option("url", postgres_db) \
        .option("dbtable", "public.clientes") \
        .option("user", postgres_user) \
        .option("password", postgres_pwd) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    # Escrita no MinIO como Delta Lake
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save("s3a://bronze/postgres_data/")

    spark.stop()

if __name__ == "__main__":
    main()
