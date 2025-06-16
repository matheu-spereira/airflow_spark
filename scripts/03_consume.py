import sys
from pyspark.sql import SparkSession

postgres_db = sys.argv[1]  
postgres_user = sys.argv[2]
postgres_pwd = sys.argv[3]

def main():
    spark = SparkSession.builder \
        .appName("WriteToPostgres") \
        .getOrCreate()
    
    # Leitura dos dados do MinIO (camada Silver)
    df = spark.read.format("delta").load("s3a://silver/pi_estimate_delta_transformed/")

    # Escrita no PostgreSQL
    df.write \
        .format("jdbc") \
        .option("url", postgres_db) \
        .option("dbtable", "public.pi_estimativas") \
        .option("user", postgres_user) \
        .option("password", postgres_pwd) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

if __name__ == "__main__":
    main()
