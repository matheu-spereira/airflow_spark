from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date

def main():
    spark = SparkSession.builder \
        .appName("TransformPiEstimate") \
        .getOrCreate()

    # Leitura dos dados do MinIO - camada bronze
    df = spark.read.format("delta").load("s3a://bronze/pi_estimate_delta/")

    # Inserir coluna com a data atual
    df_transformed = df.withColumn("data_processamento", current_date())

    # Gravar no MinIO - camada silver
    df_transformed.write.format("delta").mode("overwrite").save("s3a://silver/pi_estimate_delta_transformed/")

if __name__ == "__main__":
    main()
