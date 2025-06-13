from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType

def main():
    spark = SparkSession.builder \
        .appName("SparkPiToMinIO") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


    def inside(p):
        from random import random
        x, y = random(), random()
        return x*x + y*y < 1

    count = spark.sparkContext.parallelize(range(100000)).filter(inside).count()
    pi = 4.0 * count / 100000
    print(f"Approximate value of Pi is: {pi}")

    # Criar DataFrame com valor de Pi
    schema = StructType([StructField("pi_estimate", DoubleType(), False)])
    df = spark.createDataFrame([(pi,)], schema)

    # Gravar no MinIO (formato Parquet, você pode mudar para CSV se quiser)
    #df.write.mode("overwrite").parquet("s3a://bronze/pi_estimate/")
    df.write.format("delta").mode("overwrite").save("s3a://bronze/pi_estimate_delta/")

    spark.stop()

if __name__ == "__main__":
    main()
