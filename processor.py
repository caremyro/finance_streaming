from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import window, avg, from_unixtime
import os
from dotenv import load_dotenv

load_dotenv()
url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")

# Config`uration du SparkSession pour lire depuis Kafka
spark = SparkSession.builder \
    .appName("FinanceStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.postgresql:postgresql:42.7.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Définition du schéma pour les données boursières
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", DoubleType(), True)
])

# Lecture des données depuis Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "finance-streaming") \
    .load()


json_df = df.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast("timestamp"))

# Affichage des données transformées dans la console
#query = json_df.writeStream \
#    .outputMode("append") \
#    .format("console") \
#    .start()

aggregated_df = json_df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(
        window(col("timestamp"), "10 seconds"),
        col("symbol")
    ) \
    .agg(avg("price").alias("average_price"))

# Ecriture des données dans PostgreSQL
def write_to_postgres(batch_df, batch_id):
    try:
        if not batch_df.isEmpty():
            final_df = batch_df.select(
                col("symbol"),
                col("average_price"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end")
            )

            print(f"--- Tentative d'écriture du Batch {batch_id} ---")
            final_df.show()

            final_df.write \
                .format("jdbc") \
                .option("url", url) \
                .option("dbtable", "stock_prices") \
                .option("user", user) \
                .option("password", password) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            print(f"--- Batch {batch_id} écrit avec succès ! ---")
        else:
            print(f"--- Batch {batch_id} vide ---")
    except Exception as e:
        print(f"!!! Erreur lors de l'écriture du batch {batch_id} : {e}")

query = aggregated_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("update") \
    .option("checkpointLocation", "./checkpoint_dir") \
    .start()

# Attente de la fin du streaming
query.awaitTermination()