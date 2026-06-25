from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("KafkaStreamingFixed")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR") 

schema = StructType([
    StructField("timestamp", DoubleType(), True),
    StructField("users", IntegerType(), True)
])

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "user_activity")
    .option("startingOffsets", "latest")
    .load()
)

json_df = (
    df.selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), schema).alias("data"))
    .select("data.*")
)

events = (
    json_df.withColumn("event_time", to_timestamp(col("timestamp")))
    .withWatermark("event_time", "10 seconds")
)

agg_df = (
    events.groupBy(window(col("event_time"), "10 seconds"))
    .agg(avg("users").alias("avg_users"))
    .select(
        col("window.start").alias("start"),
        col("window.end").alias("end"),
        col("avg_users")
    )
)

query = (
    agg_df.writeStream
    .format("console")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/spark_checkpoint_avg")
    .start()
)

query.awaitTermination()
