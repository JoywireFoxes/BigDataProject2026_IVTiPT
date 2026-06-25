#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType
from kafka import KafkaProducer
import json
import time

# =========================
# 1. SPARK SESSION
# =========================
spark = SparkSession.builder \
    .appName("KafkaStructuredStreamFinal") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/dstream_checkpoint") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# =========================
# 2. KAFKA PRODUCER ДЛЯ РЕЗУЛЬТАТОВ
# =========================
output_producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# =========================
# 3. SCHEMA ВХОДНЫХ ДАННЫХ
# =========================
schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("users", IntegerType())
])

# =========================
# 4. ЧИТАЕМ KAFKA
# =========================
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_activity") \
    .option("startingOffsets", "earliest") \
    .load()

# =========================
# 5. ПАРСИМ JSON
# =========================
parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select(
    col("data.users").alias("users")
)

# =========================
# 6. СОСТОЯНИЕ И ОБРАБОТКА
# =========================
state = {
    "prev_users": 0,
    "sum_users": 0,
    "count": 0
}

def process_batch(batch_df, batch_id):
    global state

    if batch_df.count() == 0:
        return

    rows = batch_df.collect()
    
    for row in rows:
        current_users = row.users

        # Обновляем состояние
        state["sum_users"] += current_users
        state["count"] += 1
        avg_users = state["sum_users"] / state["count"]

        # Производная: текущее минус предыдущее
        derivative = current_users - state["prev_users"]
        state["prev_users"] = current_users

        # Формируем результат
        output = {
            "users": current_users,
            "avg_users": round(avg_users, 2),
            "derivative": derivative,
            "timestamp": time.time()
        }

        # Отправляем в Kafka
        output_producer.send("user_metrics", value=output)

# =========================
# 7. ЗАПУСК ПОТОКА
# =========================
query = parsed.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .trigger(processingTime="1 second") \
    .start()

query.awaitTermination()