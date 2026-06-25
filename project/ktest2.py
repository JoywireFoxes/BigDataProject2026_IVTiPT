#!/usr/bin/env python3
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'user_metrics',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',        # только новые сообщения
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Читаю топик user_metrics... (Ctrl+C для выхода)\n")

for msg in consumer:
    print(f"← {msg.value}")