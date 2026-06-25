#!/usr/bin/env python3
from kafka import KafkaConsumer  # импорт Kafka consumer

# создаём consumer, который подключается к Kafka
consumer = KafkaConsumer(
    'user_activity',              # имя топика (как в producer)
    bootstrap_servers='localhost:9092',  # адрес Kafka
    auto_offset_reset='earliest', # читать с самого начала
    enable_auto_commit=True,      # автоматически отмечать прочитанное
    group_id='test-group'         # группа потребителей
)

print("Consumer started. Waiting for messages...")

# бесконечный цикл чтения сообщений
for message in consumer:
    print("Received:", message.value.decode('utf-8'))  # выводим данные
