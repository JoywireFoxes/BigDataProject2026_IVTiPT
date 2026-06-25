#!/usr/bin/env python3
from kafka import KafkaProducer   # Kafka отправитель
import json                       # JSON формат сообщений
import time                       # задержка по времени
import math                       # синусоида
import random                     # шум (реализм)

# подключение к Kafka брокеру
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # адрес Kafka
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # dict → JSON → bytes
)

# параметры синусоиды
t = 0                               # время (ось X)
base = 3000                        # средний онлайн
amplitude = 1500                   # амплитуда колебаний

while True:

    # 1. синусоидальная нагрузка
    sine_value = math.sin(t)

    # 2. добавляем шум (±100 пользователей)
    noise = random.randint(-100, 100)

    # 3. считаем итоговый онлайн
    users = int(base + amplitude * sine_value + noise)

    # защита от отрицательных значений
    users = max(0, users)

    # 4. формируем сообщение
    message = {
        "timestamp": time.time(),   # текущее время
        "users": users              # онлайн
    }

    # 5. отправляем в Kafka
    producer.send("user_activity", value=message)

    # 6. лог в консоль (для проверки)
    #print(f"SENT → {message}")

    # 7. двигаем время (скорость волны)
    t += 0.1

    # 8. задержка (1 сек = 1 тик системы)
    time.sleep(1)
