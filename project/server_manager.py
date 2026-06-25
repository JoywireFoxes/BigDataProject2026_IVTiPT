#!/usr/bin/env python3
from kafka import KafkaConsumer, KafkaProducer
import json
import time

# =========================
# НАСТРОЙКИ
# =========================
USERS_PER_SERVER = 1000        # сколько человек держит 1 сервер
HYSTERESIS = 0.2               # 20% гистерезис
MIN_SERVERS = 1                # минимум серверов всегда
MAX_SERVERS = 10               # максимум серверов (ограничение)

THRESHOLD_ON = USERS_PER_SERVER                        # 1000
THRESHOLD_OFF = USERS_PER_SERVER * (1 - HYSTERESIS)   # 800

# =========================
# KAFKA
# =========================
consumer = KafkaConsumer(
    'user_metrics',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# =========================
# СОСТОЯНИЕ
# =========================
current_servers = MIN_SERVERS
prev_time = time.time()

print(f"Server Manager запущен.")
print(f"  1 сервер = {USERS_PER_SERVER} чел.")
print(f"  Гистерезис = {HYSTERESIS*100}%")
print(f"  Включение при прогнозе ≥ {THRESHOLD_ON}")
print(f"  Выключение при факте < {THRESHOLD_OFF}")
print(f"  Минимум серверов: {MIN_SERVERS}, максимум: {MAX_SERVERS}")
print("-" * 50)

# =========================
# ОСНОВНОЙ ЦИКЛ
# =========================
for msg in consumer:
    data = msg.value
    users = data['users']
    derivative = data['derivative']
    avg_users = data['avg_users']

    # Прогноз на следующий шаг
    predicted = users + derivative

    # Сколько серверов нужно под прогноз
    desired_by_predicted = max(1, int(predicted / USERS_PER_SERVER) + (1 if predicted % USERS_PER_SERVER > 0 else 0))
    desired_by_predicted = min(desired_by_predicted, MAX_SERVERS)

    # Сколько серверов нужно под факт (для выключения)
    desired_by_fact = max(1, int(users / USERS_PER_SERVER) + (1 if users % USERS_PER_SERVER > 0 else 0))
    desired_by_fact = min(desired_by_fact, MAX_SERVERS)

    # ЛОГИКА ГИСТЕРЕЗИСА
    if predicted >= current_servers * THRESHOLD_ON and desired_by_predicted > current_servers:
      servers_needed = desired_by_predicted
      action = "ВКЛЮЧИТЬ ДОП. СЕРВЕР"
    elif users < current_servers * THRESHOLD_OFF and derivative <= 0 and desired_by_fact < current_servers:
      servers_needed = desired_by_fact
      action = "ВЫКЛЮЧИТЬ ДОП. СЕРВЕР"
    else:
      servers_needed = current_servers
      action = "ИЗМЕНЕНИЯ НЕ ТРЕБУЮТСЯ"

    # Применяем
    old_servers = current_servers
    current_servers = max(MIN_SERVERS, min(MAX_SERVERS, servers_needed))
    changed = current_servers != old_servers

    # Вывод
    status = "●" if changed else "○"
    print(f"{status} users={users:5d} | derivative={derivative:+5d} | predicted={predicted:5d} | "
          f"серверов: {old_servers} → {current_servers} | {action}")

    # Отправляем в Kafka
    output = {
        "servers": current_servers,
        "users": users,
        "predicted": predicted,
        "derivative": derivative,
        "timestamp": time.time()
    }
    producer.send('server_count', value=output)