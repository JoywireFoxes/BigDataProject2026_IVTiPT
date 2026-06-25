#!/usr/bin/env python3
from flask import Flask, render_template, jsonify
from kafka import KafkaConsumer
import json
from collections import deque
from datetime import datetime
import threading

app = Flask(__name__)

# =========================
# БУФЕРЫ
# =========================
WINDOW = 3600
timestamps = deque(maxlen=WINDOW)
users_data = deque(maxlen=WINDOW)
avg_users_data = deque(maxlen=WINDOW)
derivative_data = deque(maxlen=WINDOW)
servers_data = deque(maxlen=WINDOW)

latest_metrics = {"users": 0, "avg_users": 0, "derivative": 0}
latest_servers = {"servers": 1}
lock = threading.Lock()

# =========================
# KAFKA CONSUMERS
# =========================
metrics_consumer = KafkaConsumer(
    'user_metrics',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

servers_consumer = KafkaConsumer(
    'server_count',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

def poll_kafka():
    global latest_metrics, latest_servers

    while True:
        metrics_msgs = metrics_consumer.poll(timeout_ms=100)
        for tp, msgs in metrics_msgs.items():
            for msg in msgs:
                latest_metrics = msg.value

        servers_msgs = servers_consumer.poll(timeout_ms=100)
        for tp, msgs in servers_msgs.items():
            for msg in msgs:
                latest_servers = msg.value

        if latest_metrics["users"] > 0:
            with lock:
                now = datetime.now().strftime("%H:%M:%S")
                timestamps.append(now)
                users_data.append(latest_metrics["users"])
                avg_users_data.append(latest_metrics["avg_users"])
                derivative_data.append(latest_metrics["derivative"])
                servers_data.append(latest_servers.get("servers", 1))

# =========================
# МАРШРУТЫ
# =========================
@app.route('/')
def index():
    return render_template('monitor.html')

@app.route('/data')
def data():
    with lock:
        return jsonify({
            "timestamps": list(timestamps),
            "users": list(users_data),
            "avg_users": list(avg_users_data),
            "derivative": list(derivative_data),
            "servers": list(servers_data)
        })

# =========================
# ЗАПУСК
# =========================
if __name__ == '__main__':
    thread = threading.Thread(target=poll_kafka, daemon=True)
    thread.start()
    print("Монитор запущен: http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=False)