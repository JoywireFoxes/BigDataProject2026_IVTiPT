#!/bin/bash

KAFKA_HOME=/home/kafka/kafka
TOPIC="user_activity"

# start project
echo -e "\e[33mProject starting.\e[0m"

# hdfs
start-dfs.sh
hdfs dfsadmin -safemode leave


echo -e "\e[35mStage 0 init zookeepeer and kafka.\e[0m"


# try to start ZOO

echo -e "\e[33mStarting Zookeeper...\e[0m"  # жёлтый текст (инфо)

sudo systemctl start zookeeper               # запускаем Zookeeper

ZK_STATUS=$(systemctl is-active zookeeper)   # получаем статус

if [ "$ZK_STATUS" = "active" ]; then
    echo -e "\e[32mZookeeper is RUNNING\e[0m"  # зелёный = успех
else
    echo -e "\e[31mZookeeper FAILED\e[0m"      # красный = ошибка
    exit 1                                     # стопаем скрипт
fi


# try to start Kafka
echo -e "\e[33mStarting Kafka...\e[0m"

sudo systemctl start kafka
sleep 10

STATUS=$(systemctl is-active kafka)

if [ "$STATUS" = "active" ]; then
    echo -e "\e[32mKafka is RUNNING\e[0m"
else
    echo "\e[31mKafka is NOT running\e[0m"
    exit 1
fi

# Check and create Kafka TOPIC
echo -e "\e[35mStage 1 Creating online topic and start online_simulation.\e[0m"

echo -e "\e[33mChecking kafka topic...\e[0m"
 
$KAFKA_HOME/bin/kafka-topics --list --bootstrap-server localhost:9092 | grep -q $TOPIC

if [ $? -ne 0 ]; then
  echo -e "\e[33mCreating topic...\e[0m"

  $KAFKA_HOME/bin/kafka-topics --create \
    --topic $TOPIC \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1

  echo -e "\e[32mTopic created\e[0m"
else
  echo -e "\e[32mTopic already exists\e[0m"
fi

# start scripts
echo -e "\e[32mStarting online_sim.py script...\e[0m"
python3 online_sim.py &

echo -e "\e[32mStarting spark_stream2.py script...\e[0m"

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  spark_stream2.py > /dev/null 2>&1 &
  # > dev... this is change output info adress. For clean console.

# Ждём 10 секунд, чтобы Spark разогнался
echo -e "\e[33mWaiting 10 seconds for Spark to initialize...\e[0m"
sleep 15

# Запускаем server_manager
echo -e "\e[32mStarting server_manager.py script...\e[0m"
python3 server_manager.py


