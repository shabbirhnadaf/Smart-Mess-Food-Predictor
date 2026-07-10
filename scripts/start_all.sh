#!/bin/bash
# Full startup sequence — run from project root
set -e

echo "========================================"
echo "  Smart Mess Predictor — Full Startup"
echo "========================================"

# 1. Start HDFS
echo "[1/9] Starting Hadoop HDFS..."
$HADOOP_HOME/sbin/start-dfs.sh
sleep 5

# 2. Setup HDFS dirs
echo "[2/9] Setting up HDFS directories..."
bash scripts/setup_hdfs_dirs.sh

# 3. Start ZooKeeper
echo "[3/9] Starting ZooKeeper..."
$KAFKA_HOME/bin/zookeeper-server-start.sh \
  -daemon config/kafka/zookeeper.properties
sleep 5

# 4. Start Kafka broker
echo "[4/9] Starting Kafka broker..."
$KAFKA_HOME/bin/kafka-server-start.sh \
  -daemon config/kafka/server.properties
sleep 5

# 5. Create topics
echo "[5/9] Creating Kafka topics..."
bash scripts/create_kafka_topics.sh

# 6. Generate seed data
echo "[6/9] Generating seed training data..."
python data/seed/generate_seed_data.py

# 7. Feature engineering
echo "[7/9] Running Spark feature engineering..."
spark-submit --master local[*] spark/batch_feature_engineering.py

# 8. Train models
echo "[8/9] Training ML models..."
spark-submit --master local[*] spark/train_demand_model.py
python ml/prophet_trainer.py

# 9. Start producers + API + dashboard
echo "[9/9] Starting services..."
python kafka/producer/order_producer.py --rate 5 &
python kafka/producer/weather_producer.py &
python kafka/producer/event_producer.py &

spark-submit --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.3 \
  spark/streaming_aggregator.py &

uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload &

cd dashboard && npm start &

echo "========================================"
echo "  ALL SERVICES RUNNING"
echo "  API:       http://localhost:8000"
echo "  API Docs:  http://localhost:8000/docs"
echo "  Dashboard: http://localhost:3000"
echo "  HDFS UI:   http://localhost:9870"
echo "========================================"
wait