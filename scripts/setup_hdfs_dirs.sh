#!/bin/bash
# Run AFTER starting HDFS: bash scripts/setup_hdfs_dirs.sh
set -e
echo "Creating HDFS directory structure..."
hdfs dfs -mkdir -p /mess-predictor/raw/orders
hdfs dfs -mkdir -p /mess-predictor/processed/features
hdfs dfs -mkdir -p /mess-predictor/processed/streaming_agg
hdfs dfs -mkdir -p /mess-predictor/models
hdfs dfs -mkdir -p /mess-predictor/predictions/live
hdfs dfs -mkdir -p /mess-predictor/checkpoints/stream_agg
hdfs dfs -chmod -R 777 /mess-predictor
echo "HDFS structure created:"
hdfs dfs -ls -R /mess-predictor