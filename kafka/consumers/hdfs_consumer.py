"""
Reads mess-orders → buffers 500 records → writes Parquet to HDFS via PySpark
Run: python kafka/consumers/hdfs_consumer.py
"""
import json,os,sys,time
from datetime import datetime
from pathlib import Path
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType,StructField,StringType,
    IntegerType,ArrayType,MapType)

load_dotenv()
BS           = os.getenv("KAFKA_BOOTSTRAP_SERVERS","localhost:9092")
TOPIC        = os.getenv("KAFKA_TOPIC_ORDERS","mess-orders")
GROUP_ID     = os.getenv("KAFKA_GROUP_ID","mess-predictor-group")
HDFS_NN      = os.getenv("HDFS_NAMENODE","hdfs://localhost:9000")
HDFS_BASE    = os.getenv("HDFS_BASE_PATH","/mess-predictor")
RAW_PATH     = f"{HDFS_NN}{HDFS_BASE}/raw/orders"
STAGING      = Path("/tmp/mess_staging")
FLUSH_EVERY  = 500
STAGING.mkdir(parents=True,exist_ok=True)

SCHEMA = StructType([
    StructField("event_id",StringType(),True),
    StructField("student_id",StringType(),True),
    StructField("timestamp",StringType(),True),
    StructField("meal_slot",StringType(),True),
    StructField("food_items",ArrayType(StringType()),True),
    StructField("quantities",MapType(StringType(),IntegerType()),True),
    StructField("hostel_block",StringType(),True),
    StructField("day_type",StringType(),True),
    StructField("swipe_count",IntegerType(),True),
    StructField("source",StringType(),True),
])

def build_spark():
    return (SparkSession.builder.appName("MessHDFSConsumer")
            .master("local[2]")
            .config("spark.driver.memory","1g")
            .config("spark.hadoop.fs.defaultFS",HDFS_NN)
            .getOrCreate())

def flush(spark,records):
    now=datetime.now()
    path=f"{RAW_PATH}/year={now.year}/month={now.month:02d}/day={now.day:02d}"
    try:
        spark.createDataFrame(records,schema=SCHEMA).write.mode("append").parquet(path)
        logger.info(f"Flushed {len(records)} records → {path}"); return True
    except Exception as e:
        logger.error(f"HDFS write failed:{e}")
        fb=STAGING/f"fallback_{now.strftime('%Y%m%d_%H%M%S_%f')}.json"
        with open(fb,"w") as fh:
            for r in records: fh.write(json.dumps(r)+"\n")
        logger.warning(f"Fallback→{fb}"); return False

def run():
    spark=build_spark()
    for n in range(1,7):
        try:
            consumer=KafkaConsumer(TOPIC,bootstrap_servers=BS,group_id=GROUP_ID,
                auto_offset_reset="earliest",enable_auto_commit=False,
                value_deserializer=lambda m:json.loads(m.decode()),max_poll_records=200)
            logger.info(f"Consumer connected → [{TOPIC}]"); break
        except NoBrokersAvailable:
            logger.warning(f"Retry {n}/6..."); time.sleep(5)
    else:
        logger.error("Cannot connect"); sys.exit(1)

    buf=[]; total=0
    try:
        for msg in consumer:
            r=msg.value
            r.setdefault("swipe_count",1); r.setdefault("source","rfid_terminal")
            if not isinstance(r.get("food_items"),list): r["food_items"]=[]
            r["quantities"]={str(k):int(v) for k,v in r.get("quantities",{}).items()}
            buf.append(r); total+=1
            if len(buf)>=FLUSH_EVERY:
                flush(spark,buf); consumer.commit(); buf.clear()
                logger.info(f"Total processed:{total:,}")
    except KeyboardInterrupt:
        if buf: flush(spark,buf); consumer.commit()
        logger.info(f"Stopped. Total:{total:,}")
    finally:
        consumer.close(); spark.stop()

if __name__=="__main__": run()