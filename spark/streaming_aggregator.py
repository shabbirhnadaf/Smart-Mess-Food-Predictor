"""
Mess Demand Streaming Aggregator - Stable Windows version
Uses foreachBatch (no watermark/window crash) and writes clean parquet to local + HDFS
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, count, sum as _sum,
    explode, expr, to_timestamp,
    lit, current_timestamp, when, greatest, ceil
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, ArrayType, MapType, TimestampType
)
import uuid, os

KAFKA_BROKER    = "localhost:9092"
KAFKA_TOPIC     = "mess-orders"
HDFS_BASE       = "file:///C:/mess-predictor/data/hdfs_mock/mess-predictor"
LOCAL_OUT       = "C:/mess-predictor/data/streaming_output"
LOCAL_DEMAND_DIR = "demand_agg_live"
LOCAL_HOSTEL_DIR = "hostel_agg"
LOCAL_CHKPT     = "file:///C:/mess-predictor/tmp/checkpoints"
HDFS_CHKPT      = HDFS_BASE + "/checkpoints"

ORDER_SCHEMA = StructType([
    StructField("event_id",     StringType(),  True),
    StructField("student_id",   StringType(),  True),
    StructField("timestamp",    StringType(),  True),
    StructField("meal_slot",    StringType(),  True),
    StructField("food_items",   ArrayType(StringType()), True),
    StructField("quantities",   MapType(StringType(), IntegerType()), True),
    StructField("hostel_block", StringType(),  True),
    StructField("day_type",     StringType(),  True),
    StructField("swipe_count",  IntegerType(), True),
    StructField("source",       StringType(),  True),
])


def create_spark():
    return (
        SparkSession.builder
        .appName("MessDemandAggregator")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions",              "2")
        .config("spark.streaming.stopGracefullyOnShutdown",  "true")
        .config("spark.sql.adaptive.enabled",                "false")
        .config("spark.hadoop.fs.defaultFS",                 "file:///C:/mess-predictor/data/hdfs_mock")
        .config("spark.local.dir",                           "C:/mess-predictor/tmp/spark-local")
        .config("spark.driver.memory",                       "1500m")
        .config("spark.driver.maxResultSize",                "512m")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.ui.enabled",                          "true")
        .config("spark.network.timeout",                     "800s")
        .config("spark.executor.heartbeatInterval",          "60s")
        .getOrCreate()
    )


def process_batch(batch_df: DataFrame, batch_id: int):
    """Called every 30s with micro-batch. Aggregates + writes parquet."""
    if batch_df.isEmpty():
        print(f"[Batch {batch_id}] Empty batch - waiting for Kafka events...")
        return

    # Parse JSON
    parsed = (
        batch_df
        .selectExpr("CAST(value AS STRING) as json_str")
        .withColumn("data", from_json(col("json_str"), ORDER_SCHEMA))
        .select(
            col("data.event_id"),
            col("data.student_id"),
            to_timestamp(col("data.timestamp")).alias("event_time"),
            col("data.meal_slot"),
            col("data.food_items"),
            col("data.quantities"),
            col("data.hostel_block"),
            col("data.day_type"),
        )
        .filter(col("meal_slot").isNotNull())
        .filter(col("event_time").isNotNull())
    )

    # Explode food items
    exploded = (
        parsed
        .withColumn("food_item", explode(col("food_items")))
        .withColumn("quantity",  expr("coalesce(quantities[food_item], 1)"))
    )

    # Aggregate demand per meal_slot + food_item
    agg = (
        exploded
        .groupBy("meal_slot", "food_item", "day_type")
        .agg(
            count("event_id").alias("order_count"),
            _sum("quantity").alias("total_quantity"),
            count("student_id").alias("unique_students"),
        )
        # Derive a prepared quantity from live ordering behavior instead of fixed ratios.
        .withColumn(
            "avg_portions_per_order",
            col("total_quantity") / greatest(col("order_count"), lit(1))
        )
        .withColumn(
            "behavior_buffer_ratio",
            when(col("avg_portions_per_order") <= 1.35, lit(0.24))
            .when(col("avg_portions_per_order") <= 1.8, lit(0.17))
            .otherwise(lit(0.11))
        )
        .withColumn(
            "slot_buffer_ratio",
            when(col("meal_slot") == "lunch", lit(0.12))
            .when(col("meal_slot") == "dinner", lit(0.09))
            .otherwise(lit(0.07))
        )
        .withColumn(
            "day_buffer_ratio",
            when(col("day_type") == "holiday", lit(-0.05))
            .when(col("day_type") == "weekend", lit(-0.02))
            .otherwise(lit(0.0))
        )
        # Small deterministic variation by batch to keep feed from becoming static.
        .withColumn("batch_variation_ratio", lit(((batch_id % 7) - 3) * 0.012))
        .withColumn(
            "prepared_ratio",
            lit(1.0) + col("behavior_buffer_ratio") + col("slot_buffer_ratio") + col("day_buffer_ratio") + col("batch_variation_ratio")
        )
        .withColumn("prepared_ratio", greatest(col("prepared_ratio"), lit(1.02)))
        .withColumn("total_prepared", ceil(col("total_quantity") * col("prepared_ratio")).cast("int"))
        .withColumn("batch_id",    lit(batch_id))
        .withColumn("window_start", current_timestamp())
    )

    row_count = agg.count()
    print(f"[Batch {batch_id}] OK {row_count} food-item aggregations from {parsed.count()} events")
    agg.show(10, truncate=False)

    # Aggregate hostel breakdown for dashboard
    hostel_agg = (
        exploded
        .groupBy("hostel_block", "meal_slot", "food_item")
        .agg(
            count("event_id").alias("hostel_order_count"),
            _sum("quantity").alias("hostel_total_quantity"),
            count("student_id").alias("hostel_unique_students"),
        )
        .withColumn("batch_id", lit(batch_id))
        .withColumn("window_start", current_timestamp())
    )

    # Write to LOCAL disk (primary - FastAPI reads this)
    local_path = f"file:///{LOCAL_OUT}/{LOCAL_DEMAND_DIR}"
    (agg.coalesce(1)
       .write
       .mode("append")
       .parquet(local_path))
    print(f"[Batch {batch_id}] Written to local: {LOCAL_OUT}/{LOCAL_DEMAND_DIR}/")

    hostel_local_path = f"file:///{LOCAL_OUT}/{LOCAL_HOSTEL_DIR}"
    (hostel_agg.coalesce(1)
       .write
       .mode("append")
       .parquet(hostel_local_path))
    print(f"[Batch {batch_id}] Written hostel aggregates to local: {LOCAL_OUT}/{LOCAL_HOSTEL_DIR}/")

    # Write to HDFS (secondary - Big Data storage)
    hdfs_path = HDFS_BASE + "/processed/streaming_agg/demand_agg"
    hdfs_hostel_path = HDFS_BASE + "/processed/streaming_agg/hostel_agg"
    try:
        (agg.coalesce(1)
           .write
           .mode("append")
           .parquet(hdfs_path))
        print(f"[Batch {batch_id}] Written to HDFS: {hdfs_path}")

        (hostel_agg.coalesce(1)
           .write
           .mode("append")
           .parquet(hdfs_hostel_path))
        print(f"[Batch {batch_id}] Written hostel aggregates to HDFS: {hdfs_hostel_path}")
    except Exception as e:
        print(f"[Batch {batch_id}] WARNING HDFS write skipped: {e}")


def run():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 65)
    print("  Smart Mess Food Demand Predictor - Streaming Aggregator")
    print(f"  Kafka  : {KAFKA_BROKER}  ->  topic: {KAFKA_TOPIC}")
    print(f"  Local  : {LOCAL_OUT}/{LOCAL_DEMAND_DIR}/")
    print(f"  HDFS   : {HDFS_BASE}/processed/streaming_agg/demand_agg/")
    print(f"  Spark UI: http://localhost:4040")
    print("=" * 65)

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",     KAFKA_BROKER)
        .option("kafka.broker.address.family", "v4")
        .option("subscribe",                   KAFKA_TOPIC)
        .option("startingOffsets",             "earliest")   # pick up ALL existing events
        .option("failOnDataLoss",              "false")
        .option("maxOffsetsPerTrigger",        "500")
        .load()
    )

    query = (
        raw.writeStream
        .queryName("mess_demand_aggregator")
        .foreachBatch(process_batch)
        .option("checkpointLocation", LOCAL_CHKPT + "/foreachbatch_v2")
        .trigger(processingTime="30 seconds")
        .start()
    )

    print("Streaming query started (foreachBatch mode)")
    print("   First batch in ~30s | Spark UI -> http://localhost:4040\n")
    query.awaitTermination()


if __name__ == "__main__":
    run()
