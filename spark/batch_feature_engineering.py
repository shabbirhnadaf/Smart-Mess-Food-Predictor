"""
Batch Feature Engineering — reads CSV from HDFS, writes Parquet features back to HDFS
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from dotenv import load_dotenv

load_dotenv()

HDFS        = os.getenv("HDFS_NAMENODE", "file:///c:/mess-predictor/data/hdfs_mock")
INPUT_CSV   = f"file:///c:/mess-predictor/data/seed/historical_mess_data.csv"
OUTPUT_PATH = f"{HDFS}/processed/features"

def build_spark():
    return (SparkSession.builder
            .appName("MessFeatureEngineering")
            .config("spark.hadoop.fs.defaultFS", HDFS)
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate())

def load(spark):
    print(f"=== Reading CSV from HDFS: {INPUT_CSV} ===")
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(INPUT_CSV))
    print(f"Loaded {df.count()} rows, {len(df.columns)} columns")
    df.printSchema()
    return df

def engineer(df):
    print("=== Engineering Features ===")

    # Cast types cleanly
    df = (df
          .withColumn("date",               F.to_date("date"))
          .withColumn("quantity_consumed",  F.col("quantity_consumed").cast("int"))
          .withColumn("quantity_prepared",  F.col("quantity_prepared").cast("int"))
          .withColumn("waste_generated",    F.col("waste_generated").cast("int"))
          .withColumn("cost_per_portion",   F.col("cost_per_portion").cast("double"))
          .withColumn("hostel_count",       F.col("hostel_count").cast("int"))
          .withColumn("weather_temp",       F.col("weather_temp").cast("double"))
          .withColumn("is_holiday",         F.col("is_holiday").cast("int"))
          .withColumn("is_exam_week",       F.col("is_exam_week").cast("int"))
          .withColumn("day_of_week",        F.col("day_of_week").cast("int"))
          .withColumn("month",              F.col("month").cast("int"))
          .withColumn("week_of_year",       F.col("week_of_year").cast("int")))

    # Cyclic day-of-week encoding (captures circular nature Mon→Sun)
    df = (df
          .withColumn("dow_sin", F.sin(2 * 3.14159 * F.col("day_of_week") / 7))
          .withColumn("dow_cos", F.cos(2 * 3.14159 * F.col("day_of_week") / 7))
          .withColumn("month_sin", F.sin(2 * 3.14159 * F.col("month") / 12))
          .withColumn("month_cos", F.cos(2 * 3.14159 * F.col("month") / 12)))

    # Hostel occupancy rate (0.0 – 1.0)
    df = df.withColumn("occupancy_rate", F.col("hostel_count") / 500.0)

    # Event impact score
    df = df.withColumn("event_impact_score",
          F.when(F.col("event_name") == "cultural_fest", 1.30)
          .when(F.col("event_name") == "sports_day",     1.20)
          .when(F.col("event_name") == "fresher_party",  1.40)
          .when(F.col("event_name") == "convocation",    1.25)
          .when(F.col("event_name") == "exam_week",      0.95)
          .when(F.col("event_name") == "holiday",        0.60)
          .otherwise(1.00))

    # 7-day rolling average demand per food_item + meal_slot
    window_7d = (Window
                 .partitionBy("food_item", "meal_slot")
                 .orderBy(F.col("date").cast("timestamp").cast("long"))
                 .rowsBetween(-6, 0))
    df = df.withColumn("rolling_7d_avg", F.avg("quantity_consumed").over(window_7d))
    df = df.withColumn("rolling_7d_waste_avg", F.avg("waste_generated").over(window_7d))
    
    # Waste percentage (to avoid division by zero)
    df = df.withColumn("waste_percentage", 
                       F.when(F.col("quantity_prepared") > 0, 
                              (F.col("waste_generated") / F.col("quantity_prepared")) * 100)
                        .otherwise(0.0))

    # Weekend flag
    df = df.withColumn("is_weekend", F.when(F.col("day_of_week") >= 5, 1).otherwise(0))

    # Meal slot encoding
    df = (df
          .withColumn("slot_breakfast", F.when(F.col("meal_slot") == "breakfast", 1).otherwise(0))
          .withColumn("slot_lunch",     F.when(F.col("meal_slot") == "lunch",     1).otherwise(0))
          .withColumn("slot_dinner",    F.when(F.col("meal_slot") == "dinner",    1).otherwise(0)))

    print(f"Feature engineering done. Total columns: {len(df.columns)}")
    return df

def save(df):
    print(f"=== Writing Parquet to HDFS: {OUTPUT_PATH} ===")
    (df.repartition(4)
       .write
       .mode("overwrite")
       .option("compression", "snappy")
       .partitionBy("meal_slot")
       .parquet(OUTPUT_PATH))
    print("=== Batch Feature Engineering COMPLETE ===")

if __name__ == "__main__":
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    df_raw  = load(spark)
    df_feat = engineer(df_raw)
    save(df_feat)
    spark.stop()