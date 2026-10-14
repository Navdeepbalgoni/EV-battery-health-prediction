"""
EV Guardian: Big Data Analytics-Based EV Health Monitoring and Fault Prediction System
Script 02: Data Preprocessing & Cleaning

Description:
    Reads raw EV battery telemetry from HDFS, standardizes timestamps into Spark
    TimestampType, cleans and casts all numeric attributes, enforces physical domain
    constraints, and streams the cleaned dataset directly into the local data/cleaned folder
    in Parquet format. Optimized for memory-efficient execution on WSL2.

Input:
    HDFS: hdfs://localhost:9000/evguardian/raw/bda_dataset_1gb.csv

Output:
    Local: data/cleaned/cleaned_telemetry.parquet
"""

import sys
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_timestamp
from pyspark.sql.types import DoubleType, IntegerType


def create_spark_session():
    """
    Initializes a memory-calibrated SparkSession suitable for WSL2 (3.7 GB RAM limit).
    """
    hadoop_conf = os.environ.get("HADOOP_CONF_DIR", "/home/asus/hadoop/etc/hadoop")
    if os.path.exists(hadoop_conf):
        os.environ["HADOOP_CONF_DIR"] = hadoop_conf

    spark = (
        SparkSession.builder
        .appName("EV_Guardian_02_Preprocessing")
        .master("local[4]")
        .config("spark.driver.memory", "1536m")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .config("spark.memory.fraction", "0.6")
        .config("spark.memory.storageFraction", "0.2")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    start_time = time.time()
    print("=" * 80)
    print(" EV GUARDIAN - SCRIPT 02: DATA PREPROCESSING & CLEANING")
    print("=" * 80)

    spark = create_spark_session()

    # 1. Dataset Paths
    hdfs_raw_path = "hdfs://localhost:9000/evguardian/raw/bda_dataset_1gb.csv"
    alt_hdfs_path = "hdfs:///evguardian/raw/bda_dataset_1gb.csv"
    local_raw_path = "/mnt/c/bdaproject/EV_Guardian_BDA/evguardian/data/raw/bda_dataset_1gb.csv"

    local_cleaned_output = "/mnt/c/bdaproject/EV_Guardian_BDA/evguardian/data/cleaned/cleaned_telemetry.parquet"

    # 2. Load Raw Telemetry (Streaming scan from HDFS)
    raw_df = None
    for path in [hdfs_raw_path, alt_hdfs_path, local_raw_path]:
        try:
            print(f"[INFO] Reading raw dataset from: {path}")
            raw_df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
            print(f"[SUCCESS] Successfully loaded raw data from: {path}\n")
            break
        except Exception as e:
            print(f"[WARN] Could not load from {path}: {e}")

    if raw_df is None:
        print("[ERROR] Failed to load raw dataset. Exiting.")
        spark.stop()
        sys.exit(1)

    # 3. Timestamp Parsing & Formatting
    print("-" * 80)
    print("[INFO] Parsing 'Timestamp' into Spark TimestampType (M/d/yyyy H:mm)...")
    cleaned_df = raw_df.withColumn(
        "Timestamp",
        to_timestamp(trim(col("Timestamp")), "M/d/yyyy H:mm")
    )

    # 4. String Cleaning (Whitespace trimming)
    string_cols = ["Vehicle_ID", "Vehicle_Type", "Charging_Status", "Charging_Type"]
    for s_col in string_cols:
        cleaned_df = cleaned_df.withColumn(s_col, trim(col(s_col)))

    # 5. Type Casting
    numeric_double_cols = [
        "Vehicle_Age_Years",
        "Battery_SOC",
        "Battery_SOH",
        "Battery_Design_Capacity_Wh",
        "Battery_Current_Capacity_Wh",
        "Battery_Voltage",
        "Battery_Current",
        "Battery_Temperature",
        "Battery_Internal_Resistance",
        "Battery_Cell_Imbalance"
    ]
    for d_col in numeric_double_cols:
        cleaned_df = cleaned_df.withColumn(d_col, col(d_col).cast(DoubleType()))

    cleaned_df = cleaned_df.withColumn("Charge_Cycles", col("Charge_Cycles").cast(IntegerType()))

    # 6. Physical Domain Constraint Filtering
    # Validating realistic telemetry bounds observed in EV battery packs
    print("[INFO] Enforcing physical integrity constraints...")
    valid_conditions = (
        col("Timestamp").isNotNull() &
        (col("Battery_SOC") >= 0.0) & (col("Battery_SOC") <= 110.0) &
        (col("Battery_SOH") >= 0.0) & (col("Battery_SOH") <= 110.0) &
        (col("Battery_Voltage") > 0.0) &
        (col("Battery_Design_Capacity_Wh") > 0.0) &
        (col("Battery_Current_Capacity_Wh") > 0.0) &
        (col("Battery_Internal_Resistance") > 0.0) &
        (col("Charge_Cycles") >= 0) &
        col("Vehicle_Type").isin(["Scooter", "Motorcycle", "Car"]) &
        col("Charging_Status").isin(["Discharging", "Charging", "Idle"]) &
        col("Charging_Type").isin(["Slow", "Fast"])
    )
    cleaned_df = cleaned_df.filter(valid_conditions)

    # 7. Write Cleaned Dataset to data/cleaned in Parquet format
    print("-" * 80)
    print(f"[INFO] Streaming cleaned dataset to Parquet at: {local_cleaned_output}")
    cleaned_df.write.mode("overwrite").parquet(local_cleaned_output)
    print(f"[SUCCESS] Successfully written to: {local_cleaned_output}")

    # 8. Post-Write Verification
    print("-" * 80)
    print("[INFO] Verifying saved cleaned dataset...")
    saved_df = spark.read.parquet(local_cleaned_output)
    cleaned_count = saved_df.count()

    print("\nCLEANED DATASET SCHEMA:")
    saved_df.printSchema()

    print("\nSAMPLE CLEANED RECORDS:")
    saved_df.show(5, truncate=False)

    elapsed = time.time() - start_time
    print("-" * 80)
    print("PREPROCESSING EXECUTION SUMMARY:")
    print("-" * 80)
    print(f"Cleaned Records Saved : {cleaned_count:,}")
    print(f"Target Destination    : {local_cleaned_output}")
    print(f"Total Execution Time  : {elapsed:.2f} seconds")
    print("=" * 80)
    print(" SCRIPT 02: PREPROCESSING COMPLETED SUCCESSFULLY")
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()
