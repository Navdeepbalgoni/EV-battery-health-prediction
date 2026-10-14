"""
EV Guardian: Big Data Analytics-Based EV Health Monitoring and Fault Prediction System
Script 03: Feature Engineering & Domain Metrics Calculation

Description:
    Loads the cleaned EV battery telemetry dataset from data/cleaned/cleaned_telemetry.parquet,
    derives physical electro-chemical and electrical battery features (power, capacity loss,
    retention ratio, thermal deviation, cycle aging intensity, resistance normalized to capacity,
    and cell imbalance percentage), and writes the enriched feature dataset to data/output.

Input:
    Local: data/cleaned/cleaned_telemetry.parquet

Output:
    Local: data/output/engineered_features.parquet
"""

import sys
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round as spark_round


def create_spark_session():
    """
    Initializes a memory-calibrated SparkSession suitable for WSL2 (3.7 GB RAM limit).
    """
    hadoop_conf = os.environ.get("HADOOP_CONF_DIR", "/home/asus/hadoop/etc/hadoop")
    if os.path.exists(hadoop_conf):
        os.environ["HADOOP_CONF_DIR"] = hadoop_conf

    spark = (
        SparkSession.builder
        .appName("EV_Guardian_03_Feature_Engineering")
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
    print(" EV GUARDIAN - SCRIPT 03: FEATURE ENGINEERING")
    print("=" * 80)

    spark = create_spark_session()

    input_path = "/mnt/c/bdaproject/EV_Guardian_BDA/evguardian/data/cleaned/cleaned_telemetry.parquet"
    output_path = "/mnt/c/bdaproject/EV_Guardian_BDA/evguardian/data/output/engineered_features.parquet"

    # 1. Load Cleaned Dataset
    print(f"[INFO] Reading cleaned dataset from: {input_path}")
    if not os.path.exists(input_path):
        print(f"[ERROR] Input dataset path {input_path} does not exist. Run 02_preprocessing.py first.")
        spark.stop()
        sys.exit(1)

    cleaned_df = spark.read.parquet(input_path)
    print(f"[SUCCESS] Loaded cleaned telemetry dataset ({cleaned_df.count():,} records)\n")

    # 2. Derive Physical & Electrical Engineering Features
    print("-" * 80)
    print("[INFO] Deriving physical battery engineering features...")
    print("-" * 80)

    # 2.1 Electrical Power in Kilowatts (P = V * I / 1000)
    # Negative during discharge, positive during charge
    df_features = cleaned_df.withColumn(
        "Battery_Power_kW",
        spark_round((col("Battery_Voltage") * col("Battery_Current")) / 1000.0, 4)
    )

    # 2.2 Capacity Loss in Watt-hours (Design Capacity - Current Capacity)
    df_features = df_features.withColumn(
        "Capacity_Loss_Wh",
        spark_round(col("Battery_Design_Capacity_Wh") - col("Battery_Current_Capacity_Wh"), 4)
    )

    # 2.3 Capacity Retention Ratio (Current Capacity / Design Capacity)
    df_features = df_features.withColumn(
        "Capacity_Retention_Ratio",
        spark_round(col("Battery_Current_Capacity_Wh") / col("Battery_Design_Capacity_Wh"), 6)
    )

    # 2.4 Thermal Delta from Nominal Ambient (25°C)
    df_features = df_features.withColumn(
        "Temp_Delta_From_Ambient",
        spark_round(col("Battery_Temperature") - 25.0, 4)
    )

    # 2.5 Operational Aging Intensity: Cumulative Charge Cycles per Vehicle Age Year
    df_features = df_features.withColumn(
        "Cycles_Per_Year",
        spark_round(
            col("Charge_Cycles") / when(col("Vehicle_Age_Years") > 0.0, col("Vehicle_Age_Years")).otherwise(1.0),
            2
        )
    )

    # 2.6 Resistance Normalized to Usable Energy Capacity (mΩ / Wh)
    df_features = df_features.withColumn(
        "Resistance_Per_Capacity",
        spark_round(
            col("Battery_Internal_Resistance") / col("Battery_Current_Capacity_Wh"),
            8
        )
    )

    # 2.7 Cell Imbalance Percentage Relative to Pack Voltage (V_imbalance / V_pack * 100)
    df_features = df_features.withColumn(
        "Cell_Imbalance_Pct",
        spark_round(
            (col("Battery_Cell_Imbalance") / col("Battery_Voltage")) * 100.0,
            6
        )
    )

    # 3. Stream Enriched Parquet Dataset to data/output
    print(f"[INFO] Writing engineered features to: {output_path}")
    df_features.write.mode("overwrite").parquet(output_path)
    print(f"[SUCCESS] Successfully saved feature dataset to: {output_path}\n")

    # 4. Post-Write Verification & Summary
    print("-" * 80)
    print("[INFO] Verifying saved feature dataset...")
    saved_features_df = spark.read.parquet(output_path)
    total_saved = saved_features_df.count()

    print("\nENGINEERED DATASET SCHEMA:")
    saved_features_df.printSchema()

    print("\nSAMPLE RECORDS WITH ENGINEERED FEATURES (TOP 5):")
    sample_cols = [
        "Vehicle_ID",
        "Vehicle_Type",
        "Battery_Power_kW",
        "Capacity_Loss_Wh",
        "Capacity_Retention_Ratio",
        "Temp_Delta_From_Ambient",
        "Cycles_Per_Year",
        "Resistance_Per_Capacity",
        "Cell_Imbalance_Pct"
    ]
    saved_features_df.select(sample_cols).show(5, truncate=False)

    print("\nSUMMARY STATISTICS FOR ENGINEERED FEATURES:")
    engineered_metric_cols = [
        "Battery_Power_kW",
        "Capacity_Loss_Wh",
        "Capacity_Retention_Ratio",
        "Temp_Delta_From_Ambient",
        "Cycles_Per_Year",
        "Resistance_Per_Capacity",
        "Cell_Imbalance_Pct"
    ]
    saved_features_df.select(engineered_metric_cols).describe().show()

    elapsed = time.time() - start_time
    print("-" * 80)
    print("FEATURE ENGINEERING EXECUTION SUMMARY:")
    print("-" * 80)
    print(f"Total Enriched Records : {total_saved:,}")
    print(f"Total Columns          : {len(saved_features_df.columns)}")
    print(f"Destination Path       : {output_path}")
    print(f"Execution Time         : {elapsed:.2f} seconds")
    print("=" * 80)
    print(" SCRIPT 03: FEATURE ENGINEERING COMPLETED SUCCESSFULLY")
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()
