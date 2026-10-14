"""
EV Guardian: Big Data Analytics-Based EV Health Monitoring and Fault Prediction System
Script 01: Raw Dataset Inspection & Schema Validation

Description:
    Reads the raw EV battery telemetry dataset directly from HDFS using PySpark,
    inspects the schema, verifies data types, computes total record and column counts,
    identifies null/missing values, and generates baseline summary statistics for
    all physical battery parameters.

HDFS Input Path:
    hdfs://localhost:9000/evguardian/raw/bda_dataset_1gb.csv
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan


def create_spark_session():
    """
    Initializes and returns a configured SparkSession compatible with WSL Hadoop/HDFS.
    """
    # Ensure HADOOP_CONF_DIR is recognized if available
    hadoop_conf = os.environ.get("HADOOP_CONF_DIR", "/home/asus/hadoop/etc/hadoop")
    if os.path.exists(hadoop_conf):
        os.environ["HADOOP_CONF_DIR"] = hadoop_conf

    spark = (
        SparkSession.builder
        .appName("EV_Guardian_01_Inspection")
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    print("=" * 80)
    print(" EV GUARDIAN - SCRIPT 01: DATASET INSPECTION & SCHEMA VALIDATION")
    print("=" * 80)

    # 1. Initialize SparkSession
    spark = create_spark_session()

    # Define primary HDFS path and local fallback path for portability
    hdfs_path = "hdfs://localhost:9000/evguardian/raw/bda_dataset_1gb.csv"
    alt_hdfs_path = "hdfs:///evguardian/raw/bda_dataset_1gb.csv"
    local_path = "/mnt/c/bdaproject/EV_Guardian_BDA/evguardian/data/raw/bda_dataset_1gb.csv"

    # 2. Load Raw Telemetry Dataset
    df = None
    for path in [hdfs_path, alt_hdfs_path, local_path]:
        try:
            print(f"[INFO] Attempting to load dataset from: {path}")
            df = (
                spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(path)
            )
            print(f"[SUCCESS] Successfully loaded dataset from: {path}\n")
            break
        except Exception as e:
            print(f"[WARN] Failed to load from {path}: {e}")

    if df is None:
        print("[ERROR] Could not load dataset from any designated path. Exiting.")
        spark.stop()
        sys.exit(1)

    # 3. Schema Inspection
    print("-" * 80)
    print("1. DATASET SCHEMA & DATA TYPES")
    print("-" * 80)
    df.printSchema()

    # 4. Total Row and Column Counts
    print("-" * 80)
    print("2. DATASET DIMENSIONS")
    print("-" * 80)
    total_columns = len(df.columns)
    total_rows = df.count()
    print(f"Total Columns : {total_columns}")
    print(f"Total Records : {total_rows:,}\n")

    # 5. Display First 5 Sample Records
    print("-" * 80)
    print("3. SAMPLE RECORDS (FIRST 5 ROWS)")
    print("-" * 80)
    df.show(5, truncate=False)

    # 6. Missing / Null / NaN Value Check across All Columns
    print("-" * 80)
    print("4. DATA QUALITY & NULL VALUE CHECK")
    print("-" * 80)
    null_exprs = []
    for c in df.columns:
        # Check for both NULL and NaN for floating/double types
        data_type = dict(df.dtypes)[c]
        if data_type in ("double", "float"):
            expr = count(when(col(c).isNull() | isnan(col(c)), c)).alias(c)
        else:
            expr = count(when(col(c).isNull(), c)).alias(c)
        null_exprs.append(expr)

    null_counts_df = df.select(null_exprs)
    null_counts_df.show(vertical=True)

    # 7. Categorical Attribute Value Counts
    print("-" * 80)
    print("5. CATEGORICAL ATTRIBUTE DISTRIBUTIONS")
    print("-" * 80)
    for cat_col in ["Vehicle_Type", "Charging_Status", "Charging_Type"]:
        print(f"\n--- Distribution for: {cat_col} ---")
        df.groupBy(cat_col).count().orderBy(col("count").desc()).show()

    # 8. Numeric Telemetry Summary Statistics
    print("-" * 80)
    print("6. SUMMARY STATISTICS FOR NUMERICAL BATTERY TELEMETRY")
    print("-" * 80)
    numeric_cols = [
        "Vehicle_Age_Years",
        "Battery_SOC",
        "Battery_SOH",
        "Battery_Design_Capacity_Wh",
        "Battery_Current_Capacity_Wh",
        "Battery_Voltage",
        "Battery_Current",
        "Battery_Temperature",
        "Battery_Internal_Resistance",
        "Battery_Cell_Imbalance",
        "Charge_Cycles"
    ]
    df.select(numeric_cols).describe().show()

    print("=" * 80)
    print(" SCRIPT 01: INSPECTION COMPLETED SUCCESSFULLY")
    print("=" * 80)

    # Clean shutdown of SparkSession
    spark.stop()


if __name__ == "__main__":
    main()
