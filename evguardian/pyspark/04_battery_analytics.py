"""
EV Guardian: Big Data Analytics-Based EV Health Monitoring and Fault Prediction System
Script 04: Battery Analytics & Advanced Health / Fault Diagnostics

Description:
    Loads the enriched EV battery feature dataset from data/output/engineered_features.parquet,
    applies multi-dimensional battery analytics, calculates electro-chemical health classifications,
    identifies thermal and cell imbalance anomalies, computes a quantitative composite
    Fault Risk Score (0-100), and flags early warning indicators for impending battery faults.
    
    Generates both granular analytics telemetry (Parquet) and aggregated fleet diagnostic
    summaries (CSV) for Hive data warehousing and RStudio visualization scripts.

Input:
    Local: data/output/engineered_features.parquet

Outputs:
    Local Parquet: data/output/battery_analytics.parquet
    Local CSVs:    results/vehicle_health_summary.csv
                   results/vehicle_type_analytics.csv
                   results/charging_impact_analytics.csv
                   results/fault_distribution_analytics.csv
"""

import sys
import os
import time
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, round as spark_round, avg, max as spark_max, min as spark_min,
    count, countDistinct, sum as spark_sum, lit
)


def create_spark_session():
    """
    Initializes a memory-calibrated SparkSession suitable for WSL2 / local development.
    """
    hadoop_conf = os.environ.get("HADOOP_CONF_DIR", "/home/asus/hadoop/etc/hadoop")
    if os.path.exists(hadoop_conf):
        os.environ["HADOOP_CONF_DIR"] = hadoop_conf

    spark = (
        SparkSession.builder
        .appName("EV_Guardian_04_Battery_Analytics")
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


def resolve_existing_path(paths):
    """
    Returns the first path from the list that exists on the current filesystem.
    """
    for p in paths:
        if os.path.exists(p):
            return p
    return paths[0]


def save_single_csv(df, target_file_path, temp_dir):
    """
    Exports a Spark DataFrame as a single clean CSV file with headers.
    """
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir, ignore_errors=True)

    df.coalesce(1).write.option("header", "true").mode("overwrite").csv(temp_dir)

    for fname in os.listdir(temp_dir):
        if fname.startswith("part-") and fname.endswith(".csv"):
            os.makedirs(os.path.dirname(target_file_path), exist_ok=True)
            shutil.copyfile(os.path.join(temp_dir, fname), target_file_path)
            break

    shutil.rmtree(temp_dir, ignore_errors=True)


def main():
    start_time = time.time()
    print("=" * 80)
    print(" EV GUARDIAN - SCRIPT 04: BATTERY ANALYTICS & FAULT DIAGNOSTICS")
    print("=" * 80)

    spark = create_spark_session()

    # 1. Resolve Input and Output Paths (Cross-platform compatibility)
    candidate_input_paths = [
        "/mnt/c/bdaproject/EV_Guardian_BDA/evguardian/data/output/engineered_features.parquet",
        "C:/bdaproject/EV_Guardian_BDA/evguardian/data/output/engineered_features.parquet",
        "data/output/engineered_features.parquet",
        "../data/output/engineered_features.parquet"
    ]
    input_path = resolve_existing_path(candidate_input_paths)

    if os.path.isabs(input_path):
        base_dir = os.path.abspath(os.path.join(os.path.dirname(input_path), "..", ".."))
    else:
        base_dir = os.path.abspath(".")

    output_parquet_path = os.path.join(base_dir, "data", "output", "battery_analytics.parquet")
    results_dir = os.path.join(base_dir, "results")
    os.makedirs(results_dir, exist_ok=True)

    print(f"[INFO] Reading engineered features from: {input_path}")
    if not os.path.exists(input_path):
        print(f"[ERROR] Input dataset path {input_path} does not exist. Run 03_feature_engineering.py first.")
        spark.stop()
        sys.exit(1)

    df_engineered = spark.read.parquet(input_path)
    total_records = df_engineered.count()
    print(f"[SUCCESS] Loaded engineered feature dataset ({total_records:,} records)\n")

    # 2. Compute Battery Health, Degradation, & Anomaly Classifications
    print("-" * 80)
    print("[INFO] Computing battery degradation categories & anomaly diagnostics...")
    print("-" * 80)

    # 2.1 State of Health (SOH) Classification
    # Healthy (>= 85%), Moderate (75-84.9%), Degraded (65-74.9%), Critical (< 65%)
    df_analytics = df_engineered.withColumn(
        "Health_Status",
        when(col("Battery_SOH") >= 85.0, "Healthy")
        .when((col("Battery_SOH") >= 75.0) & (col("Battery_SOH") < 85.0), "Moderate")
        .when((col("Battery_SOH") >= 65.0) & (col("Battery_SOH") < 75.0), "Degraded")
        .otherwise("Critical")
    )

    # 2.2 Thermal Risk Assessment
    # Normal (<= 38 deg C), Elevated (38-45 deg C), High_Overheating (> 45 deg C)
    df_analytics = df_analytics.withColumn(
        "Thermal_Risk",
        when(col("Battery_Temperature") <= 38.0, "Normal")
        .when((col("Battery_Temperature") > 38.0) & (col("Battery_Temperature") <= 45.0), "Elevated")
        .otherwise("High_Overheating")
    )

    # 2.3 Cell Voltage Imbalance Diagnostic (in Volts)
    # Normal (<= 0.025V / 25mV), Moderate_Imbalance (25-50 mV), Severe_Imbalance (> 50 mV)
    df_analytics = df_analytics.withColumn(
        "Cell_Imbalance_Status",
        when(col("Battery_Cell_Imbalance") <= 0.025, "Normal")
        .when((col("Battery_Cell_Imbalance") > 0.025) & (col("Battery_Cell_Imbalance") <= 0.050), "Moderate_Imbalance")
        .otherwise("Severe_Imbalance")
    )

    # 2.4 Internal Resistance Degradation (in mOhm)
    # Normal (<= 40 mOhm), Elevated (40-55 mOhm), High_Resistance (> 55 mOhm)
    df_analytics = df_analytics.withColumn(
        "Resistance_Status",
        when(col("Battery_Internal_Resistance") <= 40.0, "Normal")
        .when((col("Battery_Internal_Resistance") > 40.0) & (col("Battery_Internal_Resistance") <= 55.0), "Elevated")
        .otherwise("High_Resistance")
    )

    # 2.5 Operational Stress Flags
    # Deep Discharge Flag: SOC < 15.0%
    df_analytics = df_analytics.withColumn(
        "Deep_Discharge_Flag",
        when(col("Battery_SOC") < 15.0, 1).otherwise(0)
    )

    # Fast Charge Stress Flag: Fast charging under elevated temperature (>35C) or high power (>10kW)
    df_analytics = df_analytics.withColumn(
        "Fast_Charge_Stress_Flag",
        when(
            (col("Charging_Type") == "Fast") & 
            ((col("Battery_Temperature") > 35.0) | (col("Battery_Power_kW") > 10.0)),
            1
        ).otherwise(0)
    )

    # 2.6 Quantitative Composite Fault Risk Score (0.0 to 100.0)
    # Calibrated to real domain scales:
    # - SOH degradation penalty: up to 40 pts
    # - Thermal stress penalty: up to 25 pts (threshold 32 deg C)
    # - Cell imbalance penalty: up to 20 pts (threshold 0.020V)
    # - Internal resistance penalty: up to 15 pts (threshold 38 mOhm)
    soh_penalty = when(col("Battery_SOH") < 90.0, ((90.0 - col("Battery_SOH")) / 30.0) * 40.0).otherwise(0.0)
    thermal_penalty = when(col("Battery_Temperature") > 32.0, ((col("Battery_Temperature") - 32.0) / 15.0) * 25.0).otherwise(0.0)
    imbalance_penalty = when(col("Battery_Cell_Imbalance") > 0.020, ((col("Battery_Cell_Imbalance") - 0.020) / 0.040) * 20.0).otherwise(0.0)
    resistance_penalty = when(col("Battery_Internal_Resistance") > 38.0, ((col("Battery_Internal_Resistance") - 38.0) / 25.0) * 15.0).otherwise(0.0)

    raw_risk = soh_penalty + thermal_penalty + imbalance_penalty + resistance_penalty

    df_analytics = df_analytics.withColumn(
        "Fault_Risk_Score",
        spark_round(
            when(raw_risk > 100.0, 100.0).when(raw_risk < 0.0, 0.0).otherwise(raw_risk),
            2
        )
    )

    # 2.7 Fault Severity Level & Early Prediction Alert
    # LOW (< 25.0), MEDIUM (25.0-49.9), HIGH (50.0-69.9), CRITICAL (>= 70.0)
    df_analytics = df_analytics.withColumn(
        "Fault_Severity_Level",
        when(col("Fault_Risk_Score") >= 70.0, "CRITICAL")
        .when((col("Fault_Risk_Score") >= 50.0) & (col("Fault_Risk_Score") < 70.0), "HIGH")
        .when((col("Fault_Risk_Score") >= 25.0) & (col("Fault_Risk_Score") < 50.0), "MEDIUM")
        .otherwise("LOW")
    )

    # Early Fault Prediction Flag (Triggers on High/Critical severity, Overheating, or Severe Imbalance)
    df_analytics = df_analytics.withColumn(
        "Early_Fault_Prediction",
        when(
            col("Fault_Severity_Level").isin(["HIGH", "CRITICAL"]) |
            (col("Thermal_Risk") == "High_Overheating") |
            (col("Cell_Imbalance_Status") == "Severe_Imbalance"),
            1
        ).otherwise(0)
    )

    # 3. Stream Granular Analytics Dataset to data/output/battery_analytics.parquet
    print(f"[INFO] Streaming enriched battery analytics dataset to: {output_parquet_path}")
    df_analytics.write.mode("overwrite").parquet(output_parquet_path)
    print(f"[SUCCESS] Successfully saved analytics Parquet dataset\n")

    # 4. Compute Fleet & Cohort Analytical Aggregations
    print("-" * 80)
    print("[INFO] Computing aggregated fleet diagnostic summaries for Hive & RStudio...")
    print("-" * 80)

    # 4.1 Summary A: Individual Vehicle Health & Fault Summary
    vehicle_health_summary = df_analytics.groupBy("Vehicle_ID", "Vehicle_Type", "Vehicle_Age_Years").agg(
        spark_round(avg("Battery_SOH"), 2).alias("Avg_SOH"),
        spark_round(spark_min("Battery_SOH"), 2).alias("Min_SOH"),
        spark_round(avg("Battery_Temperature"), 2).alias("Avg_Temp_C"),
        spark_round(spark_max("Battery_Temperature"), 2).alias("Max_Temp_C"),
        spark_round(avg("Battery_Internal_Resistance"), 2).alias("Avg_Resistance_mOhm"),
        spark_round(spark_max("Battery_Cell_Imbalance") * 1000.0, 2).alias("Max_Cell_Imbalance_mV"),
        spark_max("Charge_Cycles").alias("Max_Charge_Cycles"),
        spark_round(avg("Fault_Risk_Score"), 2).alias("Avg_Risk_Score"),
        spark_round(spark_max("Fault_Risk_Score"), 2).alias("Max_Risk_Score"),
        spark_sum("Early_Fault_Prediction").alias("Fault_Alert_Count"),
        count("*").alias("Total_Readings")
    ).withColumn(
        "Fleet_Health_Status",
        when(col("Avg_SOH") >= 85.0, "Healthy")
        .when((col("Avg_SOH") >= 75.0) & (col("Avg_SOH") < 85.0), "Moderate")
        .when((col("Avg_SOH") >= 65.0) & (col("Avg_SOH") < 75.0), "Degraded")
        .otherwise("Critical")
    ).orderBy(col("Avg_Risk_Score").desc())

    # 4.2 Summary B: Vehicle Type Degradation Profile
    vehicle_type_analytics = df_analytics.groupBy("Vehicle_Type").agg(
        count("*").alias("Total_Telemetry_Readings"),
        countDistinct("Vehicle_ID").alias("Distinct_Vehicles"),
        spark_round(avg("Battery_SOH"), 2).alias("Avg_SOH"),
        spark_round(avg("Capacity_Loss_Wh"), 2).alias("Avg_Capacity_Loss_Wh"),
        spark_round(avg("Cycles_Per_Year"), 2).alias("Avg_Cycles_Per_Year"),
        spark_round(avg("Battery_Temperature"), 2).alias("Avg_Temperature_C"),
        spark_round(avg("Battery_Internal_Resistance"), 2).alias("Avg_Resistance_mOhm"),
        spark_round(avg("Fault_Risk_Score"), 2).alias("Avg_Risk_Score"),
        spark_sum("Early_Fault_Prediction").alias("Total_Fault_Alerts")
    ).orderBy(col("Avg_Risk_Score").desc())

    # 4.3 Summary C: Charging Protocol & Degradation Impact
    charging_impact_analytics = df_analytics.groupBy("Charging_Status", "Charging_Type").agg(
        count("*").alias("Reading_Count"),
        spark_round(avg("Battery_Power_kW"), 3).alias("Avg_Power_kW"),
        spark_round(avg("Battery_Temperature"), 2).alias("Avg_Temperature_C"),
        spark_round(spark_max("Battery_Temperature"), 2).alias("Max_Temperature_C"),
        spark_round(avg("Battery_Internal_Resistance"), 2).alias("Avg_Resistance_mOhm"),
        spark_round(avg("Battery_SOH"), 2).alias("Avg_SOH"),
        spark_sum("Fast_Charge_Stress_Flag").alias("Fast_Charge_Stress_Count"),
        spark_round((count(when(col("Battery_Temperature") > 35.0, 1)) / count("*")) * 100.0, 2).alias("High_Temp_Reading_Pct")
    ).orderBy(col("Reading_Count").desc())

    # 4.4 Summary D: Risk & Severity Distribution
    fault_distribution_analytics = df_analytics.groupBy("Health_Status", "Fault_Severity_Level").agg(
        count("*").alias("Telemetry_Points"),
        spark_round(avg("Battery_SOH"), 2).alias("Avg_SOH"),
        spark_round(avg("Battery_Temperature"), 2).alias("Avg_Temperature_C"),
        spark_round(avg("Battery_Cell_Imbalance") * 1000.0, 2).alias("Avg_Imbalance_mV"),
        spark_round(avg("Fault_Risk_Score"), 2).alias("Avg_Risk_Score")
    ).orderBy(col("Avg_Risk_Score").desc())

    # 5. Export Summary CSVs to results/ for Hive & RStudio
    print("[INFO] Exporting summary analytical tables to results/...")
    temp_csv_dir = os.path.join(results_dir, "_temp_spark_csv")

    save_single_csv(vehicle_health_summary, os.path.join(results_dir, "vehicle_health_summary.csv"), temp_csv_dir)
    save_single_csv(vehicle_type_analytics, os.path.join(results_dir, "vehicle_type_analytics.csv"), temp_csv_dir)
    save_single_csv(charging_impact_analytics, os.path.join(results_dir, "charging_impact_analytics.csv"), temp_csv_dir)
    save_single_csv(fault_distribution_analytics, os.path.join(results_dir, "fault_distribution_analytics.csv"), temp_csv_dir)

    print("[SUCCESS] All analytical CSV files exported successfully to results/\n")

    # 6. Post-Run Diagnostics & Verification Output
    print("-" * 80)
    print("1. BATTERY HEALTH STATUS DISTRIBUTION:")
    df_analytics.groupBy("Health_Status").count().orderBy(col("count").desc()).show()

    print("2. THERMAL RISK DISTRIBUTION:")
    df_analytics.groupBy("Thermal_Risk").count().orderBy(col("count").desc()).show()

    print("3. CELL IMBALANCE STATUS DISTRIBUTION:")
    df_analytics.groupBy("Cell_Imbalance_Status").count().orderBy(col("count").desc()).show()

    print("4. FAULT SEVERITY LEVEL DISTRIBUTION:")
    df_analytics.groupBy("Fault_Severity_Level").count().orderBy(col("count").desc()).show()

    print("5. VEHICLE TYPE AGGREGATED DEGRADATION PROFILE:")
    vehicle_type_analytics.show(truncate=False)

    print("6. CHARGING IMPACT SUMMARY:")
    charging_impact_analytics.show(truncate=False)

    print("7. SAMPLE PREDICTIVE TELEMETRY WITH RISK SCORES (TOP 5):")
    sample_cols = [
        "Vehicle_ID",
        "Vehicle_Type",
        "Battery_SOH",
        "Health_Status",
        "Battery_Temperature",
        "Thermal_Risk",
        "Cell_Imbalance_Status",
        "Fault_Risk_Score",
        "Fault_Severity_Level",
        "Early_Fault_Prediction"
    ]
    df_analytics.select(sample_cols).show(5, truncate=False)

    elapsed = time.time() - start_time
    print("-" * 80)
    print("BATTERY ANALYTICS EXECUTION SUMMARY:")
    print("-" * 80)
    print(f"Total Telemetry Records Analyzed : {total_records:,}")
    print(f"Total Features & Diagnostic Cols : {len(df_analytics.columns)}")
    print(f"Enriched Parquet Destination     : {output_parquet_path}")
    print(f"Analytical CSVs Exported To      : {results_dir}")
    print(f"Total Execution Time             : {elapsed:.2f} seconds")
    print("=" * 80)
    print(" SCRIPT 04: BATTERY ANALYTICS COMPLETED SUCCESSFULLY")
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()
