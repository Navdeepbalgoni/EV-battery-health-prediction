"""
EV Guardian: Big Data Analytics-Based EV Health Monitoring and Fault Prediction System
Script 05: Final Analytical Output Generation & Data Pipeline Integration

Description:
    Loads the enriched battery analytics dataset from data/output/battery_analytics.parquet,
    generates consolidated analytical data marts, computes executive fleet Key Performance
    Indicators (KPIs), isolates critical high-risk vehicles requiring preventative maintenance,
    and produces standardized datasets for Hive data warehousing, HDFS integration, and
    RStudio visualization suites.

Input:
    Local: data/output/battery_analytics.parquet

Outputs:
    Local Parquet Data Mart: data/output/master_vehicle_health.parquet
    Local CSV Reports:       results/fleet_kpi_summary.csv
                             results/critical_risk_vehicles.csv
                             results/degradation_by_usage_profile.csv
                             results/master_vehicle_health.csv
    Optional HDFS Sink:      hdfs://localhost:9000/evguardian/output/
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
        .appName("EV_Guardian_05_Final_Output")
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
    print(" EV GUARDIAN - SCRIPT 05: FINAL ANALYTICAL OUTPUT GENERATION")
    print("=" * 80)

    spark = create_spark_session()

    # 1. Resolve Input and Output Paths
    candidate_input_paths = [
        "/mnt/c/bdaproject/EV_Guardian_BDA/evguardian/data/output/battery_analytics.parquet",
        "C:/bdaproject/EV_Guardian_BDA/evguardian/data/output/battery_analytics.parquet",
        "data/output/battery_analytics.parquet",
        "../data/output/battery_analytics.parquet"
    ]
    input_path = resolve_existing_path(candidate_input_paths)

    if os.path.isabs(input_path):
        base_dir = os.path.abspath(os.path.join(os.path.dirname(input_path), "..", ".."))
    else:
        base_dir = os.path.abspath(".")

    results_dir = os.path.join(base_dir, "results")
    output_dir = os.path.join(base_dir, "data", "output")
    os.makedirs(results_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    print(f"[INFO] Reading analytics dataset from: {input_path}")
    if not os.path.exists(input_path):
        print(f"[ERROR] Input dataset path {input_path} does not exist. Run 04_battery_analytics.py first.")
        spark.stop()
        sys.exit(1)

    df_analytics = spark.read.parquet(input_path)
    total_records = df_analytics.count()
    print(f"[SUCCESS] Loaded analytics telemetry dataset ({total_records:,} records)\n")

    # 2. Build Analytical Data Marts & Summaries
    print("-" * 80)
    print("[INFO] Constructing consolidated data marts and executive KPIs...")
    print("-" * 80)

    # 2.1 Executive Fleet Key Performance Indicators (KPIs)
    total_vehicles = df_analytics.select("Vehicle_ID").distinct().count()
    fleet_kpi_summary = df_analytics.agg(
        count("*").alias("Total_Telemetry_Readings"),
        spark_round(avg("Battery_SOH"), 2).alias("Fleet_Avg_SOH_Pct"),
        spark_round(spark_min("Battery_SOH"), 2).alias("Fleet_Min_SOH_Pct"),
        spark_round(avg("Battery_Temperature"), 2).alias("Fleet_Avg_Temp_C"),
        spark_round(spark_max("Battery_Temperature"), 2).alias("Fleet_Max_Temp_C"),
        spark_round(avg("Battery_Internal_Resistance"), 2).alias("Fleet_Avg_Resistance_mOhm"),
        spark_round(spark_max("Battery_Cell_Imbalance") * 1000.0, 2).alias("Fleet_Max_Imbalance_mV"),
        spark_round(avg("Fault_Risk_Score"), 2).alias("Fleet_Avg_Risk_Score"),
        spark_sum("Early_Fault_Prediction").alias("Total_Early_Fault_Alerts"),
        spark_sum("Fast_Charge_Stress_Flag").alias("Total_Fast_Charge_Stress_Events")
    ).withColumn("Total_Monitored_Vehicles", lit(total_vehicles))

    # 2.2 Master Vehicle Health Aggregation
    master_vehicle_health = df_analytics.groupBy(
        "Vehicle_ID", "Vehicle_Type", "Vehicle_Age_Years"
    ).agg(
        spark_round(avg("Battery_SOH"), 2).alias("Avg_SOH"),
        spark_round(spark_min("Battery_SOH"), 2).alias("Min_SOH"),
        spark_round(avg("Battery_Current_Capacity_Wh"), 2).alias("Avg_Current_Capacity_Wh"),
        spark_round(avg("Capacity_Loss_Wh"), 2).alias("Avg_Capacity_Loss_Wh"),
        spark_round(avg("Capacity_Retention_Ratio") * 100.0, 2).alias("Avg_Capacity_Retention_Pct"),
        spark_round(avg("Battery_Temperature"), 2).alias("Avg_Temp_C"),
        spark_round(spark_max("Battery_Temperature"), 2).alias("Max_Temp_C"),
        spark_round(avg("Battery_Internal_Resistance"), 2).alias("Avg_Resistance_mOhm"),
        spark_round(spark_max("Battery_Cell_Imbalance") * 1000.0, 2).alias("Max_Cell_Imbalance_mV"),
        spark_max("Charge_Cycles").alias("Max_Charge_Cycles"),
        spark_round(avg("Cycles_Per_Year"), 2).alias("Avg_Cycles_Per_Year"),
        spark_round(avg("Fault_Risk_Score"), 2).alias("Avg_Risk_Score"),
        spark_round(spark_max("Fault_Risk_Score"), 2).alias("Peak_Risk_Score"),
        spark_sum("Early_Fault_Prediction").alias("Total_Fault_Alerts"),
        spark_sum("Deep_Discharge_Flag").alias("Deep_Discharge_Events"),
        spark_sum("Fast_Charge_Stress_Flag").alias("Fast_Charge_Stress_Events"),
        count("*").alias("Total_Telemetry_Readings")
    ).withColumn(
        "Fleet_Health_Category",
        when(col("Avg_SOH") >= 85.0, "Healthy")
        .when((col("Avg_SOH") >= 75.0) & (col("Avg_SOH") < 85.0), "Moderate")
        .when((col("Avg_SOH") >= 65.0) & (col("Avg_SOH") < 75.0), "Degraded")
        .otherwise("Critical")
    ).withColumn(
        "Maintenance_Recommendation",
        when(col("Peak_Risk_Score") >= 70.0, "IMMEDIATE_BATTERY_REPLACEMENT")
        .when(col("Avg_SOH") < 70.0, "PACK_REFURBISHMENT")
        .when(col("Max_Cell_Imbalance_mV") > 35.0, "BMS_CELL_BALANCING")
        .when(col("Avg_Resistance_mOhm") > 50.0, "IMPEDANCE_DIAGNOSTIC")
        .when(col("Peak_Risk_Score") >= 40.0, "PREVENTATIVE_INSPECTION")
        .otherwise("NORMAL_OPERATION")
    ).orderBy(col("Peak_Risk_Score").desc())

    # 2.3 Critical High-Risk Vehicles Cohort (Actionable Maintenance Backlog)
    critical_risk_vehicles = master_vehicle_health.filter(
        (col("Peak_Risk_Score") >= 30.0) |
        (col("Fleet_Health_Category").isin(["Degraded", "Critical"])) |
        (col("Total_Fault_Alerts") > 0)
    ).orderBy(col("Peak_Risk_Score").desc(), col("Avg_SOH").asc())

    # 2.4 Degradation by Usage & Operational Profile
    df_with_age_bracket = df_analytics.withColumn(
        "Age_Bracket",
        when(col("Vehicle_Age_Years") < 1.0, "< 1 Year")
        .when((col("Vehicle_Age_Years") >= 1.0) & (col("Vehicle_Age_Years") < 2.0), "1 - 2 Years")
        .when((col("Vehicle_Age_Years") >= 2.0) & (col("Vehicle_Age_Years") < 3.0), "2 - 3 Years")
        .when((col("Vehicle_Age_Years") >= 3.0) & (col("Vehicle_Age_Years") < 4.0), "3 - 4 Years")
        .otherwise("4+ Years")
    )

    degradation_by_usage_profile = df_with_age_bracket.groupBy(
        "Vehicle_Type", "Age_Bracket", "Charging_Type"
    ).agg(
        count("*").alias("Telemetry_Readings"),
        countDistinct("Vehicle_ID").alias("Distinct_Vehicles"),
        spark_round(avg("Battery_SOH"), 2).alias("Avg_SOH"),
        spark_round(avg("Capacity_Loss_Wh"), 2).alias("Avg_Capacity_Loss_Wh"),
        spark_round(avg("Battery_Internal_Resistance"), 2).alias("Avg_Resistance_mOhm"),
        spark_round(avg("Battery_Temperature"), 2).alias("Avg_Temperature_C"),
        spark_round(avg("Fault_Risk_Score"), 2).alias("Avg_Risk_Score"),
        spark_sum("Fast_Charge_Stress_Flag").alias("Fast_Charge_Stress_Readings")
    ).orderBy("Vehicle_Type", "Age_Bracket", "Charging_Type")

    # 3. Save Master Analytical Datasets
    print("-" * 80)
    print("[INFO] Saving master analytical tables and CSV reports...")
    print("-" * 80)

    # 3.1 Parquet Data Mart
    master_parquet_path = os.path.join(output_dir, "master_vehicle_health.parquet")
    master_vehicle_health.write.mode("overwrite").parquet(master_parquet_path)
    print(f"[SUCCESS] Saved master vehicle health Parquet data mart to: {master_parquet_path}")

    # 3.2 Export CSVs for Hive & RStudio
    temp_csv_dir = os.path.join(results_dir, "_temp_spark_csv_05")
    save_single_csv(fleet_kpi_summary, os.path.join(results_dir, "fleet_kpi_summary.csv"), temp_csv_dir)
    save_single_csv(critical_risk_vehicles, os.path.join(results_dir, "critical_risk_vehicles.csv"), temp_csv_dir)
    save_single_csv(degradation_by_usage_profile, os.path.join(results_dir, "degradation_by_usage_profile.csv"), temp_csv_dir)
    save_single_csv(master_vehicle_health, os.path.join(results_dir, "master_vehicle_health.csv"), temp_csv_dir)
    print(f"[SUCCESS] Exported all consolidated CSV reports to: {results_dir}\n")

    # 4. Optional HDFS Backup Export
    hdfs_target = "hdfs://localhost:9000/evguardian/output/master_vehicle_health"
    try:
        print(f"[INFO] Attempting HDFS export to: {hdfs_target}...")
        master_vehicle_health.write.mode("overwrite").parquet(hdfs_target)
        print(f"[SUCCESS] Successfully synchronized data mart with HDFS at: {hdfs_target}")
    except Exception as e:
        print(f"[INFO] HDFS export skipped or cluster not actively responding ({e}). Local copy secured.")

    # 5. Print Executive Scorecard
    print("-" * 80)
    print(" EXECUTIVE FLEET HEALTH & FAULT SCORECARD")
    print("-" * 80)
    fleet_kpi_summary.show(vertical=True, truncate=False)

    print("")
    print("SAMPLE CRITICAL HIGH-RISK VEHICLES REQUIRING MAINTENANCE (TOP 10):")
    critical_sample_cols = [
        "Vehicle_ID",
        "Vehicle_Type",
        "Vehicle_Age_Years",
        "Avg_SOH",
        "Avg_Resistance_mOhm",
        "Max_Cell_Imbalance_mV",
        "Peak_Risk_Score",
        "Fleet_Health_Category",
        "Maintenance_Recommendation"
    ]
    critical_risk_vehicles.select(critical_sample_cols).show(10, truncate=False)

    print("")
    print("DEGRADATION & AGE PROFILE (ACROSS FLEET SEGMENTS):")
    degradation_by_usage_profile.show(15, truncate=False)

    elapsed = time.time() - start_time
    print("-" * 80)
    print("FINAL OUTPUT PIPELINE EXECUTION SUMMARY:")
    print("-" * 80)
    print(f"Total Telemetry Processed    : {total_records:,}")
    print(f"Total Monitored EV Assets    : {total_vehicles:,}")
    print(f"Master Parquet Output        : {master_parquet_path}")
    print(f"Consolidated CSV Directory   : {results_dir}")
    print(f"Total Pipeline Execution Time: {elapsed:.2f} seconds")
    print("=" * 80)
    print(" SCRIPT 05: FINAL OUTPUT COMPLETED SUCCESSFULLY")
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()
