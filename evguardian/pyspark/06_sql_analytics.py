"""
EV Guardian: Big Data Analytics-Based EV Health Monitoring and Fault Prediction System
Script 06: Distributed SQL Analytics & Hive-Compliant Query Engine

Description:
    Implements the Data Warehousing & OLAP SQL analytics layer using PySpark's
    distributed SQL engine. Executes the analytical queries defined in
    evguardian/hive_queries/03_analysis.sql directly against the generated
    data marts without requiring a separate Apache Hive installation or Metastore service.

Queries Executed:
    1. Top 20 High-Risk Vehicles Requiring Preventative Maintenance
    2. Degradation & Health Status Distribution Across Fleet Segments
    3. Fast vs Slow Charging Impact on Thermal Stress and Internal Resistance
    4. Fleet SOH Distribution & Health Tier Aggregation
"""

import os
import sys
import time
from pyspark.sql import SparkSession


def resolve_project_root():
    """Locates the project base directory whether executed from root or pyspark/."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if os.path.basename(current_dir) == "pyspark":
        return os.path.abspath(os.path.join(current_dir, ".."))
    return current_dir


def create_spark_session():
    """Initializes SparkSession calibrated for fast analytical querying."""
    spark = (
        SparkSession.builder
        .appName("EV_Guardian_06_SQL_Analytics")
        .master("local[2]")
        .config("spark.driver.memory", "1024m")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main():
    start_time = time.time()
    base_dir = resolve_project_root()
    results_dir = os.path.join(base_dir, "results")

    print("=" * 80)
    print(" EV GUARDIAN - SCRIPT 06: DISTRIBUTED SQL ANALYTICS (HIVE-COMPLIANT)")
    print("=" * 80)
    print(f"[INFO] Results Directory: {results_dir}")

    spark = create_spark_session()

    # 1. Register Data Marts as SQL Tables / Temporary Views
    tables = {
        "critical_risk_vehicles": os.path.join(results_dir, "critical_risk_vehicles.csv"),
        "vehicle_type_analytics": os.path.join(results_dir, "vehicle_type_analytics.csv"),
        "charging_impact_analytics": os.path.join(results_dir, "charging_impact_analytics.csv"),
        "vehicle_health_summary": os.path.join(results_dir, "vehicle_health_summary.csv"),
    }

    print("\n[STEP 1] Registering analytical data marts into Spark SQL engine...")
    for table_name, file_path in tables.items():
        if os.path.exists(file_path):
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
            df.createOrReplaceTempView(table_name)
            print(f"  + Registered view '{table_name}' from {os.path.basename(file_path)}")
        else:
            print(f"  ! Warning: {file_path} not found. Skipping table registration.")

    # -------------------------------------------------------------------------
    # Query 1: Top 20 High-Risk Vehicles Requiring Maintenance
    # -------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print(" [QUERY 1] Top 10 High-Risk Vehicles Requiring Urgent Maintenance")
    print("=" * 80)
    q1 = """
    SELECT 
        Vehicle_ID,
        Vehicle_Type,
        ROUND(Vehicle_Age_Years, 1) AS Age_Years,
        ROUND(Avg_SOH, 2) AS Avg_SOH,
        ROUND(Avg_Resistance_mOhm, 2) AS Resistance_mOhm,
        ROUND(Max_Cell_Imbalance_mV, 2) AS Max_Imbalance_mV,
        ROUND(Peak_Risk_Score, 2) AS Peak_Risk,
        Maintenance_Recommendation
    FROM critical_risk_vehicles
    ORDER BY Peak_Risk_Score DESC
    LIMIT 10
    """
    spark.sql(q1).show(truncate=False)

    # -------------------------------------------------------------------------
    # Query 2: Degradation & Health Status Distribution Across Fleet Segments
    # -------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print(" [QUERY 2] Degradation & Health Metrics Across Vehicle Categories")
    print("=" * 80)
    q2 = """
    SELECT 
        Vehicle_Type,
        Distinct_Vehicles,
        ROUND(Avg_SOH, 2) AS Avg_SOH,
        ROUND(Avg_Capacity_Loss_Wh, 2) AS Capacity_Loss_Wh,
        ROUND(Avg_Resistance_mOhm, 2) AS Resistance_mOhm,
        ROUND(Avg_Risk_Score, 2) AS Risk_Score,
        Total_Fault_Alerts
    FROM vehicle_type_analytics
    ORDER BY Avg_Risk_Score DESC
    """
    spark.sql(q2).show(truncate=False)

    # -------------------------------------------------------------------------
    # Query 3: Fast vs Slow Charging Impact on Temperature and Resistance
    # -------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print(" [QUERY 3] Charging Protocol Impact: Fast vs Slow Charging Comparison")
    print("=" * 80)
    q3 = """
    SELECT 
        Charging_Status,
        Charging_Type,
        Reading_Count,
        ROUND(Avg_Power_kW, 2) AS Avg_kW,
        ROUND(Avg_Temperature_C, 2) AS Avg_Temp_C,
        ROUND(Max_Temperature_C, 2) AS Max_Temp_C,
        ROUND(Avg_Resistance_mOhm, 2) AS Avg_Resist_mOhm,
        ROUND(Avg_SOH, 2) AS Avg_SOH,
        Fast_Charge_Stress_Count
    FROM charging_impact_analytics
    ORDER BY Reading_Count DESC
    """
    spark.sql(q3).show(truncate=False)

    # -------------------------------------------------------------------------
    # Query 4: SOH Fleet Distribution & Critical Counts
    # -------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print(" [QUERY 4] Fleet Health Tier Distribution")
    print("=" * 80)
    q4 = """
    SELECT 
        Fleet_Health_Status,
        COUNT(*) AS Vehicle_Count,
        ROUND(AVG(Avg_SOH), 2) AS Mean_SOH,
        ROUND(AVG(Avg_Risk_Score), 2) AS Mean_Risk
    FROM vehicle_health_summary
    GROUP BY Fleet_Health_Status
    ORDER BY Mean_Risk DESC
    """
    spark.sql(q4).show(truncate=False)

    elapsed = time.time() - start_time
    print("=" * 80)
    print(f" [SUCCESS] Script 06: SQL Analytics completed successfully in {elapsed:.2f} seconds.")
    print("=" * 80)
    spark.stop()


if __name__ == "__main__":
    main()
