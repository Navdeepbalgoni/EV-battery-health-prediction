-- =============================================================================
-- EV Guardian: Big Data Analytics-Based EV Health Monitoring & Fault Prediction
-- Script 02: Hive Schema Definition & External Tables
-- =============================================================================

USE ev_guardian_db;

-- 1. Fleet Vehicle Health Summary Table
DROP TABLE IF EXISTS vehicle_health_summary;
CREATE EXTERNAL TABLE IF NOT EXISTS vehicle_health_summary (
    Vehicle_ID STRING,
    Vehicle_Type STRING,
    Vehicle_Age_Years DOUBLE,
    Avg_SOH DOUBLE,
    Min_SOH DOUBLE,
    Avg_Temp_C DOUBLE,
    Max_Temp_C DOUBLE,
    Avg_Resistance_mOhm DOUBLE,
    Max_Cell_Imbalance_mV DOUBLE,
    Max_Charge_Cycles INT,
    Avg_Risk_Score DOUBLE,
    Max_Risk_Score DOUBLE,
    Fault_Alert_Count INT,
    Total_Readings BIGINT,
    Fleet_Health_Status STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/evguardian/hive_tables/vehicle_health_summary'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 2. Vehicle Type Aggregated Analytics Table
DROP TABLE IF EXISTS vehicle_type_analytics;
CREATE EXTERNAL TABLE IF NOT EXISTS vehicle_type_analytics (
    Vehicle_Type STRING,
    Total_Telemetry_Readings BIGINT,
    Distinct_Vehicles BIGINT,
    Avg_SOH DOUBLE,
    Avg_Capacity_Loss_Wh DOUBLE,
    Avg_Cycles_Per_Year DOUBLE,
    Avg_Temperature_C DOUBLE,
    Avg_Resistance_mOhm DOUBLE,
    Avg_Risk_Score DOUBLE,
    Total_Fault_Alerts BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/evguardian/hive_tables/vehicle_type_analytics'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 3. Charging Protocol Impact Analytics Table
DROP TABLE IF EXISTS charging_impact_analytics;
CREATE EXTERNAL TABLE IF NOT EXISTS charging_impact_analytics (
    Charging_Status STRING,
    Charging_Type STRING,
    Reading_Count BIGINT,
    Avg_Power_kW DOUBLE,
    Avg_Temperature_C DOUBLE,
    Max_Temperature_C DOUBLE,
    Avg_Resistance_mOhm DOUBLE,
    Avg_SOH DOUBLE,
    Fast_Charge_Stress_Count BIGINT,
    High_Temp_Reading_Pct DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/evguardian/hive_tables/charging_impact_analytics'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 4. Critical High-Risk Maintenance Vehicles
DROP TABLE IF EXISTS critical_risk_vehicles;
CREATE EXTERNAL TABLE IF NOT EXISTS critical_risk_vehicles (
    Vehicle_ID STRING,
    Vehicle_Type STRING,
    Vehicle_Age_Years DOUBLE,
    Avg_SOH DOUBLE,
    Min_SOH DOUBLE,
    Avg_Current_Capacity_Wh DOUBLE,
    Avg_Capacity_Loss_Wh DOUBLE,
    Avg_Capacity_Retention_Pct DOUBLE,
    Avg_Temp_C DOUBLE,
    Max_Temp_C DOUBLE,
    Avg_Resistance_mOhm DOUBLE,
    Max_Cell_Imbalance_mV DOUBLE,
    Max_Charge_Cycles INT,
    Avg_Cycles_Per_Year DOUBLE,
    Avg_Risk_Score DOUBLE,
    Peak_Risk_Score DOUBLE,
    Total_Fault_Alerts INT,
    Deep_Discharge_Events INT,
    Fast_Charge_Stress_Events INT,
    Total_Telemetry_Readings BIGINT,
    Fleet_Health_Category STRING,
    Maintenance_Recommendation STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/evguardian/hive_tables/critical_risk_vehicles'
TBLPROPERTIES ("skip.header.line.count"="1");

SHOW TABLES;
