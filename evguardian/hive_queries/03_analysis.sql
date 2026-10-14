-- =============================================================================
-- EV Guardian: Big Data Analytics-Based EV Health Monitoring & Fault Prediction
-- Script 03: Distributed Hive Analytical Queries & Health Reporting
-- =============================================================================

USE ev_guardian_db;

-- Query 1: Top 20 High-Risk Vehicles Requiring Maintenance
SELECT 
    Vehicle_ID,
    Vehicle_Type,
    ROUND(Vehicle_Age_Years, 1) AS Age_Years,
    Avg_SOH,
    Avg_Resistance_mOhm,
    Max_Cell_Imbalance_mV,
    Peak_Risk_Score,
    Maintenance_Recommendation
FROM critical_risk_vehicles
ORDER BY Peak_Risk_Score DESC
LIMIT 20;

-- Query 2: Degradation & Health Status Distribution Across Fleet Segments
SELECT 
    Vehicle_Type,
    Distinct_Vehicles,
    Avg_SOH,
    Avg_Capacity_Loss_Wh,
    Avg_Resistance_mOhm,
    Avg_Risk_Score,
    Total_Fault_Alerts
FROM vehicle_type_analytics
ORDER BY Avg_Risk_Score DESC;

-- Query 3: Fast vs Slow Charging Impact on Temperature and Resistance
SELECT 
    Charging_Status,
    Charging_Type,
    Reading_Count,
    Avg_Power_kW,
    Avg_Temperature_C,
    Max_Temperature_C,
    Avg_Resistance_mOhm,
    Avg_SOH,
    Fast_Charge_Stress_Count
FROM charging_impact_analytics
ORDER BY Reading_Count DESC;

-- Query 4: SOH Fleet Distribution & Critical Counts
SELECT 
    Fleet_Health_Status,
    COUNT(*) AS Vehicle_Count,
    ROUND(AVG(Avg_SOH), 2) AS Mean_SOH,
    ROUND(AVG(Avg_Risk_Score), 2) AS Mean_Risk
FROM vehicle_health_summary
GROUP BY Fleet_Health_Status
ORDER BY Mean_Risk DESC;
