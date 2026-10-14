-- =============================================================================
-- EV Guardian: Big Data Analytics-Based EV Health Monitoring & Fault Prediction
-- Script 01: Hive Database Setup & Environment Configuration
-- =============================================================================

CREATE DATABASE IF NOT EXISTS ev_guardian_db
COMMENT 'EV Guardian Telemetry & Battery Analytics Database'
LOCATION '/evguardian/hive_warehouse';

USE ev_guardian_db;

SHOW DATABASES;
DESCRIBE DATABASE EXTENDED ev_guardian_db;
