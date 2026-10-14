# ==============================================================================
# EV Guardian: Big Data Analytics-Based EV Health Monitoring & Fault Prediction
# Script 02: In-Depth Battery Degradation & Charging Impact Analysis
# ==============================================================================

library(ggplot2)
library(dplyr)

results_dir <- file.path("..", "results")
if (!dir.exists(results_dir)) {
  results_dir <- "results"
}

charging_file <- file.path(results_dir, "charging_impact_analytics.csv")
degrade_file <- file.path(results_dir, "degradation_by_usage_profile.csv")

# 1. Charging Protocol Impact on Battery Temperature & Resistance
if (file.exists(charging_file)) {
  df_charge <- read.csv(charging_file)
  cat("[INFO] Loaded charging impact analytics\n")

  p3 <- ggplot(df_charge, aes(x = Charging_Status, y = Avg_Temperature_C, fill = Charging_Type)) +
    geom_bar(stat = "identity", position = position_dodge(0.8), width = 0.7) +
    scale_fill_manual(values = c("Fast" = "#E67E22", "Slow" = "#3498DB")) +
    labs(
      title = "Thermal Stress: Average Pack Temperature by Charging Mode",
      x = "Operational State",
      y = "Average Temperature (°C)",
      fill = "Charging Protocol"
    ) +
    theme_minimal(base_size = 14)

  print(p3)
  ggsave(file.path(results_dir, "plot_03_charging_thermal_impact.png"), p3, width = 8, height = 5, dpi = 300)
}

# 2. Aging Bracket Degradation Profile
if (file.exists(degrade_file)) {
  df_deg <- read.csv(degrade_file)

  p4 <- ggplot(df_deg, aes(x = Age_Bracket, y = Avg_SOH, group = interaction(Vehicle_Type, Charging_Type), color = Vehicle_Type)) +
    geom_line(aes(linetype = Charging_Type), size = 1.2) +
    geom_point(size = 3) +
    labs(
      title = "Battery SOH Degradation Trajectory Across Vehicle Age Brackets",
      x = "Vehicle Age Cohort",
      y = "Average SOH (%)",
      color = "Vehicle Type",
      linetype = "Charging Type"
    ) +
    theme_minimal(base_size = 14)

  print(p4)
  ggsave(file.path(results_dir, "plot_04_aging_degradation_curve.png"), p4, width = 9, height = 5, dpi = 300)
}
cat("[SUCCESS] Script 02: Battery Analysis plots generated successfully.\n")
