# ==============================================================================
# EV Guardian: Big Data Analytics-Based EV Health Monitoring & Fault Prediction
# Script 01: Fleet Battery Health Overview & SOH Distributions
# ==============================================================================

library(ggplot2)
library(dplyr)

# 1. Paths Resolution
results_dir <- file.path("..", "results")
if (!dir.exists(results_dir)) {
  results_dir <- "results"
}

v_type_file <- file.path(results_dir, "vehicle_type_analytics.csv")
v_health_file <- file.path(results_dir, "vehicle_health_summary.csv")

# 2. Vehicle Type Degradation Overview
if (file.exists(v_type_file)) {
  df_type <- read.csv(v_type_file)
  cat("[INFO] Loaded vehicle type analytics\n")
  print(head(df_type))

  # Bar plot: Average SOH by Vehicle Type
  p1 <- ggplot(df_type, aes(x = Vehicle_Type, y = Avg_SOH, fill = Vehicle_Type)) +
    geom_bar(stat = "identity", width = 0.6) +
    geom_text(aes(label = paste0(Avg_SOH, "%")), vjust = -0.5, fontface = "bold") +
    scale_fill_brewer(palette = "Blues") +
    ylim(0, 105) +
    labs(
      title = "EV Guardian: Average State of Health (SOH) by Vehicle Category",
      subtitle = "Fleet-wide distributed aggregation from 4.62M telemetry records",
      x = "Vehicle Category",
      y = "Average SOH (%)"
    ) +
    theme_minimal(base_size = 14) +
    theme(legend.position = "none")

  print(p1)
  ggsave(file.path(results_dir, "plot_01_soh_by_type.png"), p1, width = 8, height = 5, dpi = 300)
}

# 3. Fleet Health Status Distribution
if (file.exists(v_health_file)) {
  df_health_sample <- read.csv(v_health_file, nrows = 50000)
  
  p2 <- ggplot(df_health_sample, aes(x = Fleet_Health_Status, fill = Fleet_Health_Status)) +
    geom_bar(width = 0.5) +
    scale_fill_manual(values = c("Healthy" = "#2ECC71", "Moderate" = "#F39C12", "Degraded" = "#E74C3C", "Critical" = "#962D3E")) +
    labs(
      title = "EV Guardian: Fleet Health Status Classification",
      x = "Health Status Tier",
      y = "Vehicle Count"
    ) +
    theme_minimal(base_size = 14)

  print(p2)
  ggsave(file.path(results_dir, "plot_02_health_distribution.png"), p2, width = 8, height = 5, dpi = 300)
}
cat("[SUCCESS] Script 01: Health Overview plots generated successfully.\n")
