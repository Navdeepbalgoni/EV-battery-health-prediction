# ==============================================================================
# EV Guardian: Big Data Analytics-Based EV Health Monitoring & Fault Prediction
# Script 03: Executive Dashboard & Critical Risk Vehicle Prioritization
# ==============================================================================

library(ggplot2)
library(dplyr)

results_dir <- file.path("..", "results")
if (!dir.exists(results_dir)) {
  results_dir <- "results"
}

crit_file <- file.path(results_dir, "critical_risk_vehicles.csv")
kpi_file <- file.path(results_dir, "fleet_kpi_summary.csv")

if (file.exists(crit_file)) {
  df_crit <- read.csv(crit_file)
  cat("[INFO] Total critical high-risk vehicles identified:", nrow(df_crit), "\n")

  # Priority maintenance action breakdown
  p5 <- ggplot(df_crit, aes(x = Maintenance_Recommendation, fill = Maintenance_Recommendation)) +
    geom_bar(width = 0.5) +
    coord_flip() +
    labs(
      title = "Actionable Maintenance Recommendations for Critical Assets",
      x = "Action Prescribed by AI Diagnostics",
      y = "Vehicles Requiring Maintenance"
    ) +
    theme_minimal(base_size = 14) +
    theme(legend.position = "none")

  print(p5)
  ggsave(file.path(results_dir, "plot_05_maintenance_actions.png"), p5, width = 8, height = 5, dpi = 300)

  # Risk Score vs Cell Imbalance
  top_200 <- head(df_crit, 200)
  p6 <- ggplot(top_200, aes(x = Max_Cell_Imbalance_mV, y = Peak_Risk_Score, color = Vehicle_Type)) +
    geom_point(alpha = 0.7, size = 3) +
    geom_smooth(method = "lm", se = FALSE, linetype = "dashed") +
    labs(
      title = "Cell Imbalance Anomaly vs. Peak Fault Risk Score",
      subtitle = "Top prioritized maintenance cohort",
      x = "Maximum Cell Imbalance (mV)",
      y = "Composite Risk Score (0 - 100)"
    ) +
    theme_minimal(base_size = 14)

  print(p6)
  ggsave(file.path(results_dir, "plot_06_imbalance_vs_risk.png"), p6, width = 8, height = 5, dpi = 300)
}
cat("[SUCCESS] Script 03: Executive Visualization plots generated successfully.\n")
