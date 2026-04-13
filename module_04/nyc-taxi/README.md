# nyc-taxi Module 04 - Gold Layer Analytics

This module builds gold layer analytics on top of the silver tables from Module 3. It creates four gold tables that provide business-ready insights into NYC taxi operations.

## Gold Tables

- **revenue_by_zone**: Revenue metrics aggregated by taxi zone and borough
- **peak_hours**: Trip patterns and revenue metrics by hour of day
- **daily_trends**: Daily aggregated metrics for time-series analysis
- **zone_performance_summary**: Ranked performance metrics by zone

## Getting Started

To deploy and manage this asset bundle, follow these steps:

### 1. Deployment

- Click the **deployment rocket** 🚀 in the left sidebar to open the **Deployments** panel, then click **Deploy**.

### 2. Running Jobs & Pipelines

- To run a deployed job or pipeline, hover over the resource in the **Deployments** panel and click the **Run** button.

### 3. Managing Resources

- Use the **Add** dropdown to add resources to the asset bundle.
- Click **Schedule** on a notebook within the asset bundle to create a **job definition** that schedules the notebook.

## Documentation

- For information on using **Databricks Asset Bundles in the workspace**, see: [Databricks Asset Bundles in the workspace](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-bundles)
- For details on the **Databricks Asset Bundles format** used in this asset bundle, see: [Databricks Asset Bundles Configuration reference](https://docs.databricks.com/aws/en/dev-tools/bundles/reference)
