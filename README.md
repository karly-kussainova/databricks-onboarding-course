# Databricks Onboarding Course: NYC Taxi Data Pipeline

A hands-on course for building data pipelines on Databricks using NYC taxi trip data. Each module is a standalone Databricks Asset Bundle (DAB) that builds on the previous one, introducing progressively more complex patterns.

## Dataset

All modules work with the same three raw data sources stored in Databricks Volumes:

- **Green taxi trips** — trip records from NYC green cabs
- **Yellow taxi trips** — trip records from NYC yellow cabs
- **Taxi zone lookup** — reference table mapping location IDs to boroughs and zones

## Modules

### Module 01: Basic Data Ingestion
Introduction to reading raw files from Databricks Volumes. Creates the `new_york_taxi` catalog and `bronze` schema, reads raw parquet and CSV files, and displays results. Single-task DAB job.

### Module 02: Data Transformation & Analysis
Transforms raw bronze data into standardized silver tables. Handles schema mismatches between green and yellow taxi data, applies type casting and data quality filters, and unifies both datasets into a single analytical view. Two-task DAB job with sequential dependency.

### Module 03: Modular Pipeline Architecture
Refactors the monolithic Module 02 approach into separate, parameterized notebooks — one per transformation. Introduces DAB variables, dbutils widgets, YAML anchors for shared parameters, and parallel task execution. Five-task orchestrated DAB job.

### Module 04: Gold Layer Analytics
Builds dashboard-ready aggregation tables and KPIs on top of the silver layer. Computes revenue metrics by zone, peak hour analysis, daily trip trends, and zone performance rankings. Follows the same modular pattern as Module 03 with parameterized notebooks and task orchestration.

## Architecture

Each module follows the medallion architecture:

```
Raw Files (Volumes) → Bronze (raw Delta tables) → Silver (cleaned & standardized) → Gold (aggregated KPIs)
```

## Deployment

Each module contains its own `databricks.yml` with dev and prod targets. Deploy using the Databricks CLI:

```bash
cd module_XX/nyc-taxi
databricks bundle deploy -t dev
databricks bundle run -t dev nyc-taxi-job
```
