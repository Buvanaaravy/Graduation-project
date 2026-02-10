
# Architecture Overview

## Data Sources
- AACT (ClinicalTrials.gov) – clinical trial metadata and enrollment
- FAERS – post-approval adverse event reports

## Data Flow
1. Raw AACT and FAERS files are ingested into the Bronze layer.
2. Silver layer applies data cleansing, standardization, and deduplication.
3. Gold layer builds star-schema fact and dimension tables for analytics.

## Medallion Architecture
- Bronze: Raw data with audit columns
- Silver: Clean, validated datasets
- Gold: Analytics-ready tables

## Consumption Layer
- Power BI dashboards consume Gold-layer tables.
