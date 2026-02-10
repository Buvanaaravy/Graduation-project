# Data Model

## Fact Tables
- fact_clinical_trials
- fact_adverse_events

## Dimension Tables
- dim_drug
- dim_condition
- dim_patient
- dim_study
- dim_date

## Design Rationale
- Star schema chosen for BI performance.
- Dimensions reused across clinical and real-world datasets.
- Fact tables capture measurable events.

