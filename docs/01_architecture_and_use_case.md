# Architecture and Use Case: Geopolitical Risk Monitoring Pipeline

## Project Overview

This project builds a batch pipeline for geopolitical risk monitoring based on GDELT event data. It is aimed at analysts working in areas such as national security, strategic intelligence, or corporate risk assessment.

The pipeline ingests global event data from the **GDELT Project**, stores and transforms it in PostgreSQL. It then produces hourly summaries that highlight potentially relevant developments. This lets analysts first spot unusual countries or time windows that deserve closer attention and then trace those signals back to the underlying events.

## Why This Pipeline Is Needed

GDELT provides a large volume of structured global event data, including timestamps, actors, locations, event codes, and media-related attributes such as mentions and source URLs.

In raw form, the data is not very usable for direct analysis. The export files contain millions of records per day and require lookups to decode event codes and actor references.

## Analytical Workflow

The analytical workflow is split into two steps.

First, analysts use aggregated hourly country-level summaries to monitor activity and prioritize attention. These summaries include overall event volume, selected counts for risk-relevant categories such as protests, conflicts, and diplomatic tensions, as well as a weighted instability score.

Second, once a country or hourly window stands out, analysts can take a closer look at the underlying events. This allows them to see which concrete events, actors, and sources caused the country or time window to stand out.

## Data Architecture

**Bronze** stores the raw GDELT event data as ingested. This preserves the original records and supports traceability and reproducibility at batch level.

**Silver** contains the cleaned and enriched event layer. At this stage, timestamps are normalized, fields are decoded, and the event data is joined with reference tables such as country, event code, and actor mappings. Additional logic is applied to classify events into risk-relevant categories.

**Gold** contains the aggregated monitoring outputs. The Gold layer stores hourly country-level summaries built from the Silver events. These outputs are meant for monitoring, dashboards, and simple alerting.

## Pipeline Execution Flow

The pipeline runs in four main stages:

1. **Manifest and raw ingestion**
   Relevant GDELT export batches are identified through manifest files, registered, downloaded, and loaded into the Bronze layer.

2. **Reference data loading**
   Curated lookup data, such as event code mappings, country information, event roots, and known-group references, is loaded into dimension tables.

3. **Silver transformation**
   Bronze events are cleaned, normalized, and enriched through lookup joins and transformation logic. This step also derives geographic focus fields and maps GDELT event roots into the project’s custom risk dimensions.

4. **Gold aggregation**
   The transformed Silver events are aggregated into hourly country-level summaries that support monitoring and prioritization.
