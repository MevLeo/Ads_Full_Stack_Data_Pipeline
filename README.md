# AdFlow Media — Full-Stack Data Pipeline

A production-grade data pipeline for a fictional digital advertising agency managing paid campaigns across **Google Ads**, **YouTube Ads**, and **Bing Ads**. Built on a modern data stack using Snowflake and dbt Cloud with a medallion architecture.



## Business Scenario

AdFlow Media is a digital advertising agency running campaigns for 5 clients across 3 platforms simultaneously. The pipeline answers one core business question:

> **Which platform delivers the best return on ad spend — and where should the budget be reallocated?**

The pipeline surfaces a clear insight from the data:

| Platform | CPA | ROAS | Volume |
|---|---|---|---|
| YouTube Ads | $237 | High | Medium |
| Google Ads | $285 | Highest | High |
| Bing Ads | $299 | Lowest | Low (~35% of Google) |

**Conclusion:** YouTube has the lowest cost-per-acquisition despite lower volume, suggesting budget reallocation toward YouTube for conversion-focused campaigns.



## Architecture

```
Data Sources (CSV + Python simulation)
        │
        ▼
┌─────────────────────────────────────┐
│        ADFLOW_BRONZE (Snowflake)    │  Raw ingestion — immutable, exact source schema
│  RAW.google_ads_raw                 │
│  RAW.youtube_ads_raw                │
│  RAW.bing_ads_raw                   │
└─────────────┬───────────────────────┘
              │
              ▼
┌─────────────────────────────────────┐
│        ADFLOW_SILVER (dbt)          │  Cleaned, typed, unified
│  stg_google_ads                     │
│  stg_youtube_ads                    │
│  stg_bing_ads                       │
│  int_ads_unified  (60,008 rows)     │  All 3 platforms, one schema
└─────────────┬───────────────────────┘
              │
              ▼
┌─────────────────────────────────────┐
│        ADFLOW_GOLD (dbt)            │  Business-ready, BI-facing
│  mart_campaign_daily                │
│  mart_platform_summary              │
│  mart_ad_performance                │
└─────────────┬───────────────────────┘
              │
              ▼
     Looker Studio Dashboard
     (ROAS · CPA · CTR · CPV by platform)
```

**Medallion architecture** with 3 separate Snowflake databases — one per layer. Each layer has a strict role and no layer ever writes back to the layer above it.

**DAG:** Lineage Diagram
<img width="951" height="411" alt="image" src="https://github.com/user-attachments/assets/7c7f6dd2-b48d-4aad-8587-4d5eef560dd8" />


---

## Tech Stack

| Tool | Role |
|---|---|
| **Snowflake** | Cloud data warehouse (3 separate databases) |
| **dbt Cloud** | Transformation, testing, documentation, scheduling |
| **Python (pandas + numpy)** | Data simulation and source preparation |
| **Kaggle** | Real-world Google Ads and YouTube Ads base data |
| **Looker Studio** | BI dashboard connected to Gold layer |
| **GitHub** | Version control for all dbt models |

---

## Data Sources

The pipeline uses a deliberate hybrid sourcing strategy — real data where available, Python simulation where no public data exists.

### Base dataset — Kaggle

**[Marketing Campaign Performance Dataset](https://www.kaggle.com/datasets/manishabhatt22/marketing-campaign-performance-dataset?resource=download)**
— 200,000 rows covering 5 fictional companies across 6 channels (Google Ads, YouTube, Instagram, Facebook, Website, Email) for the full year 2021.

The dataset's `Channel_Used` column was the key: it already contained both `Google Ads` and `YouTube` as distinct channels — meaning real campaign data existed for both platforms. Only `Instagram`, `Facebook`, `Website`, and `Email` rows were discarded.

### Google Ads — filtered from Kaggle

Rows where `Channel_Used = 'Google Ads'` were extracted directly (33,438 rows). The following cleaning was applied:
- `Acquisition_Cost` string (`"$16,174.00"`) parsed to numeric `spend_usd`
- `campaign_type` values `Email` and `Influencer` filtered out in Silver (they don't belong in Google Ads)
- `conversions` derived as `clicks × conversion_rate`
- `ctr` and `cpc` computed as derived columns

### YouTube Ads — filtered from Kaggle + enriched via Python

Rows where `Channel_Used = 'YouTube'` were extracted (33,392 rows) and enriched with YouTube-specific metrics that don't exist in the Kaggle dataset:
- `view_rate` — simulated at 25–35% (real-world YouTube benchmark)
- `views` — derived as `impressions × view_rate`
- `cpv` — cost per view, derived as `spend_usd / views`
- `video_ad_type` — randomly assigned: skippable in-stream (60%), non-skippable (25%), bumper (15%)
- `conversions` — boosted by 15–25% to engineer the lower CPA narrative arc

### Bing Ads — fully simulated via Python

No usable public Bing Ads dataset exists on Kaggle. Bing data was simulated using Google Ads as the structural base, with realistic platform multipliers applied:
- Volume: ~35% of Google (reflecting real-world Bing market share)
- CPC: slightly higher than Google
- `match_type` field added: broad (40%), phrase (35%), exact (25%)
- Conversions slightly lower per click than Google

| Source file | Rows | Origin |
|---|---|---|
| `google_ads_raw.csv` | 33,438 | Filtered from Kaggle |
| `youtube_ads_raw.csv` | 33,392 | Filtered from Kaggle + Python enrichment |
| `bing_ads_raw.csv` | 33,438 | Fully simulated via Python |

---

## Project Structure

```
models/
├── staging/
│   ├── sources.yml              # Bronze source declarations
│   ├── schema.yml               # Staging layer tests
│   ├── stg_google_ads.sql       # Clean + cast Google Ads data
│   ├── stg_youtube_ads.sql      # Clean + enrich YouTube data
│   └── stg_bing_ads.sql         # Clean + cast Bing Ads data
├── intermediate/
│   └── int_ads_unified.sql      # UNION ALL across all 3 platforms
└── marts/
    ├── schema.yml               # Gold layer tests
    ├── mart_campaign_daily.sql  # Daily spend, clicks, ROAS per campaign
    ├── mart_platform_summary.sql # Cross-platform CPA/ROAS comparison
    └── mart_ad_performance.sql  # CTR, CPA, CPV per company/segment
```

---

## Key dbt Models

### `int_ads_unified`
Merges all three staging models into a single unified schema using `UNION ALL`. Platform-specific fields (`cpv`, `view_rate`, `match_type`) are preserved with `NULL` for platforms where they don't apply. This is the single source of truth for all downstream marts.

```sql
-- 60,008 rows across all platforms
select * from stg_google_ads   -- 19,985 rows
union all
select * from stg_youtube_ads  -- 20,038 rows
union all
select * from stg_bing_ads     -- 19,985 rows
```

### `mart_platform_summary`
The core business mart. Aggregates total spend, conversions, CPA, ROAS, and CTR per platform. This is the model that directly surfaces the YouTube vs Google vs Bing story.

### `mart_campaign_daily`
Time-series view of daily spend, clicks, conversions, and ROAS per campaign. Powers the trend line charts in the dashboard.

---

## Data Quality

39 dbt tests across all layers — all passing in production.

| Test type | Columns tested |
|---|---|
| `not_null` | All key metrics and identifiers |
| `accepted_values` | `platform`, `campaign_type`, `match_type` |
| `unique` | `platform` in `mart_platform_summary` |

```bash
dbt test  # All 39 tests pass
```

---

## Snowflake Setup

Three separate databases for strict layer separation:

```sql
CREATE DATABASE ADFLOW_BRONZE;  -- Raw ingestion layer
CREATE DATABASE ADFLOW_SILVER;  -- Staging + intermediate layer
CREATE DATABASE ADFLOW_GOLD;    -- Business marts layer

CREATE WAREHOUSE ADFLOW_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
```

Data is loaded into Bronze via Snowflake's `COPY INTO` command with an explicit column list to handle metadata columns (`_loaded_at`).

---

## Orchestration

A dbt Cloud **Deploy job** runs the full pipeline daily at **06:00 UTC**:

```
1. dbt deps
2. dbt source freshness
3. dbt run          (builds all 7 models)
4. dbt test         (runs all 39 tests)
```

The job is version-controlled — every model change goes through a pull request before reaching production.

---

## Business KPIs Computed

| Metric | Formula | Available in |
|---|---|---|
| CTR | `clicks / impressions` | All marts |
| CPC | `spend_usd / clicks` | All marts |
| CPA | `spend_usd / conversions` | All marts |
| CPV | `spend_usd / views` | YouTube only |
| ROAS | `avg(roi)` | All marts |
| View rate | `views / impressions` | YouTube only |

All divisions use `NULLIF` to protect against division by zero.

---

## Dashboard

The Looker Studio dashboard connects directly to `ADFLOW_GOLD` and includes:
<img width="1584" height="891" alt="image" src="https://github.com/user-attachments/assets/04c87367-0acf-4bcf-bb33-6f168c5dfe94" />


- **CPA by platform** (bar chart) — surfaces the YouTube efficiency story
- **ROAS by platform** (bar chart) — shows Google's return advantage
- **Daily spend trend** (line chart) — full 2021 campaign timeline by platform
- **CTR by company and segment** (table) — creative performance breakdown


---

## How to Run Locally

1. Download the base dataset from [Kaggle](https://www.kaggle.com/datasets/manishabhatt22/marketing-campaign-performance-dataset?resource=download)
2. Run the Python simulation script to generate the three Bronze CSVs (Google Ads and YouTube filtered from Kaggle, Bing simulated)
3. Set up a Snowflake account and create the three databases using the DDL in the Snowflake Setup section
4. Load the CSVs into `ADFLOW_BRONZE.RAW` using `COPY INTO` with an explicit column list
5. Connect dbt Cloud to your Snowflake account and set the database to `ADFLOW_SILVER`
6. Run `dbt run && dbt test`

---

## Skills Demonstrated

- Medallion architecture design with strict layer separation
- Multi-source data unification across heterogeneous schemas
- dbt model layering (staging → intermediate → marts)
- Data quality testing with dbt generic tests
- Snowflake database administration (warehouses, stages, COPY INTO)
- Python data simulation with statistically realistic multipliers
- Production deployment with dbt Cloud scheduler
- Version-controlled pipeline with GitHub integration
- Business-facing KPI computation (CPA, ROAS, CTR, CPV)

---

## Author

**Mohsen Eshghi** — Data and Product Analyst 
Stack: Snowflake · dbt Cloud · Python · SQL
