# Instakart_databricks

Instacart analytics project using Databricks Bronze, Silver, and Gold layers, Google Sheets, a Looker Studio dashboard, and a RAG assistant for explaining the analytics.

## Current progress

The Silver and Gold notebooks have been run in Databricks. Gold data was exported
to Google Sheets and used to create a Looker Studio dashboard. A RAG assistant
was also built; its documented approach uses Google Colab, sentence-transformers,
FAISS retrieval, and Gemini.

```text
Instacart CSVs in S3 -> Databricks Bronze -> Silver -> Gold
                                                    |
                                                    v
                                              Google Sheets
                                                    |
                                                    v
                                           Looker Studio dashboard

Analytics knowledge base -> embeddings -> FAISS -> Gemini RAG assistant
```

See the [project overview](docs/project_overview.md),
[Looker Studio dashboard notes](docs/looker_studio_dashboard.md), and
[RAG assistant notes](docs/rag_assistant.md).
The dashboard and assistant currently live outside this repository; the assistant
notebook and live dashboard URL have not yet been added. The numbered analytics
and machine-learning files remain scaffolding, not evidence of completed models.

## Project Structure

- `data/`: raw, processed, and curated datasets
- `notebooks/`: Databricks notebook source files
- `sql/`: reusable SQL analytics scripts
- `workflows/`: workflow design assets
- `docs/`: architecture and project documentation
- `output/`: generated parquet files and prediction outputs

## Data Ingestion

Upload and run `notebooks/01_Bronze_Ingestion.py` in Databricks. It reads the six
Instacart CSV files from the configured Amazon S3 source, keeps Auto Loader schema
state and checkpoints under an explicit S3 state prefix (never public DBFS), writes
Bronze Delta tables to `workspace.instakart_bronze`, and records each successful
run in `_ingestion_manifest`. In addition to `orders`, it creates
`bronze_products`, `bronze_aisles`, `bronze_departments`,
`bronze_order_products_prior`, and `bronze_order_products_train`. Each dataset has
an independent checkpoint and schema-state location.

## Silver cleaning

Run `notebooks/02_Silver_Layer.py` after Bronze ingestion. This single notebook
cleans all six datasets in dependency order: orders, aisles, departments,
products, and prior/train order-product rows. It validates relationships and
writes `silver_*` Delta tables plus a matching quarantine table for every
dataset.

## Gold analytics

Run `notebooks/03_Gold_Layer.py` after the Silver notebook. This
single consolidated Gold notebook publishes seven dashboard-ready Delta tables
to `workspace.instakart_gold`: executive KPIs, order-sequence trends, customer,
product, department and aisle metrics, and day-of-week/hour shopping behavior.
It validates required inputs, primary-key uniqueness, and Silver-to-Gold
order-item row counts before publishing. Calendar day/week/month trends are not
fabricated because the source Instacart dataset has no actual order date.

The current dashboard uses Google Sheets and Looker Studio; see
[the dashboard notes](docs/looker_studio_dashboard.md). The
[Power BI guide](docs/powerbi_dashboard_guide.md) is retained as an earlier
alternative design, not the implemented dashboard.
