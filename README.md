# Instacart Analytics with Databricks Medallion Architecture, Machine Learning and RAG Assistant

This project takes raw shopping data, prepares reliable reporting tables,
visualizes business patterns, and makes selected information accessible through
an AI assistant—with a separate machine-learning experiment.

Databricks Bronze, Silver, and Gold layers prepare the data. Google Sheets and
Looker Studio support reporting, while the RAG assistant explains selected
analytics knowledge. A separate ML notebook models historical shopping patterns
and compares models with optional hyperparameter tuning.

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
notebook and live dashboard URL have not yet been added. Notebooks 04–09 remain
scaffolding; the completed ML experiment is in notebook 10 below.

## Machine learning and hyperparameter tuning

Open [10_Gold_ML_Hyperparameter_Tuning.ipynb](notebooks/10_Gold_ML_Hyperparameter_Tuning.ipynb)
in [Google Colab](https://colab.research.google.com/github/sandeepkm1531-coder/Instakart_databricks/blob/main/notebooks/10_Gold_ML_Hyperparameter_Tuning.ipynb).
Paste the Google Sheet URL and select the `shopping-behavior` worksheet. Public
Sheets work without credentials; private Sheets support sign-in, and Excel/CSV
upload is also available. Set `RUN_TUNING` to enable or skip hyperparameter search.

The completed run compares a mean baseline, Ridge regression, and Random Forest
on 168 historical day/hour aggregates. The tuned Random Forest achieved test MAE
2,392.95, RMSE 2,910.44, R² 0.9574, and WAPE 8.90% on 35 held-out rows.
These measure historical pattern estimation, not future forecasting.

The [saved run](output/ml/README.md) includes the trained model, prediction helper,
package versions, tuning trials, evaluation split, and metrics.

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
