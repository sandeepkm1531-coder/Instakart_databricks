# Instakart_databricks

Databricks project structure for data ingestion, cleaning, ETL transformations, SQL analytics, feature engineering, machine learning, MLflow tracking, and predictions.

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

Run `notebooks/03_ETL_Transformations.py` after the Silver notebook. This
single consolidated Gold notebook publishes seven dashboard-ready Delta tables
to `workspace.instakart_gold`: executive KPIs, order-sequence trends, customer,
product, department and aisle metrics, and day-of-week/hour shopping behavior.
It validates required inputs, primary-key uniqueness, and Silver-to-Gold
order-item row counts before publishing. Calendar day/week/month trends are not
fabricated because the source Instacart dataset has no actual order date.
