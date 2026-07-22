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

Run `notebooks/01_Data_Ingestion.py` on Databricks after uploading raw Instakart CSV files to the configured `source_path` widget. By default, it reads from `/FileStore/instakart/raw`, writes bronze Delta tables into the Unity Catalog schema `workspace.instakart_bronze`, and records each run in `_ingestion_manifest`.

## Silver cleaning

Run `notebooks/02_Data_Cleaning.py` after ingestion. It reads `workspace.instakart_bronze.orders`, writes valid deduplicated records to `workspace.instakart_silver.orders`, and sends invalid or duplicate records to `workspace.instakart_silver.orders_quarantine` with rejection reasons.
