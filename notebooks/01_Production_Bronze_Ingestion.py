# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 - Production Bronze Ingestion
# MAGIC
# MAGIC Incrementally ingest the six Instacart CSV datasets from Amazon S3 into
# MAGIC Unity Catalog Bronze Delta tables using Databricks Auto Loader.
# MAGIC
# MAGIC Production characteristics:
# MAGIC
# MAGIC - parameterized source and target locations
# MAGIC - configuration-driven ingestion (no duplicated dataset code)
# MAGIC - one Auto Loader schema location and checkpoint per dataset
# MAGIC - append-only, rerunnable ingestion
# MAGIC - source lineage and batch audit columns
# MAGIC - rescued-data support for unexpected CSV values/columns
# MAGIC - file-presence, query-status, and table-count validation
# MAGIC
# MAGIC Bronze fields intentionally remain strings. Casting, business validation,
# MAGIC deduplication, and quarantine handling belong in the Silver layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Runtime parameters
# MAGIC
# MAGIC The defaults match the current Instacart S3 bucket. A Databricks Workflow
# MAGIC can override these widget values for another environment.

# COMMAND ----------

dbutils.widgets.text(
    "source_base_path",
    "s3://instakart-databricks-sandeep-2026/raw",
    "Source base path",
)
dbutils.widgets.text(
    "checkpoint_base_path",
    "s3://instakart-databricks-sandeep-2026/system/checkpoints/bronze",
    "Checkpoint base path",
)
dbutils.widgets.text(
    "schema_base_path",
    "s3://instakart-databricks-sandeep-2026/system/schemas/bronze",
    "Auto Loader schema path",
)
dbutils.widgets.text("catalog", "workspace", "Target catalog")
dbutils.widgets.text("bronze_schema", "instakart_bronze", "Bronze schema")

SOURCE_BASE_PATH = dbutils.widgets.get("source_base_path").strip().rstrip("/")
CHECKPOINT_BASE_PATH = dbutils.widgets.get("checkpoint_base_path").strip().rstrip("/")
SCHEMA_BASE_PATH = dbutils.widgets.get("schema_base_path").strip().rstrip("/")
CATALOG = dbutils.widgets.get("catalog").strip()
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema").strip()

if not all(
    [SOURCE_BASE_PATH, CHECKPOINT_BASE_PATH, SCHEMA_BASE_PATH, CATALOG, BRONZE_SCHEMA]
):
    raise ValueError("All notebook parameters must be non-empty")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Imports and ingestion-run identity

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import functions as F

BATCH_ID = str(uuid4())
BATCH_STARTED_AT_UTC = datetime.now(timezone.utc).isoformat()

print(f"Batch ID: {BATCH_ID}")
print(f"Source: {SOURCE_BASE_PATH}")
print(f"Target: {CATALOG}.{BRONZE_SCHEMA}")
print(f"Checkpoint root: {CHECKPOINT_BASE_PATH}")
print(f"Schema root: {SCHEMA_BASE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Dataset configuration
# MAGIC
# MAGIC The S3 files are currently stored directly under the `raw/` prefix.
# MAGIC Table names are defined separately from filenames so either can be changed
# MAGIC later without rewriting the ingestion function.

# COMMAND ----------

DATASETS = {
    "orders": {"file_name": "orders.csv"},
    "products": {"file_name": "products.csv"},
    "aisles": {"file_name": "aisles.csv"},
    "departments": {"file_name": "departments.csv"},
    "order_products_prior": {"file_name": "order_products_prior.csv"},
    "order_products_train": {"file_name": "order_products_train.csv"},
}


def quote_identifier(value):
    """Safely quote a catalog, schema, or table identifier."""
    return f"`{value.replace('`', '``')}`"


def qualified_table_name(table_name):
    return ".".join(
        [
            quote_identifier(CATALOG),
            quote_identifier(BRONZE_SCHEMA),
            quote_identifier(table_name),
        ]
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create the target schema
# MAGIC
# MAGIC The catalog must already exist and the execution identity must have
# MAGIC `USE CATALOG`, `USE SCHEMA`, and table-creation privileges.

# COMMAND ----------

spark.sql(f"USE CATALOG {quote_identifier(CATALOG)}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(BRONZE_SCHEMA)}")

print(f"Target schema is ready: {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validate source-file availability
# MAGIC
# MAGIC Stop before writing any tables if an expected source file is absent or an
# MAGIC unexpected CSV file is present. This protects the configuration from quiet
# MAGIC filename mistakes.

# COMMAND ----------

source_items = dbutils.fs.ls(SOURCE_BASE_PATH)
source_files = {
    item.name
    for item in source_items
    if not item.isDir() and item.name.lower().endswith(".csv")
}
expected_files = {config["file_name"] for config in DATASETS.values()}

missing_files = sorted(expected_files - source_files)
unexpected_files = sorted(source_files - expected_files)

file_inventory = [
    (item.name, item.path, item.size)
    for item in source_items
    if not item.isDir()
]
display(spark.createDataFrame(file_inventory, ["file_name", "file_path", "size_bytes"]))

if missing_files:
    raise FileNotFoundError(f"Expected source files are missing: {missing_files}")

if unexpected_files:
    raise ValueError(
        "Unexpected CSV files were found under the source prefix: "
        f"{unexpected_files}. Move them or add them to DATASETS."
    )

print(f"Validated {len(expected_files)} expected CSV files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Auto Loader ingestion function
# MAGIC
# MAGIC Each dataset has an independent checkpoint and schema-tracking directory.
# MAGIC `availableNow` processes all currently unprocessed files and then stops,
# MAGIC making this suitable for a scheduled Databricks Workflow.
# MAGIC
# MAGIC Do not delete checkpoints after a successful load. A fresh checkpoint can
# MAGIC cause existing files to be processed again.

# COMMAND ----------

def ingest_dataset(dataset_name, config):
    file_name = config["file_name"]
    target_table = qualified_table_name(dataset_name)
    checkpoint_path = f"{CHECKPOINT_BASE_PATH}/{dataset_name}"
    schema_path = f"{SCHEMA_BASE_PATH}/{dataset_name}"

    print(f"Starting {file_name} -> {target_table}")

    source_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("pathGlobFilter", file_name)
        .option("rescuedDataColumn", "_rescued_data")
        .load(SOURCE_BASE_PATH)
    )

    bronze_df = (
        source_df
        .withColumn("_ingest_batch_id", F.lit(BATCH_ID))
        .withColumn("_ingested_at_utc", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_file_name", F.col("_metadata.file_name"))
        .withColumn("_source_file_size", F.col("_metadata.file_size"))
        .withColumn("_source_file_modified_at", F.col("_metadata.file_modification_time"))
    )

    query = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
    )

    query.awaitTermination()

    if query.exception() is not None:
        raise RuntimeError(f"Auto Loader failed for {dataset_name}: {query.exception()}")

    progress = query.lastProgress or {}
    rows_processed = int(progress.get("numInputRows", 0))
    print(f"Completed {dataset_name}; rows processed in final micro-batch: {rows_processed}")

    return {
        "dataset": dataset_name,
        "source_file": file_name,
        "target_table": f"{CATALOG}.{BRONZE_SCHEMA}.{dataset_name}",
        "checkpoint_path": checkpoint_path,
        "status": "SUCCESS",
        "error_message": None,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Ingest all configured datasets
# MAGIC
# MAGIC A failure is captured for reporting, and the notebook fails after attempting
# MAGIC all datasets. Successful datasets retain their checkpoints and will not be
# MAGIC duplicated when the notebook is repaired and rerun.

# COMMAND ----------

results = []

for dataset_name, config in DATASETS.items():
    try:
        results.append(ingest_dataset(dataset_name, config))
    except Exception as error:
        error_message = str(error)
        print(f"FAILED: {dataset_name}: {error_message}")
        results.append(
            {
                "dataset": dataset_name,
                "source_file": config["file_name"],
                "target_table": f"{CATALOG}.{BRONZE_SCHEMA}.{dataset_name}",
                "checkpoint_path": f"{CHECKPOINT_BASE_PATH}/{dataset_name}",
                "status": "FAILED",
                "error_message": error_message,
            }
        )

result_df = spark.createDataFrame(results)
display(result_df.orderBy("dataset"))

failed_datasets = [row["dataset"] for row in results if row["status"] == "FAILED"]
if failed_datasets:
    raise RuntimeError(f"Bronze ingestion failed for: {failed_datasets}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Record the ingestion run

# COMMAND ----------

manifest_table = qualified_table_name("_ingestion_manifest")

manifest_df = (
    result_df
    .withColumn("batch_id", F.lit(BATCH_ID))
    .withColumn("batch_started_at_utc", F.lit(BATCH_STARTED_AT_UTC).cast("timestamp"))
    .withColumn("manifest_written_at_utc", F.current_timestamp())
)

(
    manifest_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(manifest_table)
)

display(spark.table(manifest_table).filter(F.col("batch_id") == BATCH_ID))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Verify Bronze tables and row counts
# MAGIC
# MAGIC On the first load of the standard Instacart dataset, compare the actual
# MAGIC counts with `expected_first_load_rows`. On subsequent runs, total table
# MAGIC counts may legitimately grow as new source files arrive.

# COMMAND ----------

expected_first_load_rows = {
    "orders": 3421083,
    "products": 49688,
    "aisles": 134,
    "departments": 21,
    "order_products_prior": 32434489,
    "order_products_train": 1384617,
}

count_rows = []

for dataset_name in DATASETS:
    actual_rows = spark.table(qualified_table_name(dataset_name)).count()
    expected_rows = expected_first_load_rows[dataset_name]
    count_rows.append(
        (
            dataset_name,
            actual_rows,
            expected_rows,
            actual_rows == expected_rows,
        )
    )

count_df = spark.createDataFrame(
    count_rows,
    ["table_name", "actual_rows", "expected_first_load_rows", "matches_first_load"],
)

display(count_df.orderBy("table_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Check rescued records
# MAGIC
# MAGIC Rescued data indicates input that did not match the inferred Bronze schema.
# MAGIC Review it before beginning Silver transformations.

# COMMAND ----------

rescued_rows = []

for dataset_name in DATASETS:
    table_df = spark.table(qualified_table_name(dataset_name))
    rescued_count = (
        table_df
        .filter(F.col("_rescued_data").isNotNull())
        .count()
        if "_rescued_data" in table_df.columns
        else 0
    )
    rescued_rows.append((dataset_name, rescued_count))

rescued_df = spark.createDataFrame(
    rescued_rows,
    ["table_name", "rescued_row_count"],
)
display(rescued_df.orderBy("table_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Completion
# MAGIC
# MAGIC Run this notebook a second time without uploading new files. The table row
# MAGIC counts should remain unchanged, demonstrating checkpoint-based idempotency.

# COMMAND ----------

completion = {
    "batch_id": BATCH_ID,
    "status": "SUCCESS",
    "source_path": SOURCE_BASE_PATH,
    "target_schema": f"{CATALOG}.{BRONZE_SCHEMA}",
    "datasets_configured": len(DATASETS),
    "manifest_table": f"{CATALOG}.{BRONZE_SCHEMA}._ingestion_manifest",
}

print(completion)

