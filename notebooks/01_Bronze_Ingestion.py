# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Instacart Bronze Ingestion
# MAGIC
# MAGIC Upload this single notebook as the replacement for the old Bronze notebooks.
# MAGIC It incrementally loads the six Instacart CSV files from Amazon S3 into
# MAGIC Unity Catalog Delta tables with Databricks Auto Loader.
# MAGIC
# MAGIC All Auto Loader state is stored on S3. This notebook deliberately rejects
# MAGIC `/schemas`, `/checkpoints`, `dbfs:/`, and other public-DBFS locations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters
# MAGIC
# MAGIC The `bronze_v3` state prefix is intentionally new, so it does not reuse the
# MAGIC failed state from earlier notebook attempts. Keep these state folders after
# MAGIC a successful run; they make later runs incremental and idempotent.

# COMMAND ----------

dbutils.widgets.text(
    "source_base_path",
    "s3://instakart-databricks-sandeep-2026/raw",
    "Source base path",
)
dbutils.widgets.text(
    "state_base_path",
    "s3://instakart-databricks-sandeep-2026/system/bronze_v3",
    "Auto Loader state path",
)
dbutils.widgets.text("catalog", "workspace", "Target catalog")
dbutils.widgets.text("bronze_schema", "instakart_bronze", "Bronze schema")

SOURCE_BASE_PATH = dbutils.widgets.get("source_base_path").strip().rstrip("/")
STATE_BASE_PATH = dbutils.widgets.get("state_base_path").strip().rstrip("/")
CATALOG = dbutils.widgets.get("catalog").strip()
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema").strip()

CHECKPOINT_BASE_PATH = f"{STATE_BASE_PATH}/checkpoints"
SCHEMA_BASE_PATH = f"{STATE_BASE_PATH}/schemas"


def require_s3_uri(label, path):
    if not path.lower().startswith("s3://"):
        raise ValueError(
            f"{label} must be an explicit s3:// URI, but received {path!r}. "
            "Public DBFS is disabled in this workspace."
        )


for path_label, configured_path in {
    "source_base_path": SOURCE_BASE_PATH,
    "state_base_path": STATE_BASE_PATH,
    "checkpoint_base_path": CHECKPOINT_BASE_PATH,
    "schema_base_path": SCHEMA_BASE_PATH,
}.items():
    require_s3_uri(path_label, configured_path)

if not CATALOG or not BRONZE_SCHEMA:
    raise ValueError("catalog and bronze_schema must be non-empty")

print(f"Source:       {SOURCE_BASE_PATH}")
print(f"Schema state: {SCHEMA_BASE_PATH}")
print(f"Checkpoints:  {CHECKPOINT_BASE_PATH}")
print(f"Target:       {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Imports and dataset configuration

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BATCH_ID = str(uuid4())
BATCH_STARTED_AT_UTC = datetime.now(timezone.utc)

DATASETS = {
    "orders": "orders.csv",
    "products": "products.csv",
    "aisles": "aisles.csv",
    "departments": "departments.csv",
    "order_products_prior": "order_products_prior.csv",
    "order_products_train": "order_products_train.csv",
}


def quote_identifier(value):
    return f"`{value.replace('`', '``')}`"


def qualified_table_name(table_name):
    return ".".join(
        [
            quote_identifier(CATALOG),
            quote_identifier(BRONZE_SCHEMA),
            quote_identifier(table_name),
        ]
    )


RESULT_SCHEMA = StructType(
    [
        StructField("dataset", StringType(), False),
        StructField("source_file", StringType(), False),
        StructField("target_table", StringType(), False),
        StructField("checkpoint_path", StringType(), False),
        StructField("rows_processed", LongType(), True),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate S3 access, files, and target schema
# MAGIC
# MAGIC This cell tests the actual state locations before any stream starts. If an
# MAGIC S3 permission is missing, the run stops here instead of silently falling
# MAGIC back to a DBFS-style path.

# COMMAND ----------

source_items = dbutils.fs.ls(SOURCE_BASE_PATH)
source_files = {
    item.name
    for item in source_items
    if not item.isDir() and item.name.lower().endswith(".csv")
}
expected_files = set(DATASETS.values())

missing_files = sorted(expected_files - source_files)
unexpected_files = sorted(source_files - expected_files)

inventory = [
    (item.name, item.path, item.size)
    for item in source_items
    if not item.isDir()
]
display(spark.createDataFrame(inventory, ["file_name", "file_path", "size_bytes"]))

if missing_files:
    raise FileNotFoundError(f"Expected source files are missing: {missing_files}")
if unexpected_files:
    raise ValueError(f"Unexpected CSV files found in raw/: {unexpected_files}")

# Verify that the execution identity can create/list both S3 state prefixes.
dbutils.fs.mkdirs(SCHEMA_BASE_PATH)
dbutils.fs.mkdirs(CHECKPOINT_BASE_PATH)
dbutils.fs.ls(SCHEMA_BASE_PATH)
dbutils.fs.ls(CHECKPOINT_BASE_PATH)

spark.sql(f"USE CATALOG {quote_identifier(CATALOG)}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(BRONZE_SCHEMA)}")

print(f"Validated all {len(DATASETS)} files and both S3 state locations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Auto Loader function
# MAGIC
# MAGIC Each dataset reads its exact file path and owns a separate S3 schema folder
# MAGIC and checkpoint. Bronze source fields stay as strings; casting, validation,
# MAGIC deduplication, and quarantine handling belong in Silver.

# COMMAND ----------

def ingest_dataset(dataset_name, file_name):
    source_path = SOURCE_BASE_PATH
    schema_path = f"{SCHEMA_BASE_PATH}/{dataset_name}"
    checkpoint_path = f"{CHECKPOINT_BASE_PATH}/{dataset_name}"
    target_table = qualified_table_name(dataset_name)

    # Defend against accidental path changes made through notebook widgets.
    require_s3_uri("source_path", source_path)
    require_s3_uri("schema_path", schema_path)
    require_s3_uri("checkpoint_path", checkpoint_path)

    print(f"\nStarting {dataset_name}")
    print(f"  input:      {source_path}")
    print(f"  schema:     {schema_path}")
    print(f"  checkpoint: {checkpoint_path}")
    print(f"  table:      {target_table}")

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
        .load(source_path)
    )

    bronze_df = (
        source_df
        .withColumn("_ingest_batch_id", F.lit(BATCH_ID))
        .withColumn("_ingested_at_utc", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_file_name", F.col("_metadata.file_name"))
        .withColumn("_source_file_size", F.col("_metadata.file_size"))
        .withColumn(
            "_source_file_modified_at",
            F.col("_metadata.file_modification_time"),
        )
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
        raise RuntimeError(str(query.exception()))

    rows_processed = sum(
        int(progress.get("numInputRows") or 0)
        for progress in query.recentProgress
    )
    print(f"Completed {dataset_name}: {rows_processed:,} rows processed")

    return (
        dataset_name,
        file_name,
        f"{CATALOG}.{BRONZE_SCHEMA}.{dataset_name}",
        checkpoint_path,
        rows_processed,
        "SUCCESS",
        None,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Run all datasets

# COMMAND ----------

result_rows = []

for dataset_name, file_name in DATASETS.items():
    try:
        result_rows.append(ingest_dataset(dataset_name, file_name))
    except Exception as error:
        error_message = str(error)
        print(f"FAILED {dataset_name}: {error_message}")
        result_rows.append(
            (
                dataset_name,
                file_name,
                f"{CATALOG}.{BRONZE_SCHEMA}.{dataset_name}",
                f"{CHECKPOINT_BASE_PATH}/{dataset_name}",
                None,
                "FAILED",
                error_message,
            )
        )

result_df = spark.createDataFrame(result_rows, RESULT_SCHEMA)
display(result_df.orderBy("dataset"))

failed_datasets = [
    row[0]
    for row in result_rows
    if row[5] == "FAILED"
]
if failed_datasets:
    raise RuntimeError(f"Bronze ingestion failed for: {failed_datasets}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Manifest and validation

# COMMAND ----------

manifest_table = qualified_table_name("_ingestion_manifest")

(
    result_df
    .withColumn("batch_id", F.lit(BATCH_ID))
    .withColumn(
        "batch_started_at_utc",
        F.lit(BATCH_STARTED_AT_UTC).cast(TimestampType()),
    )
    .withColumn("manifest_written_at_utc", F.current_timestamp())
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(manifest_table)
)

expected_first_load_rows = {
    "orders": 3421083,
    "products": 49688,
    "aisles": 134,
    "departments": 21,
    "order_products_prior": 32434489,
    "order_products_train": 1384617,
}

validation_rows = []
for dataset_name in DATASETS:
    table_df = spark.table(qualified_table_name(dataset_name))
    actual_rows = table_df.count()
    rescued_rows = (
        table_df.filter(F.col("_rescued_data").isNotNull()).count()
        if "_rescued_data" in table_df.columns
        else 0
    )
    expected_rows = expected_first_load_rows[dataset_name]
    validation_rows.append(
        (dataset_name, actual_rows, expected_rows, actual_rows == expected_rows, rescued_rows)
    )

validation_df = spark.createDataFrame(
    validation_rows,
    [
        "table_name",
        "actual_rows",
        "expected_first_load_rows",
        "matches_first_load",
        "rescued_row_count",
    ],
)
display(validation_df.orderBy("table_name"))
display(spark.table(manifest_table).filter(F.col("batch_id") == BATCH_ID))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Completion
# MAGIC
# MAGIC A rerun with no new files should report zero processed rows and leave table
# MAGIC counts unchanged. Do not delete the S3 `bronze_v3` state after success.

# COMMAND ----------

completion = {
    "batch_id": BATCH_ID,
    "status": "SUCCESS",
    "source_path": SOURCE_BASE_PATH,
    "state_path": STATE_BASE_PATH,
    "target_schema": f"{CATALOG}.{BRONZE_SCHEMA}",
    "datasets_loaded": len(DATASETS),
    "manifest_table": f"{CATALOG}.{BRONZE_SCHEMA}._ingestion_manifest",
}

print(completion)
dbutils.notebook.exit(str(completion))
