# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 - Data Ingestion
# MAGIC
# MAGIC Ingest raw Instakart CSV files into bronze Delta tables.
# MAGIC
# MAGIC Expected behavior:
# MAGIC - Reads every CSV file from the configured raw source path.
# MAGIC - Writes one bronze Delta table per source file.
# MAGIC - Adds ingestion audit columns for traceability.
# MAGIC - Creates a bronze ingestion manifest table.

# COMMAND ----------

from datetime import datetime, timezone
import re
from uuid import uuid4

from pyspark.sql import functions as F

# COMMAND ----------

try:
    dbutils.widgets.text("source_path", "/FileStore/instakart/raw", "Raw source path")
    dbutils.widgets.text("catalog", "workspace", "Target catalog")
    dbutils.widgets.text("schema", "instakart_bronze", "Target schema")
    dbutils.widgets.dropdown("write_mode", "overwrite", ["overwrite", "append"], "Write mode")
    dbutils.widgets.dropdown("infer_schema", "true", ["true", "false"], "Infer CSV schema")
except NameError:
    raise RuntimeError("This notebook is intended to run on Databricks where dbutils is available.")

SOURCE_PATH = dbutils.widgets.get("source_path").rstrip("/")
CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
WRITE_MODE = dbutils.widgets.get("write_mode").strip().lower()
INFER_SCHEMA = dbutils.widgets.get("infer_schema").strip().lower() == "true"
BATCH_ID = str(uuid4())
BATCH_STARTED_AT = datetime.now(timezone.utc).isoformat()

print(f"Batch id: {BATCH_ID}")
print(f"Source path: {SOURCE_PATH}")
print(f"Target: {CATALOG}.{SCHEMA}")
print(f"Write mode: {WRITE_MODE}")

# COMMAND ----------

def quote_identifier(identifier):
    return f"`{identifier.replace('`', '``')}`"


def qualified_table_name(table_name):
    if CATALOG == "hive_metastore":
        return f"{quote_identifier(SCHEMA)}.{quote_identifier(table_name)}"
    return f"{quote_identifier(CATALOG)}.{quote_identifier(SCHEMA)}.{quote_identifier(table_name)}"


def normalize_table_name(path):
    file_name = path.rstrip("/").split("/")[-1]
    stem = re.sub(r"\.[^.]+$", "", file_name)
    normalized = re.sub(r"[^A-Za-z0-9_]+", "_", stem).strip("_").lower()
    if not normalized:
        raise ValueError(f"Could not derive table name from path: {path}")
    if normalized[0].isdigit():
        normalized = f"t_{normalized}"
    return normalized


def list_csv_files(path):
    csv_files = []
    pending = [path]

    while pending:
        current_path = pending.pop()
        for item in dbutils.fs.ls(current_path):
            if item.isDir():
                pending.append(item.path)
            elif item.path.lower().endswith(".csv"):
                csv_files.append(item.path)

    return sorted(csv_files)

# COMMAND ----------

if CATALOG != "hive_metastore":
    spark.sql(f"USE CATALOG {quote_identifier(CATALOG)}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(SCHEMA)}")

csv_files = list_csv_files(SOURCE_PATH)

if not csv_files:
    raise FileNotFoundError(
        f"No CSV files found under {SOURCE_PATH}. Upload raw Instakart files there or change the source_path widget."
    )

display(spark.createDataFrame([(path,) for path in csv_files], ["source_file"]))

# COMMAND ----------

read_options = {
    "header": "true",
    "inferSchema": str(INFER_SCHEMA).lower(),
    "multiLine": "true",
    "escape": '"',
    "quote": '"',
    "mode": "PERMISSIVE",
}

manifest_rows = []

for source_file in csv_files:
    table_name = normalize_table_name(source_file)
    target_table = qualified_table_name(table_name)

    raw_df = spark.read.options(**read_options).csv(source_file)

    bronze_df = (
        raw_df
        .withColumn("_ingest_batch_id", F.lit(BATCH_ID))
        .withColumn("_ingested_at_utc", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

    (
        bronze_df.write
        .format("delta")
        .mode(WRITE_MODE)
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )

    row_count = spark.table(target_table).count()
    manifest_rows.append(
        {
            "batch_id": BATCH_ID,
            "batch_started_at_utc": BATCH_STARTED_AT,
            "source_file": source_file,
            "target_table": target_table.replace("`", ""),
            "write_mode": WRITE_MODE,
            "row_count_after_write": row_count,
            "status": "loaded",
        }
    )

    print(f"Loaded {source_file} -> {target_table} ({row_count} rows after write)")

# COMMAND ----------

manifest_df = spark.createDataFrame(manifest_rows)
manifest_table = qualified_table_name("_ingestion_manifest")

(
    manifest_df
    .withColumn("manifest_written_at_utc", F.current_timestamp())
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(manifest_table)
)

display(spark.table(manifest_table).where(F.col("batch_id") == BATCH_ID))

# COMMAND ----------

dbutils.notebook.exit(
    {
        "batch_id": BATCH_ID,
        "source_path": SOURCE_PATH,
        "target_schema": f"{CATALOG}.{SCHEMA}",
        "files_loaded": len(csv_files),
        "manifest_table": manifest_table.replace("`", ""),
    }
)
