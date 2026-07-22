# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 - Silver Data Cleaning
# MAGIC
# MAGIC Clean and validate the Bronze `orders` table, then write:
# MAGIC
# MAGIC - valid, deduplicated rows to `instakart_silver.orders`
# MAGIC - rejected rows to `instakart_silver.orders_quarantine`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("bronze_schema", "instakart_bronze", "Bronze schema")
dbutils.widgets.text("silver_schema", "instakart_silver", "Silver schema")
dbutils.widgets.text("source_table", "orders", "Source table")

CATALOG = dbutils.widgets.get("catalog").strip()
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema").strip()
SILVER_SCHEMA = dbutils.widgets.get("silver_schema").strip()
SOURCE_TABLE = dbutils.widgets.get("source_table").strip()


def quote_identifier(value):
    return f"`{value.replace('`', '``')}`"


def table_name(schema, table):
    return ".".join(
        [quote_identifier(CATALOG), quote_identifier(schema), quote_identifier(table)]
    )


BRONZE_ORDERS = table_name(BRONZE_SCHEMA, SOURCE_TABLE)
SILVER_ORDERS = table_name(SILVER_SCHEMA, "orders")
QUARANTINE_ORDERS = table_name(SILVER_SCHEMA, "orders_quarantine")

print(f"Reading: {BRONZE_ORDERS}")
print(f"Valid output: {SILVER_ORDERS}")
print(f"Quarantine output: {QUARANTINE_ORDERS}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(CATALOG)}.{quote_identifier(SILVER_SCHEMA)}")

bronze_df = spark.table(BRONZE_ORDERS)

required_columns = {
    "order_id",
    "user_id",
    "eval_set",
    "order_number",
    "order_dow",
    "order_hour_of_day",
    "days_since_prior_order",
}
missing_columns = sorted(required_columns - set(bronze_df.columns))

if missing_columns:
    raise ValueError(f"Bronze orders is missing required columns: {missing_columns}")

# COMMAND ----------

# Standardize business-column types while retaining all ingestion audit columns.
typed_df = (
    bronze_df
    .withColumn("order_id", F.col("order_id").cast("long"))
    .withColumn("user_id", F.col("user_id").cast("long"))
    .withColumn("eval_set", F.lower(F.trim(F.col("eval_set").cast("string"))))
    .withColumn("order_number", F.col("order_number").cast("int"))
    .withColumn("order_dow", F.col("order_dow").cast("int"))
    .withColumn("order_hour_of_day", F.col("order_hour_of_day").cast("int"))
    .withColumn("days_since_prior_order", F.col("days_since_prior_order").cast("double"))
)

# Keep the newest copy when the same order_id occurs more than once.
ordering_columns = []
if "_ingested_at_utc" in typed_df.columns:
    ordering_columns.append(F.col("_ingested_at_utc").desc_nulls_last())
if "_source_file" in typed_df.columns:
    ordering_columns.append(F.col("_source_file").desc_nulls_last())
ordering_columns.append(F.monotonically_increasing_id())

dedupe_window = Window.partitionBy("order_id").orderBy(*ordering_columns)
checked_df = typed_df.withColumn("_duplicate_rank", F.row_number().over(dedupe_window))

# concat_ws ignores nulls, producing an empty string for rows with no violations.
checked_df = checked_df.withColumn(
    "_rejection_reason",
    F.concat_ws(
        "; ",
        F.when(F.col("order_id").isNull(), F.lit("order_id is null or invalid")),
        F.when(F.col("user_id").isNull(), F.lit("user_id is null or invalid")),
        F.when(
            F.col("eval_set").isNull() | ~F.col("eval_set").isin("prior", "train", "test"),
            F.lit("invalid eval_set"),
        ),
        F.when(F.col("order_number").isNull() | (F.col("order_number") <= 0), F.lit("invalid order_number")),
        F.when(F.col("order_dow").isNull() | ~F.col("order_dow").between(0, 6), F.lit("order_dow must be 0-6")),
        F.when(F.col("order_hour_of_day").isNull() | ~F.col("order_hour_of_day").between(0, 23), F.lit("order_hour_of_day must be 0-23")),
        F.when(F.col("days_since_prior_order") < 0, F.lit("days_since_prior_order cannot be negative")),
        F.when(F.col("_duplicate_rank") > 1, F.lit("duplicate order_id")),
    ),
)

# COMMAND ----------

silver_df = (
    checked_df
    .filter(F.col("_rejection_reason") == "")
    .drop("_duplicate_rank", "_rejection_reason")
    .withColumn("_silver_processed_at_utc", F.current_timestamp())
)

quarantine_df = (
    checked_df
    .filter(F.col("_rejection_reason") != "")
    .drop("_duplicate_rank")
    .withColumn("_quarantined_at_utc", F.current_timestamp())
)

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_ORDERS)
)

(
    quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(QUARANTINE_ORDERS)
)

# COMMAND ----------

source_count = bronze_df.count()
valid_count = silver_df.count()
quarantine_count = quarantine_df.count()

summary_df = spark.createDataFrame(
    [(source_count, valid_count, quarantine_count)],
    ["bronze_rows", "silver_rows", "quarantined_rows"],
)
display(summary_df)

if source_count != valid_count + quarantine_count:
    raise RuntimeError("Row-count reconciliation failed")

display(spark.table(SILVER_ORDERS).limit(20))
display(spark.table(QUARANTINE_ORDERS).limit(20))

