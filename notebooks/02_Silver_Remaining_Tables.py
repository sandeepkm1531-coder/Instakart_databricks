# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 02B - Silver Cleaning for Product and Order-Line Data
# MAGIC
# MAGIC Clean the five additional Bronze datasets, enforce key relationships, and
# MAGIC publish valid records plus quarantine tables in `instakart_silver`.
# MAGIC Run `02_Data_Cleaning.py` first so validated Silver orders are available.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("bronze_schema", "instakart_bronze", "Bronze schema")
dbutils.widgets.text("silver_schema", "instakart_silver", "Silver schema")

CATALOG = dbutils.widgets.get("catalog").strip()
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema").strip()
SILVER_SCHEMA = dbutils.widgets.get("silver_schema").strip()

if not CATALOG or not BRONZE_SCHEMA or not SILVER_SCHEMA:
    raise ValueError("catalog, bronze_schema, and silver_schema must be non-empty")


def quote_identifier(value):
    return f"`{value.replace('`', '``')}`"


def table_name(schema, table):
    return ".".join(
        [quote_identifier(CATALOG), quote_identifier(schema), quote_identifier(table)]
    )


def require_columns(df, source_name, required_columns):
    missing = sorted(set(required_columns) - set(df.columns))
    if missing:
        raise ValueError(f"{source_name} is missing required columns: {missing}")


def add_duplicate_rank(df, key_columns):
    ordering_columns = []
    if "_ingested_at_utc" in df.columns:
        ordering_columns.append(F.col("_ingested_at_utc").desc_nulls_last())
    if "_source_file" in df.columns:
        ordering_columns.append(F.col("_source_file").desc_nulls_last())
    ordering_columns.append(F.monotonically_increasing_id())
    window = Window.partitionBy(*key_columns).orderBy(*ordering_columns)
    return df.withColumn("_duplicate_rank", F.row_number().over(window))


def split_valid_and_quarantine(checked_df):
    valid_df = (
        checked_df.filter(F.col("_rejection_reason") == "")
        .drop("_duplicate_rank", "_rejection_reason")
        .withColumn("_silver_processed_at_utc", F.current_timestamp())
    )
    quarantine_df = (
        checked_df.filter(F.col("_rejection_reason") != "")
        .drop("_duplicate_rank")
        .withColumn("_quarantined_at_utc", F.current_timestamp())
    )
    return valid_df, quarantine_df


SOURCE_TABLES = {
    "aisles": "bronze_aisles",
    "departments": "bronze_departments",
    "products": "bronze_products",
    "order_products_prior": "bronze_order_products_prior",
    "order_products_train": "bronze_order_products_train",
}

for source_table in SOURCE_TABLES.values():
    qualified_source = table_name(BRONZE_SCHEMA, source_table)
    if not spark.catalog.tableExists(qualified_source):
        raise ValueError(f"Required Bronze table does not exist: {qualified_source}")

silver_orders_table = table_name(SILVER_SCHEMA, "orders")
if not spark.catalog.tableExists(silver_orders_table):
    raise ValueError(
        f"Required Silver table does not exist: {silver_orders_table}. "
        "Run 02_Data_Cleaning.py first."
    )

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(CATALOG)}.{quote_identifier(SILVER_SCHEMA)}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Clean aisles and departments

# COMMAND ----------

bronze_aisles_df = spark.table(table_name(BRONZE_SCHEMA, SOURCE_TABLES["aisles"]))
require_columns(bronze_aisles_df, "bronze_aisles", ["aisle_id", "aisle"])

aisles_typed_df = (
    bronze_aisles_df
    .withColumn("aisle_id", F.col("aisle_id").cast("int"))
    .withColumn("aisle", F.trim(F.col("aisle").cast("string")))
)
aisles_ranked_df = add_duplicate_rank(aisles_typed_df, ["aisle_id"])
aisles_checked_df = aisles_ranked_df.withColumn(
    "_rejection_reason",
    F.concat_ws(
        "; ",
        F.when(F.col("aisle_id").isNull() | (F.col("aisle_id") <= 0), "invalid aisle_id"),
        F.when(F.col("aisle").isNull() | (F.length("aisle") == 0), "aisle is empty"),
        F.when(F.col("_duplicate_rank") > 1, "duplicate aisle_id"),
    ),
)
silver_aisles_df, quarantine_aisles_df = split_valid_and_quarantine(aisles_checked_df)

bronze_departments_df = spark.table(
    table_name(BRONZE_SCHEMA, SOURCE_TABLES["departments"])
)
require_columns(
    bronze_departments_df,
    "bronze_departments",
    ["department_id", "department"],
)

departments_typed_df = (
    bronze_departments_df
    .withColumn("department_id", F.col("department_id").cast("int"))
    .withColumn("department", F.trim(F.col("department").cast("string")))
)
departments_ranked_df = add_duplicate_rank(departments_typed_df, ["department_id"])
departments_checked_df = departments_ranked_df.withColumn(
    "_rejection_reason",
    F.concat_ws(
        "; ",
        F.when(
            F.col("department_id").isNull() | (F.col("department_id") <= 0),
            "invalid department_id",
        ),
        F.when(
            F.col("department").isNull() | (F.length("department") == 0),
            "department is empty",
        ),
        F.when(F.col("_duplicate_rank") > 1, "duplicate department_id"),
    ),
)
silver_departments_df, quarantine_departments_df = split_valid_and_quarantine(
    departments_checked_df
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Clean products and enforce dimension references

# COMMAND ----------

bronze_products_df = spark.table(table_name(BRONZE_SCHEMA, SOURCE_TABLES["products"]))
require_columns(
    bronze_products_df,
    "bronze_products",
    ["product_id", "product_name", "aisle_id", "department_id"],
)

products_typed_df = (
    bronze_products_df
    .withColumn("product_id", F.col("product_id").cast("long"))
    .withColumn("product_name", F.trim(F.col("product_name").cast("string")))
    .withColumn("aisle_id", F.col("aisle_id").cast("int"))
    .withColumn("department_id", F.col("department_id").cast("int"))
)
products_ranked_df = add_duplicate_rank(products_typed_df, ["product_id"])

valid_aisle_keys = silver_aisles_df.select("aisle_id").withColumn(
    "_aisle_exists", F.lit(True)
)
valid_department_keys = silver_departments_df.select("department_id").withColumn(
    "_department_exists", F.lit(True)
)

products_checked_df = (
    products_ranked_df
    .join(valid_aisle_keys, "aisle_id", "left")
    .join(valid_department_keys, "department_id", "left")
    .withColumn(
        "_rejection_reason",
        F.concat_ws(
            "; ",
            F.when(
                F.col("product_id").isNull() | (F.col("product_id") <= 0),
                "invalid product_id",
            ),
            F.when(
                F.col("product_name").isNull() | (F.length("product_name") == 0),
                "product_name is empty",
            ),
            F.when(F.col("aisle_id").isNull(), "invalid aisle_id"),
            F.when(F.col("department_id").isNull(), "invalid department_id"),
            F.when(F.col("_aisle_exists").isNull(), "aisle_id not found in valid aisles"),
            F.when(
                F.col("_department_exists").isNull(),
                "department_id not found in valid departments",
            ),
            F.when(F.col("_duplicate_rank") > 1, "duplicate product_id"),
        ),
    )
    .drop("_aisle_exists", "_department_exists")
)
silver_products_df, quarantine_products_df = split_valid_and_quarantine(
    products_checked_df
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Clean prior and train order-product rows

# COMMAND ----------

silver_order_keys_df = spark.table(silver_orders_table).select("order_id", "eval_set")
silver_product_keys_df = silver_products_df.select("product_id").withColumn(
    "_product_exists", F.lit(True)
)


def clean_order_products(source_table, expected_eval_set):
    bronze_df = spark.table(table_name(BRONZE_SCHEMA, source_table))
    require_columns(
        bronze_df,
        source_table,
        ["order_id", "product_id", "add_to_cart_order", "reordered"],
    )

    typed_df = (
        bronze_df
        .withColumn("order_id", F.col("order_id").cast("long"))
        .withColumn("product_id", F.col("product_id").cast("long"))
        .withColumn("add_to_cart_order", F.col("add_to_cart_order").cast("int"))
        .withColumn("reordered", F.col("reordered").cast("int"))
    )
    ranked_df = add_duplicate_rank(typed_df, ["order_id", "product_id"])
    order_keys_df = silver_order_keys_df.select(
        "order_id", F.col("eval_set").alias("_order_eval_set")
    )

    checked_df = (
        ranked_df
        .join(order_keys_df, "order_id", "left")
        .join(silver_product_keys_df, "product_id", "left")
        .withColumn(
            "_rejection_reason",
            F.concat_ws(
                "; ",
                F.when(F.col("order_id").isNull(), "invalid order_id"),
                F.when(F.col("product_id").isNull(), "invalid product_id"),
                F.when(
                    F.col("add_to_cart_order").isNull()
                    | (F.col("add_to_cart_order") <= 0),
                    "invalid add_to_cart_order",
                ),
                F.when(~F.col("reordered").isin(0, 1) | F.col("reordered").isNull(), "reordered must be 0 or 1"),
                F.when(F.col("_order_eval_set").isNull(), "order_id not found in Silver orders"),
                F.when(
                    F.col("_order_eval_set").isNotNull()
                    & (F.col("_order_eval_set") != expected_eval_set),
                    f"order does not belong to {expected_eval_set} eval_set",
                ),
                F.when(F.col("_product_exists").isNull(), "product_id not found in valid products"),
                F.when(F.col("_duplicate_rank") > 1, "duplicate order_id and product_id"),
            ),
        )
        .drop("_order_eval_set", "_product_exists")
    )
    return bronze_df, split_valid_and_quarantine(checked_df)


bronze_prior_df, (silver_prior_df, quarantine_prior_df) = clean_order_products(
    SOURCE_TABLES["order_products_prior"], "prior"
)
bronze_train_df, (silver_train_df, quarantine_train_df) = clean_order_products(
    SOURCE_TABLES["order_products_train"], "train"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Publish Silver and quarantine tables

# COMMAND ----------

OUTPUTS = {
    "silver_aisles": (bronze_aisles_df, silver_aisles_df, quarantine_aisles_df),
    "silver_departments": (
        bronze_departments_df,
        silver_departments_df,
        quarantine_departments_df,
    ),
    "silver_products": (bronze_products_df, silver_products_df, quarantine_products_df),
    "silver_order_products_prior": (
        bronze_prior_df,
        silver_prior_df,
        quarantine_prior_df,
    ),
    "silver_order_products_train": (
        bronze_train_df,
        silver_train_df,
        quarantine_train_df,
    ),
}

summary_rows = []
for target_name, (source_df, valid_df, quarantine_df) in OUTPUTS.items():
    valid_table = table_name(SILVER_SCHEMA, target_name)
    quarantine_table = table_name(SILVER_SCHEMA, f"{target_name}_quarantine")

    (
        valid_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(valid_table)
    )
    (
        quarantine_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(quarantine_table)
    )

    source_count = source_df.count()
    valid_count = spark.table(valid_table).count()
    quarantine_count = spark.table(quarantine_table).count()
    if source_count != valid_count + quarantine_count:
        raise RuntimeError(f"Row-count reconciliation failed for {target_name}")

    summary_rows.append(
        (target_name, source_count, valid_count, quarantine_count, valid_table, quarantine_table)
    )

summary_df = spark.createDataFrame(
    summary_rows,
    [
        "dataset",
        "bronze_rows",
        "silver_rows",
        "quarantined_rows",
        "silver_table",
        "quarantine_table",
    ],
)
display(summary_df.orderBy("dataset"))

completion = {
    "status": "SUCCESS",
    "source_schema": f"{CATALOG}.{BRONZE_SCHEMA}",
    "target_schema": f"{CATALOG}.{SILVER_SCHEMA}",
    "datasets_published": len(OUTPUTS),
}
print(completion)
dbutils.notebook.exit(str(completion))

