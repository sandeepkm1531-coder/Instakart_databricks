# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 - Consolidated Gold Layer
# MAGIC
# MAGIC Build dashboard-ready aggregates from the six validated Silver tables.
# MAGIC The notebook reads only Silver, writes Delta, and is rerunnable with overwrite.
# MAGIC
# MAGIC **Source limitation:** Instacart has no calendar order date. Therefore
# MAGIC `gold_order_trends` uses customer order sequence (`order_number`) as its grain.
# MAGIC Calendar day/week/month and month-over-month metrics must not be inferred from
# MAGIC day-of-week or `days_since_prior_order`; add a genuine `order_date` upstream to
# MAGIC enable those views.
# MAGIC
# MAGIC ### Published table grains
# MAGIC
# MAGIC | Delta table | Grain | Dashboard purpose |
# MAGIC |---|---|---|
# MAGIC | `gold_kpi_summary` | One row per refresh | Executive KPI cards |
# MAGIC | `gold_order_trends` | One row per customer order-sequence number | Lifecycle trend proxy |
# MAGIC | `gold_customer_metrics` | One row per customer | Segments and top customers |
# MAGIC | `gold_product_metrics` | One row per product | Product ranking and reorder analysis |
# MAGIC | `gold_department_metrics` | One row per department | Department contribution |
# MAGIC | `gold_aisle_metrics` | One row per aisle | Aisle contribution |
# MAGIC | `gold_shopping_behavior` | One row per day-of-week and hour | Heatmaps and shopping patterns |
# MAGIC
# MAGIC Rates are stored as decimals (for example, `0.6250` = 62.50%). Contribution
# MAGIC fields ending in `_pct` are stored from 0 to 100. `orders_with_item_detail`
# MAGIC is exposed because Instacart's `test` orders have no order-product rows.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("silver_schema", "instakart_silver", "Silver schema")
dbutils.widgets.text("gold_schema", "instakart_gold", "Gold schema")

CATALOG = dbutils.widgets.get("catalog").strip()
SILVER_SCHEMA = dbutils.widgets.get("silver_schema").strip()
GOLD_SCHEMA = dbutils.widgets.get("gold_schema").strip()

if not CATALOG or not SILVER_SCHEMA or not GOLD_SCHEMA:
    raise ValueError("catalog, silver_schema, and gold_schema must be non-empty")


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


def assert_unique(df, table_label, key_columns):
    duplicate_exists = (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
        > 0
    )
    if duplicate_exists:
        raise RuntimeError(
            f"{table_label} contains duplicate key values for {key_columns}"
        )


SOURCE_TABLES = {
    "orders": "silver_orders",
    "aisles": "silver_aisles",
    "departments": "silver_departments",
    "products": "silver_products",
    "prior": "silver_order_products_prior",
    "train": "silver_order_products_train",
}

for source_table in SOURCE_TABLES.values():
    qualified_source = table_name(SILVER_SCHEMA, source_table)
    if not spark.catalog.tableExists(qualified_source):
        raise ValueError(f"Required Silver table does not exist: {qualified_source}")

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(CATALOG)}.{quote_identifier(GOLD_SCHEMA)}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load and validate Silver inputs

# COMMAND ----------

orders_df = spark.table(table_name(SILVER_SCHEMA, SOURCE_TABLES["orders"]))
aisles_df = spark.table(table_name(SILVER_SCHEMA, SOURCE_TABLES["aisles"]))
departments_df = spark.table(
    table_name(SILVER_SCHEMA, SOURCE_TABLES["departments"])
)
products_df = spark.table(table_name(SILVER_SCHEMA, SOURCE_TABLES["products"]))
prior_df = spark.table(table_name(SILVER_SCHEMA, SOURCE_TABLES["prior"]))
train_df = spark.table(table_name(SILVER_SCHEMA, SOURCE_TABLES["train"]))

require_columns(
    orders_df,
    "silver_orders",
    [
        "order_id",
        "user_id",
        "eval_set",
        "order_number",
        "order_dow",
        "order_hour_of_day",
        "days_since_prior_order",
    ],
)
require_columns(aisles_df, "silver_aisles", ["aisle_id", "aisle"])
require_columns(
    departments_df,
    "silver_departments",
    ["department_id", "department"],
)
require_columns(
    products_df,
    "silver_products",
    ["product_id", "product_name", "aisle_id", "department_id"],
)
for name, df in [("silver_prior", prior_df), ("silver_train", train_df)]:
    require_columns(
        df,
        name,
        ["order_id", "product_id", "add_to_cart_order", "reordered"],
    )

assert_unique(orders_df, "silver_orders", ["order_id"])
assert_unique(aisles_df, "silver_aisles", ["aisle_id"])
assert_unique(departments_df, "silver_departments", ["department_id"])
assert_unique(products_df, "silver_products", ["product_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Gold dimensions and facts

# COMMAND ----------

product_dimension_df = (
    products_df.alias("p")
    .join(aisles_df.alias("a"), "aisle_id", "inner")
    .join(departments_df.alias("d"), "department_id", "inner")
    .select(
        "product_id",
        "product_name",
        "aisle_id",
        F.col("a.aisle").alias("aisle_name"),
        "department_id",
        F.col("d.department").alias("department_name"),
    )
    .withColumn("_gold_processed_at_utc", F.current_timestamp())
)

order_fact_df = (
    orders_df.select(
        "order_id",
        "user_id",
        "eval_set",
        "order_number",
        "order_dow",
        "order_hour_of_day",
        "days_since_prior_order",
    )
    .withColumn(
        "day_name",
        F.element_at(
            F.array(
                F.lit("Sunday"),
                F.lit("Monday"),
                F.lit("Tuesday"),
                F.lit("Wednesday"),
                F.lit("Thursday"),
                F.lit("Friday"),
                F.lit("Saturday"),
            ),
            F.col("order_dow") + 1,
        ),
    )
    .withColumn(
        "day_part",
        F.when(F.col("order_hour_of_day") < 6, "Overnight")
        .when(F.col("order_hour_of_day") < 12, "Morning")
        .when(F.col("order_hour_of_day") < 17, "Afternoon")
        .when(F.col("order_hour_of_day") < 21, "Evening")
        .otherwise("Night"),
    )
    .withColumn("_gold_processed_at_utc", F.current_timestamp())
)

order_items_df = (
    prior_df.select("order_id", "product_id", "add_to_cart_order", "reordered")
    .unionByName(
        train_df.select("order_id", "product_id", "add_to_cart_order", "reordered")
    )
)

order_item_fact_df = (
    order_items_df.alias("i")
    .join(
        orders_df.select(
            "order_id",
            "user_id",
            "eval_set",
            "order_number",
            "order_dow",
            "order_hour_of_day",
        ).alias("o"),
        "order_id",
        "inner",
    )
    .select(
        "order_id",
        "product_id",
        "user_id",
        "eval_set",
        "order_number",
        "order_dow",
        "order_hour_of_day",
        "add_to_cart_order",
        "reordered",
    )
    .withColumn("_gold_processed_at_utc", F.current_timestamp())
)

assert_unique(product_dimension_df, "product_dimension", ["product_id"])
assert_unique(
    order_item_fact_df,
    "order_item_fact",
    ["order_id", "product_id"],
)

expected_item_rows = prior_df.count() + train_df.count()
actual_item_rows = order_item_fact_df.count()
if actual_item_rows != expected_item_rows:
    raise RuntimeError(
        "Gold order-item row count does not match Silver prior plus train rows: "
        f"expected {expected_item_rows}, found {actual_item_rows}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Business-ready Gold aggregates

# COMMAND ----------

item_details_df = order_item_fact_df.join(
    product_dimension_df.drop("_gold_processed_at_utc"),
    "product_id",
    "inner",
)

orphan_product_exists = (
    order_item_fact_df.select("product_id")
    .distinct()
    .join(product_dimension_df.select("product_id"), "product_id", "left_anti")
    .limit(1)
    .count()
    > 0
)
if orphan_product_exists:
    raise RuntimeError(
        "Gold order items contain a product missing from the product dimension"
    )

product_rank_window = Window.partitionBy("department_id").orderBy(
    F.desc("items_ordered"), F.asc("product_id")
)
gold_product_metrics_df = (
    item_details_df.groupBy(
        "product_id",
        "product_name",
        "aisle_id",
        "aisle_name",
        "department_id",
        "department_name",
    )
    .agg(
        F.count("*").alias("items_ordered"),
        F.countDistinct("order_id").alias("orders"),
        F.countDistinct("user_id").alias("unique_customers"),
        F.sum("reordered").alias("reordered_items"),
        F.round(F.avg(F.col("reordered").cast("double")), 4).alias(
            "reorder_rate"
        ),
        F.round(F.avg("add_to_cart_order"), 2).alias("avg_cart_position"),
    )
    .withColumn("department_product_rank", F.row_number().over(product_rank_window))
    .withColumn("_gold_processed_at_utc", F.current_timestamp())
)

user_order_metrics_df = orders_df.groupBy("user_id").agg(
    F.count("*").alias("total_orders"),
    F.max("order_number").alias("lifetime_order_number"),
    F.sum(F.when(F.col("eval_set") == "prior", 1).otherwise(0)).alias(
        "prior_orders"
    ),
    F.sum(F.when(F.col("eval_set") == "train", 1).otherwise(0)).alias(
        "train_orders"
    ),
    F.sum(F.when(F.col("eval_set") == "test", 1).otherwise(0)).alias("test_orders"),
    F.round(F.avg("days_since_prior_order"), 2).alias(
        "avg_days_between_orders"
    ),
)

user_item_metrics_df = order_item_fact_df.groupBy("user_id").agg(
    F.countDistinct("order_id").alias("orders_with_item_detail"),
    F.count("*").alias("total_items"),
    F.countDistinct("product_id").alias("distinct_products"),
    F.sum("reordered").alias("reordered_items"),
    F.round(F.avg(F.col("reordered").cast("double")), 4).alias("reorder_rate"),
)

basket_metrics_df = (
    order_item_fact_df.groupBy("user_id", "order_id")
    .agg(F.count("*").alias("basket_size"))
    .groupBy("user_id")
    .agg(F.round(F.avg("basket_size"), 2).alias("avg_basket_size"))
)

gold_customer_metrics_df = (
    user_order_metrics_df
    .join(user_item_metrics_df, "user_id", "left")
    .join(basket_metrics_df, "user_id", "left")
    .fillna(
        0,
        subset=[
            "total_items",
            "orders_with_item_detail",
            "distinct_products",
            "reordered_items",
            "reorder_rate",
            "avg_basket_size",
        ],
    )
    .withColumn(
        "customer_segment",
        F.when(F.col("total_orders") <= 2, "New")
        .when(
            (F.col("total_orders") >= 10) & (F.col("reorder_rate") >= 0.50),
            "Loyal",
        )
        .when(F.col("avg_days_between_orders") >= 21, "At Risk")
        .otherwise("Active"),
    )
    .withColumn(
        "customer_rank",
        F.row_number().over(
            Window.orderBy(F.desc("total_orders"), F.asc("user_id"))
        ),
    )
    .withColumn("_gold_processed_at_utc", F.current_timestamp())
)

department_total_window = Window.partitionBy()
gold_department_metrics_df = (
    item_details_df.groupBy("department_id", "department_name")
    .agg(
        F.count("*").alias("items_ordered"),
        F.countDistinct("order_id").alias("orders"),
        F.countDistinct("user_id").alias("unique_customers"),
        F.countDistinct("product_id").alias("distinct_products_ordered"),
        F.round(F.avg(F.col("reordered").cast("double")), 4).alias(
            "reorder_rate"
        ),
    )
    .withColumn(
        "item_share_pct",
        F.round(
            F.lit(100.0)
            * F.col("items_ordered")
            / F.sum("items_ordered").over(department_total_window),
            4,
        ),
    )
    .withColumn("_gold_processed_at_utc", F.current_timestamp())
)

aisle_total_window = Window.partitionBy()
gold_aisle_metrics_df = (
    item_details_df.groupBy("aisle_id", "aisle_name")
    .agg(
        F.count("*").alias("items_ordered"),
        F.countDistinct("order_id").alias("orders"),
        F.countDistinct("user_id").alias("unique_customers"),
        F.countDistinct("product_id").alias("distinct_products_ordered"),
        F.round(F.avg(F.col("reordered").cast("double")), 4).alias(
            "reorder_rate"
        ),
    )
    .withColumn(
        "item_share_pct",
        F.round(
            F.lit(100.0)
            * F.col("items_ordered")
            / F.sum("items_ordered").over(aisle_total_window),
            4,
        ),
    )
    .withColumn("_gold_processed_at_utc", F.current_timestamp())
)

basket_by_order_df = order_item_fact_df.groupBy("order_id").agg(
    F.count("*").alias("basket_size"),
    F.sum("reordered").alias("reordered_items"),
)

shopping_base_df = (
    order_fact_df.drop("_gold_processed_at_utc")
    .join(basket_by_order_df, "order_id", "left")
    .fillna(0, subset=["basket_size", "reordered_items"])
)

gold_shopping_behavior_df = (
    shopping_base_df.groupBy(
        "order_dow", "day_name", "order_hour_of_day", "day_part"
    )
    .agg(
        F.countDistinct("order_id").alias("orders"),
        F.countDistinct("user_id").alias("unique_customers"),
        F.sum("basket_size").alias("items_ordered"),
        F.round(
            F.avg(F.when(F.col("basket_size") > 0, F.col("basket_size"))), 2
        ).alias("avg_basket_size"),
        F.sum("reordered_items").alias("reordered_items"),
        F.round(F.avg("days_since_prior_order"), 2).alias(
            "avg_days_since_prior_order"
        ),
    )
    .withColumn(
        "reorder_rate",
        F.when(
            F.col("items_ordered") > 0,
            F.round(F.col("reordered_items") / F.col("items_ordered"), 4),
        ),
    )
    .withColumn("_gold_processed_at_utc", F.current_timestamp())
)

orders_by_sequence_df = orders_df.groupBy(
    F.col("order_number").alias("order_sequence_number")
).agg(
    F.countDistinct("order_id").alias("orders"),
    F.countDistinct("user_id").alias("unique_customers"),
)

items_by_sequence_df = order_item_fact_df.groupBy(
    F.col("order_number").alias("order_sequence_number")
).agg(
    F.countDistinct("order_id").alias("orders_with_item_detail"),
    F.count("*").alias("items_ordered"),
    F.sum("reordered").alias("reordered_items"),
    F.round(F.avg("add_to_cart_order"), 2).alias("avg_cart_position"),
)

gold_order_trends_df = (
    orders_by_sequence_df
    .join(items_by_sequence_df, "order_sequence_number", "left")
    .fillna(
        0,
        subset=["orders_with_item_detail", "items_ordered", "reordered_items"],
    )
    .withColumn(
        "avg_products_per_known_order",
        F.when(
            F.col("orders_with_item_detail") > 0,
            F.round(F.col("items_ordered") / F.col("orders_with_item_detail"), 2),
        ),
    )
    .withColumn(
        "reorder_rate",
        F.when(
            F.col("items_ordered") > 0,
            F.round(F.col("reordered_items") / F.col("items_ordered"), 4),
        ),
    )
    .withColumn("trend_grain", F.lit("customer_order_sequence"))
    .withColumn("_gold_processed_at_utc", F.current_timestamp())
)

order_kpis_df = orders_df.agg(
    F.countDistinct("order_id").alias("total_orders"),
    F.countDistinct("user_id").alias("total_customers"),
)
item_kpis_df = order_item_fact_df.agg(
    F.countDistinct("order_id").alias("orders_with_item_detail"),
    F.count("*").alias("total_products_ordered"),
    F.countDistinct("product_id").alias("unique_products_purchased"),
    F.sum("reordered").alias("reordered_products"),
)

gold_kpi_summary_df = (
    order_kpis_df.crossJoin(item_kpis_df)
    .withColumn(
        "average_basket_size",
        F.round(
            F.col("total_products_ordered") / F.col("orders_with_item_detail"),
            2,
        ),
    )
    .withColumn(
        "average_orders_per_customer",
        F.round(F.col("total_orders") / F.col("total_customers"), 2),
    )
    .withColumn(
        "reorder_rate",
        F.round(F.col("reordered_products") / F.col("total_products_ordered"), 4),
    )
    .withColumn("_gold_processed_at_utc", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Publish Gold tables and report completion

# COMMAND ----------

OUTPUTS = {
    "gold_kpi_summary": gold_kpi_summary_df,
    "gold_order_trends": gold_order_trends_df,
    "gold_customer_metrics": gold_customer_metrics_df,
    "gold_product_metrics": gold_product_metrics_df,
    "gold_department_metrics": gold_department_metrics_df,
    "gold_aisle_metrics": gold_aisle_metrics_df,
    "gold_shopping_behavior": gold_shopping_behavior_df,
}

summary_rows = []
for target_name, output_df in OUTPUTS.items():
    target_table = table_name(GOLD_SCHEMA, target_name)
    (
        output_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )
    row_count = spark.table(target_table).count()
    summary_rows.append((target_name, target_table, row_count))

summary_df = spark.createDataFrame(
    summary_rows,
    ["dataset", "gold_table", "row_count"],
)
display(summary_df.orderBy("dataset"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard mapping and refresh strategy
# MAGIC
# MAGIC - **Executive overview:** `gold_kpi_summary`
# MAGIC - **Order lifecycle trend:** `gold_order_trends`; replace with calendar grains
# MAGIC   only after Silver receives a real `order_date`
# MAGIC - **Customers:** `gold_customer_metrics`; filter `customer_rank <= 10` for top
# MAGIC   customers. Segment rules are New (1-2 orders), Loyal (10+ orders and 50%+
# MAGIC   reorder rate), At Risk (3+ orders with 21+ average days between orders),
# MAGIC   and Active (remaining customers).
# MAGIC - **Products:** `gold_product_metrics`; order by `items_ordered` for top products,
# MAGIC   by `reordered_items` for top reordered products, or filter
# MAGIC   `department_product_rank <= 10` for department leaders.
# MAGIC - **Departments / aisles:** contribution, reach, and reorder rate come from
# MAGIC   `gold_department_metrics` and `gold_aisle_metrics`.
# MAGIC - **Shopping behavior:** use `gold_shopping_behavior` directly as a
# MAGIC   day-of-week x hour heatmap dataset.
# MAGIC
# MAGIC This notebook uses a **full overwrite refresh**, which is appropriate for the
# MAGIC static Instacart dataset and guarantees repeatable results. For production
# MAGIC append-only data, preserve these table grains but incrementally `MERGE` facts,
# MAGIC recompute affected aggregate keys, and orchestrate Bronze -> Silver -> Gold in
# MAGIC a Databricks Workflow. All outputs include `_gold_processed_at_utc` for freshness.

completion = {
    "status": "SUCCESS",
    "source_schema": f"{CATALOG}.{SILVER_SCHEMA}",
    "target_schema": f"{CATALOG}.{GOLD_SCHEMA}",
    "datasets_published": len(OUTPUTS),
}
print(completion)
dbutils.notebook.exit(str(completion))
