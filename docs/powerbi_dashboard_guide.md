# Instacart Gold - Power BI Dashboard Build Guide

## Status: alternative dashboard design

The implemented project dashboard uses Google Sheets and Looker Studio.
See [Looker Studio dashboard notes](looker_studio_dashboard.md). This Power BI
guide is retained as an earlier alternative design.

## Scope and metric rules

The report has four pages: Executive Overview, Customer Behaviour, Product
Performance, and Shopping Behaviour. It uses only tables in
`workspace.instakart_gold`. It must not display sales, revenue, calendar trends,
monthly growth, or forecasts because those facts do not exist in the source.

`total_orders` includes all Instacart order records. `orders_with_item_detail`
excludes test orders because Instacart does not publish order-product rows for
the test set. Basket-size and item-based measures must therefore use
`orders_with_item_detail` as their denominator.

## Connect Power BI to Databricks

Power BI Desktop requires Windows. Use the Windows laptop for the `.pbix`, or
use the Power BI service in a browser if a Databricks-backed semantic model has
already been published.

1. In Databricks, run `03_Gold_Layer.py` and confirm that all seven tables were
   published successfully.
2. Start a Databricks SQL warehouse.
3. Open its **Connection details** and copy **Server hostname** and **HTTP path**.
4. In Power BI Desktop, select **Get data > Azure Databricks**.
5. Enter the server hostname and HTTP path, then authenticate with the method
   supported by the workspace.
6. Choose **Import** mode. The Gold tables are already aggregated and the
   Instacart dataset is static, so Import gives faster report interaction and
   avoids keeping the SQL warehouse active for every click.
7. Select only these tables from `workspace.instakart_gold`:
   - `gold_kpi_summary`
   - `gold_order_trends`
   - `gold_customer_metrics`
   - `gold_product_metrics`
   - `gold_department_metrics`
   - `gold_aisle_metrics`
   - `gold_shopping_behavior`

## Semantic model

Keep the aggregate tables disconnected except for these two single-direction,
one-to-many relationships:

- `gold_department_metrics[department_id]` (1) to
  `gold_product_metrics[department_id]` (*)
- `gold_aisle_metrics[aisle_id]` (1) to
  `gold_product_metrics[aisle_id]` (*)

Do not relate tables merely because they contain similarly named aggregate
columns such as `orders` or `unique_customers`; that creates invalid
many-to-many filtering. Use page-local slicers from the table feeding that page.

Hide the technical `gold_updated_at` columns from report view.
Keep one visible refresh timestamp from `gold_kpi_summary`. Sort
`gold_shopping_behavior[day_name]` by `order_dow`.

Recommended formats:

- Counts: whole number with thousands separator
- Basket size and average order counts: two decimals
- `reorder_rate`: percentage with one decimal
- `item_share_pct`: number from 0 to 100; use the custom format `0.00\%`
  rather than Power BI's percentage data type
- Timestamps: local date and time

## DAX measures

Create these measures. A dedicated empty `_Measures` table is optional.

```DAX
Total Orders =
MAX ( gold_kpi_summary[total_orders] )

Total Customers =
MAX ( gold_kpi_summary[total_customers] )

Total Items =
MAX ( gold_kpi_summary[total_products_ordered] )

Average Basket Size =
MAX ( gold_kpi_summary[average_basket_size] )

Average Orders per Customer =
MAX ( gold_kpi_summary[average_orders_per_customer] )

Overall Reorder Rate =
MAX ( gold_kpi_summary[reorder_rate] )

Unique Products Purchased =
MAX ( gold_kpi_summary[unique_products_purchased] )

Customer Count =
DISTINCTCOUNT ( gold_customer_metrics[user_id] )

Average Customer Basket Size =
AVERAGE ( gold_customer_metrics[avg_basket_size] )

Average Days Between Orders =
AVERAGE ( gold_customer_metrics[avg_days_between_orders] )

Product Items =
SUM ( gold_product_metrics[items_ordered] )

Product Orders =
SUM ( gold_product_metrics[orders] )

Product Reordered Items =
SUM ( gold_product_metrics[reordered_items] )

Product Reorder Rate =
DIVIDE ( [Product Reordered Items], [Product Items] )

Department Items =
SUM ( gold_department_metrics[items_ordered] )

Aisle Items =
SUM ( gold_aisle_metrics[items_ordered] )

Shopping Orders =
SUM ( gold_shopping_behavior[orders] )

Shopping Items =
SUM ( gold_shopping_behavior[items_ordered] )

Shopping Known Orders =
SUM ( gold_shopping_behavior[orders_with_item_detail] )

Shopping Average Basket Size =
DIVIDE ( [Shopping Items], [Shopping Known Orders] )

Shopping Reordered Items =
SUM ( gold_shopping_behavior[reordered_items] )

Shopping Reorder Rate =
DIVIDE ( [Shopping Reordered Items], [Shopping Items] )

Last Gold Refresh =
MAX ( gold_kpi_summary[gold_updated_at] )
```

Do not average pre-aggregated reorder rates across products, departments, aisles,
days, or hours. Recalculate them with `DIVIDE(sum numerator, sum denominator)`
as above whenever the required numerator and denominator are available.

## Page 1 - Executive Overview

Use a 16:9 page with a title band, six KPI cards, and two lower visuals.

| Visual | Fields | Gold table |
|---|---|---|
| Card | Total Orders | `gold_kpi_summary` |
| Card | Total Customers | `gold_kpi_summary` |
| Card | Total Items | `gold_kpi_summary` |
| Card | Average Basket Size | `gold_kpi_summary` |
| Card | Overall Reorder Rate | `gold_kpi_summary` |
| Card | Unique Products Purchased | `gold_kpi_summary` |
| Horizontal bar | Department name; Department Items | `gold_department_metrics` |
| Donut | Department name; `item_share_pct` | `gold_department_metrics` |

Apply a Top N = 10 visual filter to the bar chart. Sort descending by Department
Items. Place a small Last Gold Refresh card in the page footer.

## Page 2 - Customer Behaviour

| Visual | Fields | Gold table |
|---|---|---|
| Donut | `customer_segment`; Customer Count | `gold_customer_metrics` |
| Histogram/column bins | `total_orders`; Customer Count | `gold_customer_metrics` |
| Scatter | X `total_orders`; Y `avg_basket_size`; size `total_items`; legend segment | `gold_customer_metrics` |
| Bar | `user_id`; `total_orders` | `gold_customer_metrics` |
| Line | `order_sequence_number`; `orders` | `gold_order_trends` |
| Line | `order_sequence_number`; `avg_products_per_known_order` | `gold_order_trends` |

Filter the customer bar to `customer_rank <= 10`. Add a segment slicer. Treat
the lifecycle visuals as order-sequence analysis, not calendar time.

## Page 3 - Product Performance

| Visual | Fields | Gold table |
|---|---|---|
| Bar | `product_name`; Product Items | `gold_product_metrics` |
| Bar | `product_name`; `reordered_items` | `gold_product_metrics` |
| Scatter | X `unique_customers`; Y Product Reorder Rate; size Product Items | `gold_product_metrics` |
| Matrix | department, aisle, product; items, orders, reorder rate, average cart position | `gold_product_metrics` |
| Bar | `department_name`; Department Items | `gold_department_metrics` |
| Bar | `aisle_name`; Aisle Items | `gold_aisle_metrics` |

Apply Top N = 10 to both product bars. Add department and aisle slicers. For a
top-products-within-department view, filter `department_product_rank <= 10`.

## Page 4 - Shopping Behaviour

| Visual | Fields | Gold table |
|---|---|---|
| Column | `day_name`; Shopping Orders | `gold_shopping_behavior` |
| Line | `order_hour_of_day`; Shopping Orders | `gold_shopping_behavior` |
| Matrix heatmap | Rows `day_name`; columns hour; values Shopping Orders | `gold_shopping_behavior` |
| Line | hour; Shopping Average Basket Size | `gold_shopping_behavior` |
| Clustered column | `day_part`; Shopping Orders and Shopping Reordered Items | `gold_shopping_behavior` |
| Line | hour; Shopping Reorder Rate | `gold_shopping_behavior` |

For the heatmap, turn off subtotals and apply background-color conditional
formatting to Shopping Orders using a light-to-dark sequential scale. Add a
day-part slicer.

## Report-wide design

- Canvas: 16:9, light neutral background, consistent 16-24 px spacing
- Primary color: dark blue; accent: teal; alert/highlight: amber
- Use the same page header, navigation buttons, filter placement, and number
  formats on all pages
- Keep no more than 6-8 visuals per page
- Enable report-page tooltips for product, department, aisle, and customer detail
- Add a definitions tooltip explaining test orders and customer segments
- Provide Reset Filters and page-navigation buttons

## Validation before publishing

1. KPI cards must match the single row in `gold_kpi_summary`.
2. Department and aisle item-share percentages should each sum to approximately
   100%, allowing for rounding.
3. Sum of Product Items must equal `total_products_ordered` when no product
   filters are active.
4. Shopping Items must equal `total_products_ordered` when no day/hour filters
   are active.
5. The customer table must contain one row per `user_id`.
6. Product bars must use visual-level Top N filters and descending sort.
7. No visual may be labelled sales, revenue, date trend, monthly growth, or
   forecast.

## Refresh and publishing

Run Bronze, Silver, and Gold before refreshing the Power BI semantic model. In
Import mode, publish the report and configure semantic-model credentials and a
refresh schedule in Power BI Service. Keep the SQL warehouse available during
refresh. If usage becomes near-real-time or the marts become much larger,
evaluate DirectQuery rather than switching by default.
