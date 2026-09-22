# Looker Studio dashboard

The implemented dashboard uses Gold data exported from Databricks to Google
Sheets, with Looker Studio consuming the sheets. The project owner confirmed
creating this dashboard. The workbook `Databricks_Test.xlsx` was reviewed as a
supporting snapshot; it is not a live connection or a dashboard definition.

## Exported datasets

| Workbook tab | Content |
| --- | --- |
| `databricks-output` | Order trends by customer order sequence |
| `kpi-summary` | Overall orders, customers, items, basket size, and reorder rate |
| `department-metrics` | Department volume, reach, item share, and reorder rate |
| `aisle-metrics` | Aisle volume, reach, item share, and reorder rate |
| `shopping-behavior` | Day-of-week and hour-level shopping metrics |
| `customer-segments` | Customer counts and aggregate behavior by segment |
| `top-products` | Product names, volume, reorder rate, and department rank |

The workbook also has a `Sheet1` customer/amount test table, which is not part of
the documented Gold dashboard datasets. Customer segments and top products are
derived exports; their presence does not imply additional Gold tables.

## Snapshot metrics

The supplied `kpi-summary` row contains 3,421,083 orders, 206,209 customers,
33,819,103 ordered items, an average basket size of 10.11, and a reorder rate of
0.5901 (59.01%). These are snapshot values, not live dashboard readings.

## Metric interpretation

- `order_sequence_number` describes the customer's first, second, or later order.
  It is not a calendar date. The source does not support real monthly trends.
- `order_dow` and `order_hour_of_day` describe shopping patterns, not dated events.
- Item totals count order-product rows, not revenue. The source lacks prices.
- Reorder rates are fractions; format 0.5901 as 59.01%, not 0.5901%.
- Distinct customer and order counts across departments or aisles overlap and
  should not be summed to obtain overall totals. Use the KPI summary.

The supplied export uses `_gold_processed_at_utc`; the maintained Gold notebook
uses `gold_updated_at`. Check this field mapping when refreshing from that source.
Refresh automation and live report sharing settings have not been documented.

## Repository status

The Google Sheets and Looker Studio URLs and report screenshots are not yet
included. The workbook was used to document the dataset structure; this update
does not republish its raw contents. The Power BI guide is an earlier alternative
and does not describe the implemented dashboard.
