# Instacart End-to-End Analytics, Machine Learning and RAG Assistant

This project takes raw shopping data, prepares reliable reporting tables,
visualizes business patterns, and makes selected information accessible through
an AI assistant—with a separate machine-learning experiment.

Databricks Bronze, Silver, and Gold layers process the grocery-order data. Gold
results are exported to Google Sheets for a Looker Studio dashboard. The RAG
assistant explains selected metrics and dataset knowledge. The separate ML
experiment estimates historical shopping patterns and includes hyperparameter
tuning; it is not part of the assistant's retrieval workflow.

## Implemented workflow

1. Bronze ingests the six source datasets from Amazon S3 into Delta tables.
2. Silver cleans the data and separates invalid records into quarantine tables.
3. Gold produces business aggregates for KPIs, order sequence, customers,
   products, departments, aisles, and shopping behavior.
4. Google Sheets holds exported dashboard datasets consumed by Looker Studio.
5. The RAG assistant retrieves analytics knowledge to ground its explanations.

The project owner confirmed running Silver and Gold and creating the dashboard
and assistant. Supporting material reviewed for this update consists of
`Databricks_Test.xlsx` and `building RAG on top of Instackart.docx`.

## Repository coverage

The repository contains the Databricks pipeline source and documentation.
The live Google Sheet, Looker Studio report, and executable RAG notebook are
external artifacts and are not included here. The RAG document describes a build
approach; it does not by itself establish the exact final deployed implementation.
Notebooks 04–09 remain scaffolding. Notebook 10 now contains a completed Colab ML
experiment with optional hyperparameter tuning over the shopping-behavior Gold
aggregates. Its [saved model and evaluation](../output/ml/README.md) are included.
This is historical pattern estimation, not a dated forecasting experiment.

See [Looker Studio dashboard](looker_studio_dashboard.md) and
[RAG assistant](rag_assistant.md) for details. The existing
[Power BI guide](powerbi_dashboard_guide.md) is an alternative design.

