# Completed Gold ML run

These files are the user-supplied Colab results from September 22, 2026, preserved
without changing the model or recorded metrics. The source is the
`shopping-behavior` Gold worksheet with 168 unique day/hour aggregates.

## Evaluation

Training used 133 rows; the test set contains 35 rows spanning hour codes
0, 8, 11, 16, and 18 across all seven day codes. Four-fold model selection groups
training rows by hour. The selected configuration was then evaluated on the
held-out hours before refitting on all 168 rows for the saved model.

| Model | Test MAE | Test RMSE | Test R² | Test WAPE |
| --- | ---: | ---: | ---: | ---: |
| Mean baseline | 14,401.94 | 16,331.14 | -0.3420 | 53.56% |
| Tuned Random Forest | 2,392.95 | 2,910.44 | 0.9574 | 8.90% |

The search tested 20 combinations and selected 400 trees, maximum depth 5,
minimum leaf size 1, and feature fraction 0.7. Training CV MAE was 3,803.03 versus
3,828.58 for Ridge; that small difference does not establish a decisive advantage.
Ridge had less variation across folds. R² is not a classification accuracy score.

The dataset has no actual dates. Results describe estimation at omitted hours
within one historical snapshot, not future demand, revenue, or individual reorders.
The saved model has been refitted on all rows and must not be used to recompute
the original held-out test scores.

## Files

- `model.joblib`: fitted model bundle and feature metadata.
- `features.py`, `predict.py`: feature construction and example inference.
- `requirements.txt`: versions recorded in the Colab run.
- `metadata.json`: configuration, source fingerprint, selected parameters and split.
- `cv_results.csv`, `tuning_trials.csv`: training-fold comparison and search trials.
- `test_metrics.csv`, `heldout_predictions.csv`: original holdout results.
- `evaluation_split.csv`: day/hour membership of training and test partitions.

## Run the saved model

From this directory, in a Python environment with the saved requirements:

```bash
python -m pip install -r requirements.txt
python predict.py
```

Only load trusted joblib files. Estimates represent total orders over the source
snapshot, not orders for a future day. The notebook now additionally supports
public Google Sheets access without sign-in; that loader change does not alter
this recorded run or the modeling logic.
