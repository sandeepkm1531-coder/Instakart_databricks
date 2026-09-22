import joblib
import pandas as pd
from features import make_features
bundle = joblib.load('model.joblib')
rows = pd.DataFrame({'order_dow': [1], 'order_hour_of_day': [10]})
X = make_features(rows)[bundle['feature_columns']]
print(bundle['model'].predict(X))
