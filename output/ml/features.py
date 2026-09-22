import numpy as np
import pandas as pd

def make_features(frame):
    day = pd.to_numeric(frame['order_dow'], errors='raise')
    hour = pd.to_numeric(frame['order_hour_of_day'], errors='raise')
    if day.isna().any() or hour.isna().any() or not day.between(0, 6).all() or not hour.between(0, 23).all():
        raise ValueError('Day must be 0..6 and hour 0..23, without missing values.')
    if (day != np.floor(day)).any() or (hour != np.floor(hour)).any():
        raise ValueError('Day and hour must be integers.')
    return pd.DataFrame({
        'day_sin': np.sin(2 * np.pi * day / 7),
        'day_cos': np.cos(2 * np.pi * day / 7),
        'hour_sin': np.sin(2 * np.pi * hour / 24),
        'hour_cos': np.cos(2 * np.pi * hour / 24),
        'hour_sin_2': np.sin(4 * np.pi * hour / 24),
        'hour_cos_2': np.cos(4 * np.pi * hour / 24),
    }, index=frame.index)
