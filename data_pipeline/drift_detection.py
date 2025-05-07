import numpy as np
from scipy.stats import ks_2samp
from collections import Counter

def calculate_psi(expected_counts, actual_counts):
    all_categories = set(expected_counts.keys()).union(actual_counts.keys())
    total_expected = sum(expected_counts.values())
    total_actual = sum(actual_counts.values())
    if total_expected == 0 or total_actual == 0:
        return None
    expected = np.array([expected_counts.get(cat, 0) for cat in all_categories]) / total_expected
    actual = np.array([actual_counts.get(cat, 0) for cat in all_categories]) / total_actual
    expected = np.where(expected == 0, 1e-6, expected)
    actual = np.where(actual == 0, 1e-6, actual)
    return np.sum((expected - actual) * np.log(expected / actual))

def detect_ks_drift(historical, new):
    return ks_2samp(historical, new)
