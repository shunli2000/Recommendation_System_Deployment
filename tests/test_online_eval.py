import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from math import sqrt
import os
import pickle
from surprise import Dataset, Reader
from sklearn.metrics import mean_squared_error
from online_evaluation import load_data, load_model, evaluate_model

@patch("online_evaluation.cursor")
def test_load_data(mock_cursor):
    mock_cursor.fetchall.return_value = [
        (1, 101, 4.0),
        (2, 102, 5.0)
    ]

    df = load_data()
    assert not df.empty
    assert list(df.columns) == ["user_id", "movie_id", "rating"]
    assert len(df) == 2
    assert df.iloc[0]['user_id'] == 1
    assert df.iloc[1]['movie_id'] == 102
    assert df.iloc[0]['rating'] == 4.0

@patch("builtins.open", new_callable=MagicMock)
@patch("online_evaluation.pickle.load")
def test_load_model(mock_pickle_load, mock_open):
    mock_model = MagicMock()
    mock_pickle_load.return_value = mock_model
    model = load_model("dummy/path/model.pkl")
    assert model == mock_model
    mock_open.assert_called_once()

def test_evaluate_model():
    df = pd.DataFrame({
        "user_id": [1, 2],
        "movie_id": [101, 102],
        "rating": [4.0, 5.0]
    })

    class MockPrediction:
        def __init__(self, uid, iid, r_ui, est):
            self.uid = uid
            self.iid = iid
            self.r_ui = r_ui
            self.est = est
            self.details = {}

    mock_model = MagicMock()
    mock_predictions = [
        MockPrediction(1, 101, 4.0, 4.5),
        MockPrediction(2, 102, 5.0, 4.8)
    ]
    mock_model.model.test.return_value = mock_predictions
    rmse = evaluate_model(mock_model, df)
    y_true = [4.0, 5.0]
    y_pred = [4.5, 4.8]
    expected_rmse = sqrt(mean_squared_error(y_true, y_pred))
    assert abs(rmse - expected_rmse) < 1e-6

@patch("online_evaluation.load_data")
@patch("online_evaluation.load_model")
@patch("online_evaluation.evaluate_model")
def test_main(mock_evaluate_model, mock_load_model, mock_load_data):
    mock_df = pd.DataFrame({
        "user_id": [1],
        "movie_id": [101],
        "rating": [4.0]
    })
    mock_load_data.return_value = mock_df
    mock_model = MagicMock()
    mock_load_model.return_value = mock_model
    mock_evaluate_model.return_value = 0.9
    from online_evaluation import main
    main()
    mock_load_data.assert_called_once()
    mock_load_model.assert_called_once()
    mock_evaluate_model.assert_called_once_with(mock_model, mock_df)

if __name__ == "__main__":
    pytest.main(["-v"])
