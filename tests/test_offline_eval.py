import os
import sys
import pytest
from surprise import Dataset, Reader, SVD

project_root = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, project_root)

from ML.SVDCFModel import SVDCFModel

def get_data_path():
    return os.path.join(project_root, "ML", "user_movie.csv")

@pytest.fixture
def model():
    return SVDCFModel()

def test_train_model(model):
    best_params = model.train(get_data_path())
    assert isinstance(best_params, list)

def test_save_and_load_model(model):
    model.train(get_data_path())
    model.save_model('test_model.pkl')
    assert os.path.exists('test_model.pkl')

    model.load_model('test_model.pkl')
    assert model.model is not None

    # Clean up
    os.remove('test_model.pkl')

def test_predict(model):
    model.train(get_data_path())
    user_id = 1
    movie_id = 1
    prediction = model.predict(user_id, movie_id)
    assert isinstance(prediction, float)

def test_recommend_top_n(model):
    model.train(get_data_path())
    user_id = 1
    top_n = model.recommend_top_n(user_id, n=5)
    assert isinstance(top_n, list)
    assert len(top_n) == 5
    assert all(isinstance(movie_id, str) for movie_id in top_n)