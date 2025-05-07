import pytest
import requests

API_BASE_URL = "http://128.2.204.215:8080"

def test_fetch_movie_info_live():
    movie_id = "the+thomas+crown+affair+1968"  # Replace with a real valid movie ID known to exist in your system
    response = requests.get(f"{API_BASE_URL}/movie/{movie_id}")
    assert response.status_code == 200

def test_fetch_user_info_live():
    user_id = "16394"  # Replace with a real valid user ID known to exist in your system
    response = requests.get(f"{API_BASE_URL}/user/{user_id}")
    assert response.status_code == 200
