import requests
import urllib.parse

API_BASE_URL = "http://128.2.204.215:8080"

def fetch_movie_info(movie_id):
    try:
        movie_id_encoded = urllib.parse.quote(movie_id)
        response = requests.get(f"{API_BASE_URL}/movie/{movie_id_encoded}")
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Movie API error: {e}")
    return None

def fetch_user_info(user_id):
    try:
        response = requests.get(f"{API_BASE_URL}/user/{user_id}")
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"User API error: {e}")
    return None
