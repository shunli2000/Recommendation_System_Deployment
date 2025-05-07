import logging
from flask import Flask, jsonify
from threading import Thread, Lock
from prometheus_client import start_http_server, Counter, Gauge, Histogram
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_caching import Cache
import psycopg2

import os
import sys
import time
import pickle
import socket
import argparse
import random
from math import sqrt

# Ensure Python finds ML module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ML.SVDCFModel import SVDCFModel

REQUEST_COUNT = Counter("recommendation_requests_total", "Total number of recommendation requests", ["endpoint"])
AVERAGE_MOVIE_POPULARITY = Counter("average_movie_popularity", "Average popularity of recommended movies")
AVERAGE_RMSE = Gauge("average_rmse", "Average RMSE for recommendations")
RMSE_HISTOGRAM = Histogram(
    "rmse_histogram", 
    "RMSE distribution for recommendation predictions",
    buckets=[0.5, 1.0, 1.5, 2.0, 3.0, 5.0, 10.0]
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

class RecommendationAPI:
    def __init__(self, model_path, verbose=False):
        self.app = Flask(__name__)
        self.verbose = verbose
        self.model_registry = {}
        self.model_lock = Lock()
        self.model = None
        self.conn = None

        # Rate limiting and caching
        self.limiter = Limiter(get_remote_address, app=self.app, default_limits=["10 per minute"])
        self.app.config['CACHE_TYPE'] = 'SimpleCache'
        self.app.config['CACHE_DEFAULT_TIMEOUT'] = 30
        self.cache = Cache(self.app)

        if self.verbose:
            logging.getLogger().setLevel(logging.DEBUG)

        self.preload_models()
        self.model = self.load_model(model_path)
        self.setup_routes()
        self.connect_to_db()

    def connect_to_db(self):
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(
                dbname=os.getenv("POSTGRES_DB", "movie_data"),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", "mlip-007"),
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=os.getenv("POSTGRES_PORT", "5432")
            )

    def close_db_connection(self):
        if self.conn and not self.conn.closed:
            self.conn.close()

    def __del__(self):
        self.close_db_connection()

    def preload_models(self):
        models_to_preload = {
            "service1": "ML/models/service1/SVDCF.pkl",
            "service2": "ML/models/service2/SVDCF.pkl",
            "service-dev": "ML/models/service-dev/SVDCF.pkl",
        }
        for name, path in models_to_preload.items():
            try:
                logging.info(f"Preloading model: {name} from {path}")
                self.model_registry[name] = self.load_model(path)
            except Exception as e:
                logging.error(f"Failed to preload model {name}: {e}")

    def load_model(self, model_path):
        try:
            logging.info(f"Loading model from {model_path}...")
            with open(model_path, "rb") as f:
                return pickle.load(f)
        except Exception as e:
            logging.error(f"Error loading model from {model_path}: {e}")
            return None

    def load_model_async(self, model_path):
        try:
            logging.info(f"Loading model from {model_path} asynchronously...")
            new_model = self.load_model(model_path)
            if new_model is not None:
                with self.model_lock:
                    self.model_registry[model_path] = new_model
                logging.info(f"Model {model_path} loaded and added to registry.")
        except Exception as e:
            pass

    def get_recommendations(self, user_id):
        user_id = str(user_id)
        logging.info(f"Getting recommendations for user {user_id}...")
        try:
            recommended_movies = self.model.recommend_top_n(user_id, n=40)
            res = [movie_id for movie_id in recommended_movies if "+" in movie_id][:20]

            if random.randint(1, 10) >= 1:
                try:
                    rmse = self.calculate_rmse_for_user(user_id)
                    if rmse is not None:
                        RMSE_HISTOGRAM.observe(rmse)
                        logging.info(f"Calculated RMSE for user {user_id}: {rmse}")
                except ValueError as e:
                    logging.warning(f"Could not calculate RMSE for user {user_id}: {e}")

            logging.info(f"Recommendations for user {user_id}: {res}")
            return res
        except Exception as e:
            logging.error(f"Error in recommendation: {e}")
            return []

    def get_average_movie_popularity(self, movie_ids):
        try:
            self.connect_to_db()
            cursor = self.conn.cursor()
            query = "SELECT AVG(popularity) FROM movies WHERE movie_id IN %s"
            cursor.execute(query, (tuple(movie_ids),))
            avg_popularity = cursor.fetchone()[0]
            cursor.close()
            return avg_popularity
        except Exception as e:
            logging.error(f"Error fetching average movie popularity: {e}")
            return None

    def calculate_rmse_for_user(self, user_id):
        self.connect_to_db()
        cursor = self.conn.cursor()
        query = """
        SELECT movie_id, CAST(rating AS FLOAT) AS rating
        FROM user_ratings
        WHERE user_id = %s
        """
        cursor.execute(query, (user_id,))
        user_ratings = cursor.fetchall()
        cursor.close()

        if not user_ratings:
            logging.warning(f"No ratings found for user {user_id}")
            return None

        predictions = []
        actual_ratings = []
        for movie_id, actual_rating in user_ratings:
            predicted_rating = self.model.predict(user_id, movie_id)
            predictions.append(predicted_rating)
            actual_ratings.append(actual_rating)

        squared_errors = [(pred - actual) ** 2 for pred, actual in zip(predictions, actual_ratings)]
        rmse = sqrt(sum(squared_errors) / len(squared_errors))
        logging.info(f"RMSE for user {user_id}: {rmse}")
        return rmse

    def setup_routes(self):
        @self.app.route('/recommend/<int:userid>', methods=['GET'])
        @self.limiter.limit("10 per minute")
        def recommend(userid):
            REQUEST_COUNT.labels(endpoint="/recommend").inc()
            cache_key = f"recommendation_{userid}"

            try:
                cached_response = self.cache.get(cache_key)
                if cached_response:
                    logging.info(f"Serving cached recommendations for user {userid}")
                    return cached_response, 200

                host = socket.gethostbyname(socket.gethostname())
                start_time = time.time()

                recommendations = self.get_recommendations(userid)
                average_popularity = self.get_average_movie_popularity(recommendations)

                if average_popularity is not None:
                    logging.info(f"Average movie popularity for user {userid}: {average_popularity}")
                    AVERAGE_MOVIE_POPULARITY.inc(average_popularity)

                response = ",".join(map(str, recommendations))
                self.cache.set(cache_key, response)
                response_time = int((time.time() - start_time) * 1000)

                logging.info("=" * 50)
                logging.info(f"Timestamp: {time.time()}, UserID: {userid}, Request from {host}, Status: 200, "
                             f"Result: {response}, Response Time: {response_time}ms")
                logging.info("=" * 50)

                return response, 200

            except Exception as e:
                logging.error(f"Error processing recommendation request: {e}")
                return jsonify({"error": str(e)}), 500

        @self.app.route('/')
        def health():
            try:
                logging.info("Health check passed")
                return "OK", 200
            except Exception as e:
                logging.error(f"Health check failed: {e}")
                return jsonify({"error": str(e)}), 500

        @self.app.route('/change_model/<model_path>', methods=['POST'])
        def change_model(model_path):
            try:
                with self.model_lock:
                    Thread(target=self.load_model_async, args=(model_path,)).start()
                    logging.info(f"Model loading initiated for {model_path}")
                    return jsonify({"status": "success", "message": f"Model loading initiated for {model_path}"}), 202
            except Exception as e:
                logging.error(f"Error changing model: {e}")
                return jsonify({"status": "error", "message": str(e)}), 500

        @self.app.route('/metrics', methods=['GET'])
        def metrics():
            from prometheus_client import generate_latest
            return generate_latest(), 200

def create_app(model_path, verbose=False):
    return RecommendationAPI(model_path, verbose).app

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run the Flask API with a selected model.")
    parser.add_argument('--model_path', required=True, help="Path to the model file")
    parser.add_argument('--port', type=int, default=8083, help="Port to run the Flask app on")
    parser.add_argument('--verbose', action='store_true', help="Enable verbose output")

    args = parser.parse_args()

    app = create_app(args.model_path, args.verbose)
    app.run(host='0.0.0.0', port=args.port)