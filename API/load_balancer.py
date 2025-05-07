from flask import Flask, request, jsonify
import requests
import random
import logging

from prometheus_client import start_http_server, Counter, Histogram

REQUEST_COUNT = Counter("load_balancer_requests_total", "Total number of requests handled by the load balancer", ["backend"])
REQUEST_LATENCY = Histogram(
    'recommendation_request_duration_seconds',
    'Latency of recommendation requests',
    ['endpoint']
)

app = Flask(__name__)

# Internal service names from docker-compose
MODEL_SERVERS = [
    "http://model-service1:8083",
    "http://model-service2:8083",
]

# Set up logging
logging.basicConfig(level=logging.INFO)

@app.route("/recommend/<int:userid>", methods=["GET"])
@REQUEST_LATENCY.labels(endpoint="/recommend").time()
def recommend(userid):
    
    # Hash-based strategy for load balancing
    backend = MODEL_SERVERS[hash(userid) % len(MODEL_SERVERS)]
    REQUEST_COUNT.labels(backend=backend).inc()  # Increment the counter for the chosen backend
    
    try:
        # Forward request to chosen backend
        res = requests.get(f"{backend}/recommend/{userid}")

        # Log the routing decision
        logging.info(f"User {userid} routed to {backend} (status {res.status_code})")

        return res.text, res.status_code

    except requests.exceptions.RequestException as e:
        logging.error(f"Request to {backend} failed: {e}")
        return jsonify({"error": f"Backend error: {str(e)}"}), 502

@app.route("/")
def health():
    return "Load balancer is running", 200

@app.route("/metrics", methods=["GET"])
def metrics():
    """Expose Prometheus metrics."""
    from prometheus_client import generate_latest
    return generate_latest(), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8082)
