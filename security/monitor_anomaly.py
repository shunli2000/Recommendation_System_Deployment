import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from kafka import KafkaConsumer

# Config
MOVIE_THRESHOLD = 20     # flag if a movie gets >X ratings in window
USER_THRESHOLD = 10      # flag if a user rates >X movies in window
TIME_WINDOW = timedelta(minutes=5)

# State tracking
user_activity = defaultdict(deque)  # user_id -> deque of timestamps
movie_activity = defaultdict(deque)  # movie_id -> deque of timestamps

consumer = KafkaConsumer(
    'your_topic_name',          # replace with your Kafka topic
    bootstrap_servers='localhost:9092',
    group_id='anomaly_detector',
    auto_offset_reset='latest',
    value_deserializer=lambda x: x.decode('utf-8')
)

print("Monitoring Kafka stream for rating manipulation...")

def cleanup(activity_dict, now):
    for key in list(activity_dict):
        dq = activity_dict[key]
        while dq and dq[0] < now - TIME_WINDOW:
            dq.popleft()

for message in consumer:
    log = message.value.strip()
    
    try:
        # Example log: "2025-04-24T18:02:31,219515,GET /rate/senna+2010=4"
        parts = log.split(',')
        timestamp = datetime.fromisoformat(parts[0])
        user_id = parts[1]
        url = parts[2]

        if "/rate/" in url:
            rate_part = url.split("/rate/")[1]
            movie, rating = rate_part.split('=')
            rating = int(rating)

            # Track user activity
            user_activity[user_id].append(timestamp)
            if len(user_activity[user_id]) > USER_THRESHOLD:
                print(f"[⚠️ USER] User {user_id} rated {len(user_activity[user_id])} movies in last {TIME_WINDOW}!")

            # Track movie activity
            movie_activity[movie].append(timestamp)
            if len(movie_activity[movie]) > MOVIE_THRESHOLD:
                print(f"[🚨 MOVIE] Movie '{movie}' received {len(movie_activity[movie])} ratings in last {TIME_WINDOW}!")

            # Cleanup old entries
            cleanup(user_activity, timestamp)
            cleanup(movie_activity, timestamp)

    except Exception as e:
        print("Parse error:", e)
