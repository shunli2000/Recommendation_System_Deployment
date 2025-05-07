from data_pipeline.kafka_consumer import get_kafka_consumer
from api_call import fetch_movie_info, fetch_user_info
from data_pipeline.schema_validation import validate_movie_schema, validate_user_schema
from data_pipeline.drift_detection import detect_ks_drift, calculate_psi
from collections import Counter
from data_pipeline.db_writer import (
    get_db_connection,
    insert_movie,
    insert_user,
    upsert_interaction,
    upsert_ratings,
    get_reference_distribution
)

user_info_cache = {}
movie_info_cache = {}
user_ratings = {}
user_movie_data = {}
collected_data = {
    "budget": [], "revenue": [], "popularity": [], "age": [],
    "genres": [], "gender": [], "watch_count": [], "rating": []
}

PSI_THRESHOLD = 0.25
KS_THRESHOLD = 0.05
BATCH_SIZE = 1000

# Load historical reference data
historical_data = {
    "genres": Counter(get_reference_distribution("movies", "genres")),
    "budget": get_reference_distribution("movies", "budget"),
    "revenue": get_reference_distribution("movies", "revenue"),
    "popularity": get_reference_distribution("movies", "popularity"),
    "age": get_reference_distribution("users", "age"),
    "gender": Counter(get_reference_distribution("users", "gender")),
    "rating": Counter(get_reference_distribution("user_ratings", "rating")),
    "watch_count": get_reference_distribution("user_movie_interaction", "count")
}

stats = {
    'movies_inserted': 0,
    'users_inserted': 0,
    'ratings_processed': 0,
    'interactions_processed': 0,
    'skipped_ratings': 0,
    'skipped_interactions': 0,
    'api_errors': 0,
    'db_errors': 0,
    'messages_processed': 0
}

def check_drift():
    for feature in ["genres", "gender", "rating"]:
        actual = Counter(collected_data[feature])
        expected = historical_data[feature]
        psi = calculate_psi(expected, actual)

        if psi is not None and psi > PSI_THRESHOLD:
            print(f"Drift detected in {feature} (PSI > {PSI_THRESHOLD})")

    for feature in ["budget", "revenue", "popularity", "age", "watch_count"]:
        ks_stat, p_val = detect_ks_drift(historical_data[feature], collected_data[feature])

        if p_val < KS_THRESHOLD:
            print(f"Drift detected in {feature} (p < {KS_THRESHOLD})")

    # Clear collected data
    for key in collected_data:
        collected_data[key].clear()


def process():
    consumer = get_kafka_consumer()
    conn, cursor = get_db_connection()
    message_count = 0

    for msg in consumer:
        message_count += 1
        stats['messages_processed'] += 1
        try:
            line = msg.value.strip()
            parts = line.split(",")
            if len(parts) < 3:
                continue

            timestamp, user_id, request = parts[0].strip(), parts[1].strip(), parts[2].strip()

            if "/rate/" in request:
                movie_id = request.split("=")[0].split("/")[-1]
                rating = request.split("=")[-1]
                cursor.execute("SELECT 1 FROM movies WHERE movie_id = %s", (movie_id,))
                if cursor.fetchone():
                    user_ratings[(user_id, movie_id)] = rating
                    collected_data["rating"].append(rating)
                else:
                    stats['skipped_ratings'] += 1

            elif "/data/m/" in request:
                movie_id = request.split("/")[-2]
                if movie_id not in movie_info_cache:
                    movie_info = fetch_movie_info(movie_id)
                    if movie_info and validate_movie_schema(movie_info):
                        movie_info_cache[movie_id] = movie_info
                        genres = ", ".join([g['name'] for g in movie_info.get("genres", [])])
                        collected_data["budget"].append(movie_info["budget"])
                        collected_data["revenue"].append(movie_info["revenue"])
                        collected_data["popularity"].append(movie_info["popularity"])
                        collected_data["genres"].extend([g['name'] for g in movie_info["genres"]])
                        try:
                            insert_movie(cursor, movie_id, movie_info, genres)
                            conn.commit()
                            stats['movies_inserted'] += 1
                        except:
                            conn.rollback()
                            stats['db_errors'] += 1

                cursor.execute("SELECT 1 FROM movies WHERE movie_id = %s", (movie_id,))
                if cursor.fetchone():
                    key = (user_id, movie_id)
                    if key in user_movie_data:
                        existing_ts, count = user_movie_data[key]
                        user_movie_data[key] = (max(timestamp, existing_ts), count + 1)
                    else:
                        user_movie_data[key] = (timestamp, 1)
                    collected_data["watch_count"].append(user_movie_data[key][1])
                else:
                    stats['skipped_interactions'] += 1

            if user_id not in user_info_cache:
                user_info = fetch_user_info(user_id)
                if user_info and validate_user_schema(user_info):
                    user_info_cache[user_id] = user_info
                    collected_data["age"].append(user_info["age"])
                    collected_data["gender"].append(user_info["gender"])
                    try:
                        insert_user(cursor, user_id, user_info)
                        conn.commit()
                        stats['users_inserted'] += 1
                    except:
                        conn.rollback()
                        stats['db_errors'] += 1

            if message_count % BATCH_SIZE == 0:
                check_drift()
                try:
                    upsert_interaction(cursor, user_movie_data)
                    conn.commit()
                    stats['interactions_processed'] += len(user_movie_data)
                    user_movie_data.clear()
                except:
                    conn.rollback()
                    stats['db_errors'] += 1

                try:
                    upsert_ratings(cursor, user_ratings)
                    conn.commit()
                    stats['ratings_processed'] += len(user_ratings)
                    user_ratings.clear()
                except:
                    conn.rollback()
                    stats['db_errors'] += 1

        except Exception as e:
            print(f"Message processing error: {e}")

    cursor.close()
    conn.close()
    print("\n========== FINAL SUMMARY ==========")
    for k, v in stats.items():
        print(f"{k.replace('_', ' ').capitalize()}: {v}")

if __name__ == "__main__":
    process()
