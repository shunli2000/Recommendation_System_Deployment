from kafka import KafkaConsumer
import requests
import psycopg2
import urllib.parse
import numpy as np
from scipy.stats import ks_2samp
from collections import Counter
import time

# PostgreSQL connection setup
conn = psycopg2.connect(
    dbname="movie_data",
    user="postgres",
    password="mlip-007",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# Kafka consumer setup
consumer = KafkaConsumer(
    "movielog7",
    bootstrap_servers="localhost:9092",
    auto_offset_reset='earliest',
    value_deserializer=lambda m: m.decode('utf-8')
)

# API Base URL for fetching user and movie details
api_base_url = "http://128.2.204.215:8080"

# Temporary in-memory storage
user_movie_data = {}
user_ratings = {}
user_info_cache = {} 
movie_info_cache = {}


# PSI for categorical drift detection
def calculate_psi(expected_counts, actual_counts):
    """Ensure both distributions have the same set of bins before computing PSI"""
    all_categories = set(expected_counts.keys()).union(set(actual_counts.keys()))

    total_actual = sum(actual_counts.values())
    total_expected = sum(expected_counts.values())

    # Check if either distribution is empty
    if total_actual == 0 or total_expected == 0:
        print("Warning: Empty actual or expected distribution, skipping PSI computation.")
        return None  # Skip computation if there's no valid data
    
    expected = np.array([expected_counts.get(cat, 0) for cat in all_categories]) / sum(expected_counts.values())
    actual = np.array([actual_counts.get(cat, 0) for cat in all_categories]) / sum(actual_counts.values())

    # Avoid division by zero errors
    actual = np.where(actual == 0, 1e-6, actual)
    expected = np.where(expected == 0, 1e-6, expected)

    psi_values = (expected - actual) * np.log(expected / actual)
    return np.sum(psi_values)

def check_psi_drift():
    global event_counter
    if event_counter % batch_size == 0 and event_counter > 0:
        actual_genres_distribution = Counter(collected_data["genres"])
        actual_gender_distribution = Counter(collected_data["gender"])
        actual_rating_distribution = Counter(collected_data["rating"])

        psi_genres = calculate_psi(historical_genres, actual_genres_distribution)
        psi_gender = calculate_psi(historical_gender, actual_gender_distribution)
        psi_rating = calculate_psi(historical_ratings, actual_rating_distribution)

        print(f"PSI results - Genres: {psi_genres}, Gender: {psi_gender}, Rating: {psi_rating}")

        if psi_genres is not None and psi_genres > PSI_THRESHOLD:
            print("Alert: Significant genre distribution drift detected!")
        if psi_gender is not None and psi_gender > PSI_THRESHOLD:
            print("Alert: Significant gender distribution drift detected!")
        if psi_rating is not None and psi_rating > PSI_THRESHOLD:
            print("Alert: Significant rating distribution drift detected!")

        # Reset collected data
        collected_data["genres"].clear()
        collected_data["gender"].clear()
        collected_data["rating"].clear()


# KS-Test for numerical drift detection
def detect_ks_drift(historical_data, new_data):
    """ Perform KS-test to compare two numerical distributions """
    ks_stat, p_value = ks_2samp(historical_data, new_data)
    return ks_stat, p_value

def check_ks_drift():
    global event_counter
    if event_counter % batch_size == 0 and event_counter > 0:
        ks_budget = detect_ks_drift(historical_budget, collected_data["budget"])
        ks_revenue = detect_ks_drift(historical_revenue, collected_data["revenue"])
        ks_popularity = detect_ks_drift(historical_popularity, collected_data["popularity"])
        ks_age = detect_ks_drift(historical_age, collected_data["age"])
        ks_watch_count = detect_ks_drift(historical_watch_count, collected_data["watch_count"])

        print(f"KS-test results - Budget: {ks_budget}, Revenue: {ks_revenue}, Popularity: {ks_popularity}, Age: {ks_age}, Watch_Count: {ks_watch_count}")

        if ks_budget[1] < KS_THRESHOLD:
            print("Alert: Significant budget distribution drift detected!")
        if ks_revenue[1] < KS_THRESHOLD:
            print("Alert: Significant revenue distribution drift detected!")
        if ks_popularity[1] < KS_THRESHOLD:
            print("Alert: Significant popularity distribution drift detected!")
        if ks_age[1] < KS_THRESHOLD:
            print("Alert: Significant age distribution drift detected!")
        if ks_watch_count[1] < KS_THRESHOLD:
            print("Alert: Significant watch count distribution drift detected!")

        # Reset collected data
        collected_data["budget"].clear()
        collected_data["revenue"].clear()
        collected_data["popularity"].clear()
        collected_data["age"].clear()
        collected_data["watch_count"].clear()

# Fetch historical distributions for PSI/KS-test
def get_reference_distribution(table, column):
    cursor.execute(f"SELECT {column} FROM {table}")
    data = cursor.fetchall()
    return [row[0] for row in data if row[0] is not None]

# Define PSI and KS thresholds
PSI_THRESHOLD = 0.25
KS_THRESHOLD = 0.05
batch_size = 1000  # Batch size for drift detection
event_counter = 0  # Count the number of processed events

# Load historical distributions
historical_genres = Counter(get_reference_distribution("movies", "genres"))
historical_budget = get_reference_distribution("movies", "budget")
historical_revenue = get_reference_distribution("movies", "revenue")
historical_popularity = get_reference_distribution("movies", "popularity")
historical_age = get_reference_distribution("users", "age")
historical_gender = Counter(get_reference_distribution("users", "gender"))
historical_ratings = Counter(get_reference_distribution("user_ratings", "rating"))
historical_watch_count = get_reference_distribution("user_movie_interaction", "count")

# Collected data for batch drift detection
collected_data = {
    "budget": [],
    "revenue": [],
    "popularity": [],
    "age": [],
    "genres": [],
    "gender": [],
    "watch_count": [],
    "rating": []
}

# Function to validate schema
def validate_movie_schema(movie_info):
    required_fields = ['title', 'adult', 'genres', 'release_date', 'budget', 'revenue', 'popularity', 'runtime']
    for field in required_fields:
        if field not in movie_info or movie_info[field] is None:
            return False
    return True

def validate_user_schema(user_info):
    """Ensure user data contains all necessary fields and valid types."""
    required_fields = ['age', 'occupation', 'gender']  # Adjust based on actual user schema
    for field in required_fields:
        if field not in user_info or user_info[field] is None:
            return False
    return True


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

print("Listening to Kafka topic...")

# Process messages in batches to reduce transaction overhead
batch_size_transaction = 100
message_count = 0

for message in consumer:
    event_counter += 1
    message_count += 1
    stats['messages_processed'] += 1

    try:
        line = message.value.strip()
        parts = line.split(",")

        # Skip malformed lines
        if len(parts) < 3:
            continue

        # Extract components
        timestamp = parts[0].strip()
        user_id = parts[1].strip()
        request = parts[2].strip()

        # Check if it's a rating event
        if "/rate/" in request:
            rate_parts = request.split("=")  # Format: "/rate/movie_id=rating"
            if len(rate_parts) == 2:
                movie_id = rate_parts[0].split("/")[-1]
                rating = rate_parts[1]

                # Check if movie exists in movies table before adding to user_ratings
                cursor.execute("SELECT 1 FROM movies WHERE movie_id = %s", (movie_id,))
                movie_exists = cursor.fetchone() is not None
                
                if movie_exists:
                    user_ratings[(user_id, movie_id)] = rating
                    collected_data["rating"].append(rating)
                else:
                    print(f"Skipping rating for non-existent movie {movie_id}")
                    stats['skipped_ratings'] += 1

        # Check if it's a movie interaction event
        elif "/data/m/" in request:
            movie_id = request.split("/")[-2]

            # Fetch movie metadata from API if not cached
            if movie_id not in movie_info_cache:
                try:
                    movie_id_encoded = urllib.parse.quote(movie_id)
                    response = requests.get(f"{api_base_url}/movie/{movie_id_encoded}")

                    if response.status_code == 200:
                        movie_info = response.json()

                        if validate_movie_schema(movie_info):                            
                            movie_info_cache[movie_id] = movie_info

                            # Extract and format genre names as a comma-separated string
                            genres = ", ".join([genre["name"] for genre in movie_info.get("genres", [])])

                            # Collect data for drift detection
                            collected_data["budget"].append(movie_info["budget"])
                            collected_data["revenue"].append(movie_info["revenue"])
                            collected_data["popularity"].append(movie_info["popularity"])
                            collected_data["genres"].extend([genre["name"] for genre in movie_info.get("genres", [])])

                            try:
                                cursor.execute("""
                                    INSERT INTO movies (movie_id, title, adult, genres, release_date, budget, revenue, popularity, runtime)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (movie_id) DO NOTHING;
                                """, (
                                    movie_id, 
                                    movie_info.get('title'), 
                                    movie_info.get('adult'), 
                                    genres, 
                                    movie_info.get('release_date'), 
                                    movie_info.get('budget'), 
                                    movie_info.get('revenue'), 
                                    movie_info.get('popularity'), 
                                    movie_info.get('runtime')
                                ))
                                conn.commit()
                                stats['movies_inserted'] += 1
                            except Exception as db_err:
                                conn.rollback()
                                print(f"DB error inserting movie {movie_id}: {db_err}")
                                stats['db_errors'] += 1
                        else:
                            print(f"Invalid schema for {movie_id}, skipping.")
                            cursor.execute("INSERT INTO invalid_data_log (movie_id, error_message, detected_at) VALUES (%s, 'Schema validation failed', NOW());", (movie_id,))
                        
                except Exception as api_err:
                    print(f"Failed to fetch movie info for {movie_id}: {api_err}")
                    stats['api_errors'] += 1
                
                # Small delay to prevent API rate limiting
                time.sleep(0.05)
            
            # Check if movie exists in movies table before updating interaction
            cursor.execute("SELECT 1 FROM movies WHERE movie_id = %s", (movie_id,))
            movie_exists = cursor.fetchone() is not None

            if movie_exists:
                # Debug print before updating
                print(f"Before update: {user_movie_data.get((user_id, movie_id), 'Not Found')}")

                # Update user-movie interaction dictionary
                if (user_id, movie_id) in user_movie_data:
                    existing_timestamp, count = user_movie_data[(user_id, movie_id)]
                    # Always increment count regardless of timestamp
                    user_movie_data[(user_id, movie_id)] = (max(timestamp, existing_timestamp), count + 1)
                else:
                    # First-time interaction
                    user_movie_data[(user_id, movie_id)] = (timestamp, 1)
                
                # Debug print after updating
                print(f"After update: {user_movie_data[(user_id, movie_id)]}")
                
                # Collect total watch count data for drift detection
                watch_count = user_movie_data[(user_id, movie_id)][1]  # Get the accumulated count
                collected_data["watch_count"].append(watch_count)  # Store only count values
            else:
                print(f"Skipping interaction for movie not in database: {movie_id}")
                stats['skipped_interactions'] += 1

        # Fetch user metadata from API if not cached
        if user_id not in user_info_cache:
            try:
                response = requests.get(f"{api_base_url}/user/{user_id}")
                if response.status_code == 200:
                    user_info = response.json()

                    if validate_user_schema(user_info):
                        user_info_cache[user_id] = user_info

                        collected_data["age"].append(user_info["age"])
                        collected_data["gender"].append(user_info["gender"])

                        try:
                            cursor.execute("""
                                INSERT INTO users (user_id, age, occupation, gender)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (user_id) DO NOTHING;
                            """, (user_id, user_info.get('age'), user_info.get('occupation'), user_info.get('gender')))
                            conn.commit()
                            stats['users_inserted'] += 1
                        except Exception as db_err:
                            conn.rollback()
                            print(f"DB error inserting user {user_id}: {db_err}")
                            stats['db_errors'] += 1
                    else:
                        print(f"Invalid schema for user {user_id}, skipping.")
                        cursor.execute("""
                            INSERT INTO invalid_user_data_log (user_id, error_message, detected_at) 
                            VALUES (%s, 'Schema validation failed', NOW());
                        """, (user_id,))

            except Exception as api_err:
                print(f"Failed to fetch user info for {user_id}: {api_err}")
                stats['api_errors'] += 1

            time.sleep(0.05)
        
        check_ks_drift()  # Run KS-Test for numerical drift
        check_psi_drift()  # Run PSI for categorical drift

        # Process in batches to reduce transaction overhead
        if message_count % batch_size_transaction == 0:
            # Insert/update user-movie interactions in a single transaction
            if user_movie_data:
                try:
                    """Debugging"""
                    for key, value in user_movie_data.items():
                        print(f"{key}: {value}")
                    
                    for (user, movie), (latest_timestamp, count) in user_movie_data.items():
                        cursor.execute("""
                            INSERT INTO user_movie_interaction (user_id, movie_id, last_interaction, count)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (user_id, movie_id)
                            DO UPDATE SET last_interaction = EXCLUDED.last_interaction, count = user_movie_interaction.count + EXCLUDED.count;
                        """, (user, movie, latest_timestamp, count))
                    conn.commit()
                    stats['interactions_processed'] += len(user_movie_data)

                    # Debug print to confirm correct insertion
                    print(f"Inserted data: {user_movie_data}")

                except Exception as e:
                    conn.rollback()
                    print(f"Error processing user-movie interactions: {e}")
                    stats['db_errors'] += 1

            # Insert/update user ratings in a single transaction
            if user_ratings:
                try:
                    for (user, movie), rating in user_ratings.items():
                        cursor.execute("""
                            INSERT INTO user_ratings (user_id, movie_id, rating)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (user_id, movie_id)
                            DO UPDATE SET rating = EXCLUDED.rating;
                        """, (user, movie, rating))
                    conn.commit()
                    stats['ratings_processed'] += len(user_ratings)

                except Exception as e:
                    conn.rollback()
                    print(f"Error processing user ratings: {e}")
                    stats['db_errors'] += 1
        
        if message_count > 0 and message_count % 100 == 0:  # Report every 100 messages
            print(f"\n--- Progress Report (after {message_count} messages) ---")
            print(f"Movies inserted: {stats['movies_inserted']}")
            print(f"Users inserted: {stats['users_inserted']}")
            print(f"Ratings processed: {stats['ratings_processed']}")
            print(f"Interactions processed: {stats['interactions_processed']}")
            print(f"Skipped ratings: {stats['skipped_ratings']}")
            print(f"Skipped interactions: {stats['skipped_interactions']}")
            print(f"API errors: {stats['api_errors']}")
            print(f"DB errors: {stats['db_errors']}")
            print("---------------------------------------------------\n")

    except Exception as general_err:
        print(f"Error processing message: {general_err}")

# Process any remaining data
if user_movie_data:
    try:
        for (user, movie), (latest_timestamp, count) in user_movie_data.items():
            cursor.execute("""
                INSERT INTO user_movie_interaction (user_id, movie_id, last_interaction, count)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id, movie_id)
                DO UPDATE SET last_interaction = EXCLUDED.last_interaction, count = EXCLUDED.count;
            """, (user, movie, latest_timestamp, count))
        conn.commit()
        stats['interactions_processed'] += len(user_movie_data)
    except Exception as e:
        conn.rollback()
        print(f"Error processing final user-movie interactions: {e}")
        stats['db_errors'] += 1

if user_ratings:
    try:
        for (user, movie), rating in user_ratings.items():
            cursor.execute("""
                INSERT INTO user_ratings (user_id, movie_id, rating)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id, movie_id)
                DO UPDATE SET rating = EXCLUDED.rating;
            """, (user, movie, rating))
        conn.commit()
        stats['ratings_processed'] += len(user_ratings)
    except Exception as e:
        conn.rollback()
        print(f"Error processing final user ratings: {e}")
        stats['db_errors'] += 1

# Get final database counts
try:
    cursor.execute("SELECT COUNT(*) FROM movies")
    movie_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM users")
    user_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM user_ratings")
    rating_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM user_movie_interaction")
    interaction_count = cursor.fetchone()[0]
except Exception as e:
    print(f"Error getting database counts: {e}")
    movie_count = user_count = rating_count = interaction_count = "ERROR"

# Add this at the end of your script, before closing connections
print("\n========== FINAL SUMMARY ==========")
print(f"Total messages processed: {message_count}")
print(f"Movies inserted: {stats['movies_inserted']}")
print(f"Users inserted: {stats['users_inserted']}")
print(f"Ratings processed: {stats['ratings_processed']}")
print(f"Interactions processed: {stats['interactions_processed']}")
print(f"Skipped ratings: {stats['skipped_ratings']}")
print(f"Skipped interactions: {stats['skipped_interactions']}")
print(f"API errors: {stats['api_errors']}")
print(f"DB errors: {stats['db_errors']}")
print("====================================\n")

print("\n---------- DATABASE COUNTS ----------")
print(f"Movies in database: {movie_count}")
print(f"Users in database: {user_count}")
print(f"Ratings in database: {rating_count}")
print(f"Interactions in database: {interaction_count}")
print("-------------------------------------\n")

# Close connections
cursor.close()
conn.close()
