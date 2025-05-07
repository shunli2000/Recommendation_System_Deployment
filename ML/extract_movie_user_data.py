import requests
import csv
import json

# Define API base URL
api_base_url = "http://128.2.204.215:8080"

# Step 1: Extract unique user IDs and movie IDs from the log file
user_ids = set()  # Use sets to store unique values
movie_ids = set()

with open("movielog7_raw.txt", "r") as file:
    for line in file:
        parts = line.strip().split(",")
        if len(parts) > 2:
            userid = parts[1]  # Extract user ID
            movie_path = parts[2]  # Example: "/data/m/exporting+raymond+2011/66.mpg"
            movie_id = movie_path.split("/")[-2]  # Extract movie ID "exporting+raymond+2011"

            user_ids.add(userid)
            movie_ids.add(movie_id)

"""
# Step 2: Query API for user data and store in users_data.csv
with open("users_data.csv", "w", newline="") as user_file:
    user_writer = csv.writer(user_file)
    user_writer.writerow(["user_id", "age", "occupation", "gender"])  # CSV header

    for user_id in user_ids:
        response = requests.get(f"{api_base_url}/user/{user_id}")
        if response.status_code == 200:
            data = response.json()
            user_writer.writerow([data["user_id"], data["age"], data["occupation"], data["gender"]])
        else:
            print(f"Failed to fetch user data for {user_id}")

print("User data successfully saved to users_data.csv")
"""

# Step 3: Query API for movie data and store in movies_data.csv
with open("movies_data.csv", "w", newline="") as movie_file:
    movie_writer = csv.writer(movie_file)
    movie_writer.writerow(["movie_id", "title", "original_title", "tmdb_id", "imdb_id", 
                           "genres", "budget", "revenue", "popularity", "runtime", 
                           "vote_average", "release_date", "production_companies", 
                           "production_countries", "spoken_languages"])  # Updated CSV header

    for movie_id in movie_ids:
        response = requests.get(f"{api_base_url}/movie/{movie_id}")
        if response.status_code == 200:
            data = response.json()

            # Extract required fields
            genres = ", ".join([genre["name"] for genre in data.get("genres", [])])
            production_companies = ", ".join([comp["name"] for comp in data.get("production_companies", [])])
            production_countries = ", ".join([country["name"] for country in data.get("production_countries", [])])
            spoken_languages = ", ".join([lang["name"] for lang in data.get("spoken_languages", [])])

            movie_writer.writerow([data["id"], data["title"], data["original_title"], 
                                   data.get("tmdb_id", ""), data.get("imdb_id", ""), 
                                   genres, data.get("budget", ""), data.get("revenue", ""), 
                                   data.get("popularity", ""), data.get("runtime", ""), 
                                   data.get("vote_average", ""), data.get("release_date", ""), 
                                   production_companies, production_countries, spoken_languages])
        else:
            print(f"Failed to fetch movie data for {movie_id}")

print("Movie data successfully saved to movies_data.csv")
