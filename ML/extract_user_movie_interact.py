import csv

# Define file paths
input_file = "movielog7_raw.txt"  # Ensure the correct file path
output_file = "user_movie.csv"

# Dictionary to store (user_id, movie_id) -> (latest timestamp, watch count)
user_movie_data = {}
user_ratings = {}  # Store (user_id, movie_id) -> rating

# Process the input file
try:
    with open(input_file, "r") as infile:
        for line in infile:
            try:
                parts = line.strip().split(",")
                if len(parts) < 3:
                    continue  # Skip malformed lines

                timestamp = parts[0].strip()  # Extract timestamp
                user_id = parts[1].strip()  # Extract user ID
                request = parts[2].strip()  # Extract the request path

                if "/rate/" in request:
                    # Extract movie_id and rating
                    rate_parts = request.split("=")
                    if len(rate_parts) == 2:
                        movie_id = rate_parts[0].split("/")[-1]  # Extract movie_id
                        rating = rate_parts[1]  # Extract rating
                        user_ratings[(user_id, movie_id)] = rating  # Store rating

                elif "/data/m/" in request:
                    # Extract movie_id
                    movie_id = request.split("/")[-2]

                    # Check if the user-movie pair exists
                    if (user_id, movie_id) in user_movie_data:
                        # Update latest timestamp and increment count
                        existing_timestamp, count = user_movie_data[(user_id, movie_id)]
                        if timestamp > existing_timestamp:
                            user_movie_data[(user_id, movie_id)] = (timestamp, count + 1)
                        else:
                            user_movie_data[(user_id, movie_id)] = (existing_timestamp, count + 1)
                    else:
                        # First time user watches the movie
                        user_movie_data[(user_id, movie_id)] = (timestamp, 1)

            except Exception as e:
                print(f"Skipping invalid line due to error: {e}")

    # Write extracted data to CSV
    with open(output_file, "w", newline="") as outfile:
        csv_writer = csv.writer(outfile)
        csv_writer.writerow(["timestamp", "user_id", "movie_id", "watch_count", "rate"])  # CSV Header
        
        # Write the latest interactions along with watch counts and ratings
        for (user_id, movie_id), (timestamp, count) in user_movie_data.items():
            rating = user_ratings.get((user_id, movie_id), "N/A")  # Get rating, default to "N/A"
            csv_writer.writerow([timestamp, user_id, movie_id, count, rating])

    print(f"Data successfully saved to {output_file}")

except FileNotFoundError:
    print(f"Error: Input file {input_file} not found.")
except Exception as e:
    print(f"An error occurred: {e}")
