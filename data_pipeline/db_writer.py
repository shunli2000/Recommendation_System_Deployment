import psycopg2

def get_db_connection():
    conn = psycopg2.connect(
        dbname="movie_data",
        user="postgres",
        password="mlip-007",
        host="localhost",
        port="5432"
    )
    return conn, conn.cursor()

def insert_movie(cursor, movie_id, movie_info, genres):
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

def insert_user(cursor, user_id, user_info):
    cursor.execute("""
        INSERT INTO users (user_id, age, occupation, gender)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (user_id) DO NOTHING;
    """, (user_id, user_info.get('age'), user_info.get('occupation'), user_info.get('gender')))

def upsert_interaction(cursor, user_movie_data):
    for (user, movie), (ts, count) in user_movie_data.items():
        cursor.execute("""
            INSERT INTO user_movie_interaction (user_id, movie_id, last_interaction, count)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id, movie_id)
            DO UPDATE SET last_interaction = EXCLUDED.last_interaction, 
                          count = user_movie_interaction.count + EXCLUDED.count;
        """, (user, movie, ts, count))

def upsert_ratings(cursor, user_ratings):
    for (user, movie), rating in user_ratings.items():
        cursor.execute("""
            INSERT INTO user_ratings (user_id, movie_id, rating)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id, movie_id)
            DO UPDATE SET rating = EXCLUDED.rating;
        """, (user, movie, rating))

def get_reference_distribution(table, column):
    """
    Fetches a column's values from the specified table.
    Returns a flat list of values.
    """
    query = f"SELECT {column} FROM {table};"
    try:
        conn, cursor = get_db_connection()
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return [row[0] for row in data if row[0] is not None]
    except Exception as e:
        print(f"Error fetching reference data from {table}.{column}: {e}")
        return []
