import psycopg2
import pandas as pd
from scipy.stats import ttest_ind

# PostgreSQL connection setup
conn = psycopg2.connect(
    dbname="movie_data",
    user="postgres",
    password="mlip-007",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

def load_user_ratings():
    """Load user ratings from the database."""
    query = r"""
    SELECT user_id, rating
    FROM user_ratings
    WHERE rating ~ '^[0-9]+(\.[0-9]+)?$' AND CAST(rating AS FLOAT) > 0 AND CAST(rating AS FLOAT) <= 5
    """
    cursor.execute(query)
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=['user_id', 'rating'])
    df['rating'] = df['rating'].astype(float)
    df.dropna(inplace=True)
    return df

def load_user_interactions():
    """Load user interactions (watch count) from the database."""
    query = """
    SELECT user_id, count
    FROM user_movie_interaction
    WHERE user_id ~ '^[0-9]+$' and count IS NOT NULL and count> 0 AND count < 100
    """
    cursor.execute(query)
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=['user_id', 'count'])
    return df

def split_users(df):
    """Split users into control and experiment groups based on user_id."""
    # Filter out rows with non-numeric user_id
    df = df[df['user_id'].astype(str).str.isdigit()].copy()  # Use `.copy()` to avoid SettingWithCopyWarning
    
    # Convert user_id to integer for splitting
    df.loc[:, 'user_id'] = df['user_id'].astype(int)
    
    # Assign group based on user_id
    df.loc[:, 'group'] = df['user_id'].apply(lambda x: 'control' if x % 2 != 0 else 'experiment')
    return df

def perform_t_test(control_data, experiment_data, metric_name):
    """Perform a t-test and print the results, including averages."""
    control_avg = control_data.mean()
    experiment_avg = experiment_data.mean()
    t_stat, p_value = ttest_ind(control_data, experiment_data, equal_var=False)
    
    print(f"T-test for {metric_name}:")
    print(f"Control group average: {control_avg:.4f}")
    print(f"Experiment group average: {experiment_avg:.4f}")
    print(f"T-statistic: {t_stat:.4f}, P-value: {p_value:.4f}")
    
    if p_value < 0.05:
        print(f"Result: Significant difference in {metric_name} between control and experiment groups.\n")
    else:
        print(f"Result: No significant difference in {metric_name} between control and experiment groups.\n")

def main():
    # Load user ratings and interactions
    ratings_df = load_user_ratings()
    interactions_df = load_user_interactions()

    # Split users into control and experiment groups
    ratings_df = split_users(ratings_df)
    interactions_df = split_users(interactions_df)

    # Group data by control and experiment
    control_ratings = ratings_df[ratings_df['group'] == 'control']['rating']
    experiment_ratings = ratings_df[ratings_df['group'] == 'experiment']['rating']

    control_counts = interactions_df[interactions_df['group'] == 'control']['count']
    experiment_counts = interactions_df[interactions_df['group'] == 'experiment']['count']

    # Perform t-tests
    perform_t_test(control_ratings, experiment_ratings, "ratings")
    perform_t_test(control_counts, experiment_counts, "watch counts")

if __name__ == "__main__":
    main()

# Close the database connection
cursor.close()
conn.close()