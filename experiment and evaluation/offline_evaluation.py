import pandas as pd
from surprise import Dataset, Reader
from surprise import accuracy
from sklearn.model_selection import train_test_split
from API.app import RecommendationAPI

model_paths = {
        'old': 'ML/models/SVDCF_OLD.pkl',  # Control group model
        'new': 'ML/models/SVDCF.pkl'      # Experiment group model
    }

recommendation_api = RecommendationAPI(model_paths, verbose=False)

# Calculate Precision@K
def calculate_precision_at_k(predictions, k=10):
    user_est_true = {}
    for uid, iid, true_r, est, _ in predictions:
        if uid not in user_est_true:
            user_est_true[uid] = []
        user_est_true[uid].append((est, true_r))

    precisions = []
    for uid, user_ratings in user_est_true.items():
        user_ratings.sort(key=lambda x: x[0], reverse=True)
        top_k = user_ratings[:k]
        relevant_count = sum((true_r >= 4) for _, true_r in top_k)
        precisions.append(relevant_count / k)

    return sum(precisions) / len(precisions) if precisions else 0


def offline_evaluation(data_path):
    try:
        # Load the dataset
        df = pd.read_csv(data_path)
        df.dropna(inplace=True)

        # Adjust based on the actual column name
        rating_column = 'rating' if 'rating' in df.columns else 'watch_count'
        reader = Reader(rating_scale=(1, 5))
        data = Dataset.load_from_df(df[['user_id', 'movie_id', rating_column]], reader)

        # Train-test split
        trainset, testset = train_test_split(df, test_size=0.2)

        # Generate recommendations for each user in the test set
        user_recommendations = {
            user_id: recommendation_api.get_recommendations(user_id) or [] for user_id in df['user_id'].unique()
        }

        # Predictions for test data
        predictions = []
        for _, row in testset.iterrows():
            uid, iid, true_r = row['user_id'], row['movie_id'], row[rating_column]
            recommended_movies = recommendation_api.get_recommendations(uid)

            # Safely check if the recommendation is not None or empty
            if recommended_movies and iid in recommended_movies:
                est = 1  # Predicted as relevant
            else:
                est = 0  # Predicted as not relevant
            
            predictions.append((uid, iid, true_r, est, None))

        # Calculate Precision@K
        precision = calculate_precision_at_k(predictions, k=10)

        # Print and save results
        print(f"Offline Precision@10: {precision:.4f}")
        with open("offline_evaluation_output.txt", 'w') as f:
            f.write(f"Offline Precision@10: {precision:.4f}\n")

    except Exception as e:
        print(f"An error occurred during offline evaluation: {e}")
        with open("offline_evaluation_output.txt", 'w') as f:
            f.write(f"Error during offline evaluation: {str(e)}\n")


if __name__ == "__main__":
    offline_evaluation("ML/user_movie.csv")
