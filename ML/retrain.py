import pandas as pd
from surprise import Dataset, Reader, SVD
from surprise.model_selection import train_test_split, GridSearchCV
from surprise import accuracy
import pickle
import os
import psycopg2

class SVDCFModel:
    def __init__(self):
        self.model = None
    
    def load_data_from_db(self):
        # PostgreSQL connection setup
        conn = psycopg2.connect(
            dbname="movie_data",
            user="postgres",
            password="mlip-007",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        # Load data from the database
        query = """
        SELECT ur.user_id, ur.movie_id, CAST(ur.rating AS FLOAT) AS rating
        FROM user_ratings ur
        WHERE ur.rating ~ '^[0-9]+(\.[0-9]+)?$' AND CAST(ur.rating AS FLOAT) > 0 AND CAST(ur.rating AS FLOAT) <= 5
        """
        cursor.execute(query)
        data = cursor.fetchall()
        columns = ['user_id', 'movie_id', 'rating']
        df = pd.DataFrame(data, columns=columns)

        # Ensure correct data types
        df['user_id'] = df['user_id'].astype(str)
        df['movie_id'] = df['movie_id'].astype(str)
        df['rating'] = df['rating'].astype(float)
        df.dropna(inplace=True)

        # Close the connection
        cursor.close()
        conn.close()

        return df

    def train(self):
        # Load data
        df = self.load_data_from_db()
        reader = Reader(rating_scale=(df['rating'].min(), df['rating'].max()))
        data = Dataset.load_from_df(df[['user_id', 'movie_id', 'rating']], reader)

        # Split data into train and test sets
        self.trainset, testset = train_test_split(data, test_size=0.2)

        # Define parameter grid
        param_grid = {
            "n_epochs": [5, 10],
            "lr_all": [0.002, 0.005],
            "reg_all": [0.4, 0.6]
        }

        # Perform grid search
        gs = GridSearchCV(SVD, param_grid, measures=["rmse"], cv=3)
        gs.fit(data)
        best_params = gs.best_params["rmse"]

        # Train model with best parameters
        self.model = SVD(n_epochs=best_params['n_epochs'],
                         lr_all=best_params['lr_all'],
                         reg_all=best_params['reg_all'])
        self.model.fit(self.trainset)

        # Evaluate the model
        predictions = self.model.test(testset)
        accuracy.rmse(predictions)

        # Return best parameters as a list
        return [best_params['n_epochs'], best_params['lr_all'], best_params['reg_all']]

    def save_model(self, model_path):
        # Rename the current model file if it exists
        old_model_path = model_path.replace("SVDCF.pkl", "SVDCF_OLD.pkl")
        if os.path.exists(model_path):
            os.rename(model_path, old_model_path)
            print(f"Old model renamed to {old_model_path}")
        # Save the new model
        with open(model_path, 'wb') as f:
            pickle.dump(self, f)

    def load_model(self, model_path):
        with open(model_path, 'rb') as f:
            self.model= pickle.load(f)

    def predict(self, user_id, movie_id):
        return self.model.predict(user_id, movie_id).est

    def recommend_top_n(self, user_id, n=20):
        # Get all movie ids
        all_movie_ids = self.model.trainset.all_items()
        all_movie_ids = [self.model.trainset.to_raw_iid(iid) for iid in all_movie_ids]

        # Predict ratings for all movies for the given user
        predictions = [self.model.predict(user_id, movie_id) for movie_id in all_movie_ids]

        # Sort predictions by estimated rating
        predictions.sort(key=lambda x: x.est, reverse=True)

        # Get the top n movie ids
        top_n_movie_ids = [pred.iid for pred in predictions[:n]]

        return top_n_movie_ids

if __name__ == "__main__":
    model = SVDCFModel()
    print("Training model...")
    best_params = model.train()
    print("Model trained successfully!")
    model.save_model('models/SVDCF.pkl')
    print("Model saved successfully!")
    
