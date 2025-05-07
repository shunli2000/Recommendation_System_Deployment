import pandas as pd
from surprise import Dataset, Reader, SVD
from surprise.model_selection import train_test_split, GridSearchCV
from surprise import accuracy
import pickle

class SVDCFModel:
    def __init__(self):
        self.model = None

    def train(self, data_path):
        # Load data
        df = pd.read_csv(data_path)
        df.dropna(inplace=True)
        reader = Reader(rating_scale=(df['watch_count'].min(), df['watch_count'].max()))
        data = Dataset.load_from_df(df[['user_id', 'movie_id', 'watch_count']], reader)

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
    best_params = model.train('ML/user_movie.csv')
    print("Model trained successfully!")
    model.save_model('models/SVDCF.pkl')
    print("Model saved successfully!")
    
