import psycopg2
import pandas as pd
from sklearn.metrics import mean_squared_error
from math import sqrt
from surprise import Dataset, Reader
import os
import pickle
from ML.SVDCFModel import SVDCFModel

# Set environment variable for client encoding
os.environ['PGCLIENTENCODING'] = 'UTF8'

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
def load_data():
    query = r"""
    SELECT ur.user_id, ur.movie_id, ur.rating
    FROM user_ratings ur
    WHERE ur.rating ~ '^[0-9]+(\.[0-9]+)?$' AND CAST(ur.rating AS FLOAT) > 0 AND CAST(ur.rating AS FLOAT) <= 5
    """
    cursor.execute(query)
    data = cursor.fetchall()
    columns = ['user_id', 'movie_id', 'rating']
    df = pd.DataFrame(data, columns=columns)
    df.dropna(inplace=True)
    return df


# Load the trained model
def load_model(model_path):
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
        return model

# Evaluate the model
def evaluate_model(model, df):
    reader = Reader(rating_scale=(df['rating'].min(), df['rating'].max()))
    data = Dataset.load_from_df(df[['user_id', 'movie_id', 'rating']], reader)
    trainset = data.build_full_trainset()
    predictions = model.model.test(trainset.build_testset())
    y_true = [pred.r_ui for pred in predictions]
    y_pred = [pred.est for pred in predictions]
    rmse = sqrt(mean_squared_error(y_true, y_pred))
    return rmse

# Main function
def main():

    # Load the model
    model_path = os.path.join("ML", "models", "SVDCF.pkl")
    model = load_model(model_path)
    
    # Load new data from Kafka
    new_df = load_data()
    
    # Evaluate the model on new data
    rmse_new = evaluate_model(model, new_df)
    print(f"Model RMSE on new data: {rmse_new}")
    assert rmse_new < 1.0, "Model performance on new data is not satisfactory"

if __name__ == "__main__":
    main()

# Close connections
cursor.close()
conn.close()