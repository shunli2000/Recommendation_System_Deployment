import mlflow
import pandas as pd

uri = "http://127.0.0.1:6001"
mlflow.set_tracking_uri(uri=uri)

logged_model = "runs:/d8bc7cb77e764eedacfd629f6718b417/svd_recommender"  

loaded_model = mlflow.pyfunc.load_model(logged_model)

test_data = pd.DataFrame({
    'user_id': [1, 2, 3],  
    'movie_id': [101, 102, 103]  
})

predictions = loaded_model.predict(test_data)

print("Predicted watch counts:")
for i, (user, movie) in enumerate(zip(test_data['user_id'], test_data['movie_id'])):
    print(f"User {user} for Movie {movie}: {predictions[i]:.2f}")

try:
    svd_model = loaded_model._model_impl.python_model.model
    user_id = 1 
    top_n = svd_model.test(svd_model.trainset.build_testset()) 
    print(f"\nTop recommendations for user {user_id} would go here")
except AttributeError:
    print("\nNote: For full recommendation functionality, use the original model class directly")
