def validate_movie_schema(movie_info):
    required_fields = ['title', 'adult', 'genres', 'release_date', 'budget', 'revenue', 'popularity', 'runtime']
    return all(field in movie_info and movie_info[field] is not None for field in required_fields)

def validate_user_schema(user_info):
    required_fields = ['age', 'occupation', 'gender']
    return all(field in user_info and user_info[field] is not None for field in required_fields)
