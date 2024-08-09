import pandas as pd
import time
import random
from imdb import IMDb, IMDbError
from concurrent.futures import ThreadPoolExecutor, as_completed
import ast
import requests

# API Key
my_api_key = 'c7764722dac6a2e287a0924a16be4078'


# Data Cleaning
def clean_currency_columns(df: pd.DataFrame, *column_names: str) -> pd.DataFrame :
    """
    Clean currency columns by removing dollar signs and commas, 
    then converting the columns to float.

    Parameters:
    df(pd.Dataframe): The dataframe containing the colums to clean. 
    *column_names(str): Column names to clean, provided as individual arguments.

    Returns: 
    pd.DataFrame: The dataframe with cleaned columns.    
    """

    for column_name in column_names:
        if not isinstance(column_name, str):
            raise TypeError(f"Column name must be a sting. Invalid argument : {column_name}")
        
        if column_name in df.columns:
            # Check if the column is already of type float
            if pd.api.types.is_float_dtype(df[column_name]):
                print(f"Column '{column_name}' is already cleaned and of type float.")
                continue

        if column_name in df.columns:
            df[column_name] = df[column_name].str.replace('$','' ,regex= False)
            df[column_name] = df[column_name].str.replace(',','' ,regex= False)
            df[column_name] = df[column_name].astype(float)
        
        else:
            print(f"Warning: Column '{column_name}' does not exist in the dataframe.")


    return df

# API call functions 

def get_genre_names(api_key):
    """
    Retrieve genre names from TMDB API and return a dictionary mapping genre IDs to genre names.

    Parameters:
    - api_key (str): Your TMDB API key.

    Returns:
    - genre_dict (dict): A dictionary mapping genre IDs to genre names.
    """
    url = f"https://api.themoviedb.org/3/genre/movie/list?api_key={api_key}&language=en"
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        genre_dict = {genre['id']: genre['name'] for genre in data['genres']}
        return genre_dict
    else:
        print("Failed to retrieve genre names from TMDB API.")
        return None
    

# Function to map genre IDs to genre names. 
def map_genre_ids(genre_ids,genre_dict):

    """
    When a csv dataset is converted into a pandas dataframe , if there are
    lists in any of the columns, the lists are stored as string objects (not actual strings)
    Hence the use of the literal_eval() function from the ast library to convert the 
    list string objects into actual lists.
    
    """
    genre_list = ast.literal_eval(genre_ids)
    return [genre_dict[genre_id] for genre_id in genre_list]

# Initialize IMDb instance
ia = IMDb()

# Initialize counters
successful_requests = 0
unsuccessful_requests = 0
start_time = time.time()

def fetch_genre_imdb(movie_name):
    global successful_requests, unsuccessful_requests
    try:
        # Search for the movie
        results = ia.search_movie(movie_name)

        # If no results, increment unsuccessful_requests
        if not results:
            unsuccessful_requests += 1
            return movie_name, None

        # Get the first result (most likely to be correct)
        movie = results[0]
        ia.update(movie)

        # Return the genre(s)
        if 'genres' in movie.keys():
            successful_requests += 1
            return movie_name, movie['genres']
        else:
            unsuccessful_requests += 1
            return movie_name, None
    except IMDbError as e:
        # Handle the IMDb error
        print(f"Error fetching {movie_name}: {e}")
        unsuccessful_requests += 1
        return movie_name, None

def batch_fetch_genres(movie_names, batch_size=10, max_retries=5):
    genres = {}
    total_requests = len(movie_names)
    elapsed_times = []

    with ThreadPoolExecutor(max_workers=batch_size) as executor:
        future_to_movie = {executor.submit(fetch_genre_imdb, movie): movie for movie in movie_names}
        
        for i, future in enumerate(as_completed(future_to_movie)):
            start_request_time = time.time()
            
            movie_name, genre = future.result()
            if genre is not None:
                genres[movie_name] = genre
            
            end_request_time = time.time()
            request_time = end_request_time - start_request_time
            elapsed_times.append(request_time)
            
            # Calculate remaining requests
            remaining_requests = total_requests - (successful_requests + unsuccessful_requests)
            
            # Calculate average time per request and estimate time left
            if elapsed_times:
                avg_time_per_request = sum(elapsed_times) / len(elapsed_times)
            else:
                avg_time_per_request = 0
            
            estimated_time_left = avg_time_per_request * remaining_requests
            
            # Print the counts and remaining requests after each movie
            print(f"After processing {i + 1}/{total_requests} movies:")
            print(f"  Successful requests: {successful_requests}")
            print(f"  Unsuccessful requests: {unsuccessful_requests}")
            print(f"  Remaining requests: {remaining_requests}")
            print(f"  Approximate time left: {estimated_time_left / 60:.2f} minutes")
            print("-" * 40)
    
    return genres

def update_dataframe_with_genres(df, movie_column, batch_size=10, max_retries=5):
    movie_names = df[movie_column].tolist()
    genre_data = batch_fetch_genres(movie_names, batch_size, max_retries)
    
    # Map the genres to the DataFrame
    df['Genre'] = df[movie_column].map(genre_data)
    
    return df


# Function to format numbers. 
def format_numbers(num):
    if abs(num) >= 1_000_000_000 or abs(num) <= 0  >= 1_000_000_000 : # Format in billions
        return f'{num/1_000_000_000:.2f}B'
    elif abs(num) >= 1_000_000 or abs(num) <= 0  >= 1_000_00:  # Format in millions
        return f'{num/1_000_000:.2f}M'
    else:
        return f'{num:,}' # Format with commas for thousands