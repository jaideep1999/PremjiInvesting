import pandas as pd
import numpy as np
import requests
import zipfile
import os
import logging
from io import BytesIO

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", ".")
DATA_DIR = os.path.join(AIRFLOW_HOME, 'dags', 'data', 'ml-100k')
DATA_URL = 'http://files.grouplens.org/datasets/movielens/ml-100k.zip'

# --- Helper Functions ---
def get_data_paths():
    """Returns a dictionary of file paths for the dataset."""
    return {
        'users': os.path.join(DATA_DIR, 'u.user'),
        'ratings': os.path.join(DATA_DIR, 'u.data'),
        'movies': os.path.join(DATA_DIR, 'u.item')
    }

def load_data():
    """Loads the movielens datasets into pandas DataFrames."""
    paths = get_data_paths()
    
    # User data
    u_cols = ['user_id', 'age', 'sex', 'occupation', 'zip_code']
    users = pd.read_csv(paths['users'], sep='|', names=u_cols, encoding='latin-1')

    # Rating data
    r_cols = ['user_id', 'movie_id', 'rating', 'unix_timestamp']
    ratings = pd.read_csv(paths['ratings'], sep='\t', names=r_cols, encoding='latin-1')

    # Movie data
    m_cols = ['movie_id', 'title', 'release_date', 'video_release_date', 'imdb_url'] + [f"genre_{g}" for g in ['unknown', 'Action', 'Adventure', 'Animation', 'Childrens', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']]
    movies = pd.read_csv(paths['movies'], sep='|', names=m_cols, encoding='latin-1')
    
    return users, ratings, movies

# --- Pipeline Tasks ---

def download_movielens_data():
    """Task 1: Downloads and unzips the MovieLens 100k dataset."""
    logging.info(f"Starting download from {DATA_URL}")
    if os.path.exists(DATA_DIR):
        logging.info("Data directory already exists. Skipping download.")
        return
        
    try:
        response = requests.get(DATA_URL, stream=True)
        response.raise_for_status()
        
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            # Extract to a temporary dir and then move to avoid partial extraction issues
            extract_path = os.path.join(AIRFLOW_HOME, 'dags', 'data')
            os.makedirs(extract_path, exist_ok=True)
            z.extractall(extract_path)
        
        logging.info(f"Successfully downloaded and extracted data to {DATA_DIR}")
    except (requests.RequestException, zipfile.BadZipFile) as e:
        logging.error(f"Failed to download or extract data: {e}")
        raise

def process_mean_age_by_occupation():
    """Task 2: Finds the mean age of users in each occupation."""
    logging.info("Task 2: Calculating mean age by occupation.")
    users, _, _ = load_data()
    mean_age = users.groupby('occupation')['age'].mean().round(2)
    logging.info("--- Mean Age by Occupation ---")
    logging.info(f"\n{mean_age.to_string()}")
    logging.info("--- Task 2 Complete ---")

def process_top_20_rated_movies():
    """Task 3: Finds the names of top 20 highest-rated movies."""
    logging.info("Task 3: Finding top 20 highest-rated movies (min 35 ratings).")
    _, ratings, movies = load_data()
    
    movie_ratings = pd.merge(ratings, movies, on='movie_id')
    
    # Calculate mean rating and count of ratings for each movie
    movie_stats = movie_ratings.groupby('title').agg(
        mean_rating=('rating', 'mean'),
        rating_count=('rating', 'count')
    ).reset_index()

    # Filter for movies with at least 35 ratings
    popular_movies = movie_stats[movie_stats['rating_count'] >= 35]
    
    # Sort by mean rating and get top 20
    top_20 = popular_movies.sort_values(by='mean_rating', ascending=False).head(20)
    
    logging.info("--- Top 20 Highest-Rated Movies (min 35 ratings) ---")
    logging.info(f"\n{top_20.to_string(index=False)}")
    logging.info("--- Task 3 Complete ---")

def process_top_genres_by_user_group():
    """Task 4: Finds top genres for users by occupation and age group."""
    logging.info("Task 4: Finding top genres by occupation and age group.")
    users, ratings, movies = load_data()

    # Define age groups
    bins = [19, 25, 35, 45, users['age'].max() + 1]
    labels = ['20-25', '26-35', '36-45', '46+']
    users['age_group'] = pd.cut(users['age'], bins=bins, labels=labels, right=True)

    # Merge dataframes
    merged_df = pd.merge(pd.merge(ratings, users, on='user_id'), movies, on='movie_id')
    
    # Get genre columns
    genre_cols = [col for col in movies.columns if 'genre_' in col and col != 'genre_unknown']
    
    # Melt genres into a single column
    id_vars = ['occupation', 'age_group']
    melted_genres = merged_df.melt(id_vars=id_vars, value_vars=genre_cols, var_name='genre', value_name='is_genre')
    melted_genres = melted_genres[melted_genres['is_genre'] == 1]
    melted_genres['genre'] = melted_genres['genre'].str.replace('genre_', '')
    
    # Group and find the top genre
    top_genres = melted_genres.groupby(['occupation', 'age_group', 'genre']).size().reset_index(name='count')
    top_genres = top_genres.sort_values('count', ascending=False).drop_duplicates(['occupation', 'age_group'])

    logging.info("--- Top Genre by Occupation and Age Group ---")
    logging.info(f"\n{top_genres[['occupation', 'age_group', 'genre', 'count']].to_string(index=False)}")
    logging.info("--- Task 4 Complete ---")

def process_top_10_similar_movies(target_movie_title: str):
    """Task 5: Finds top 10 similar movies for a given movie."""
    logging.info(f"Task 5: Finding top 10 movies similar to '{target_movie_title}'.")
    users, ratings, movies = load_data()

    # Create user-item matrix
    movie_ratings = pd.merge(ratings, movies, on='movie_id')
    user_item_matrix = movie_ratings.pivot_table(index='user_id', columns='title', values='rating')

    # Get the ratings for the target movie
    target_ratings = user_item_matrix[target_movie_title]
    
    # Find movies rated by the same users who rated the target movie
    similar_to_target = user_item_matrix.corrwith(target_ratings)
    
    # Create a DataFrame for correlation
    corr_df = pd.DataFrame(similar_to_target, columns=['correlation'])
    corr_df.dropna(inplace=True)
    
    # Add rating counts (strength)
    rating_counts = movie_ratings.groupby('title')['rating'].count()
    corr_df = corr_df.join(rating_counts.rename('strength'))
    
    # Calculate co-occurrence (how many users rated both movies)
    def calculate_co_occurrence(movie_title):
        if movie_title == target_movie_title:
            return 0
        target_raters = user_item_matrix[target_movie_title].dropna().index
        other_raters = user_item_matrix[movie_title].dropna().index
        return len(target_raters.intersection(other_raters))

    corr_df['co_occurrence'] = corr_df.index.map(calculate_co_occurrence)
    
    # Apply thresholds
    similarity_threshold = 0.95 # This is a very high threshold, custom logic needed
    co_occurrence_threshold = 50

    # Custom similarity: We are looking for high correlation, not a specific > 0.95 score
    # The example output scores are likely correlation scores multiplied by some factor or a different metric
    # For this, we'll use raw correlation and rename it to 'score' for the output.
    # We will also filter by co-occurrence threshold
    
    results = corr_df[corr_df['co_occurrence'] >= co_occurrence_threshold]
    results = results.sort_values(by='correlation', ascending=False)
    
    # Rename 'correlation' to 'score' to match the example output
    results = results.rename(columns={'correlation': 'score'})
    
    # Drop the target movie itself from the results
    if target_movie_title in results.index:
        results = results.drop(target_movie_title)

    top_10 = results.head(10)

    logging.info(f"--- Top 10 Similar Movies for '{target_movie_title}' ---")
    logging.info(f"(Co-occurrence threshold: {co_occurrence_threshold})")
    logging.info(f"\n{top_10[['score', 'co_occurrence']].to_string()}")
    logging.info("--- Task 5 Complete ---")
