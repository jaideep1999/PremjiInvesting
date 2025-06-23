import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import timedelta
import logging
import os

# Set the AIRFLOW_HOME environment variable
os.environ['AIRFLOW_HOME'] = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

from scripts.movielens_processor import (
    download_movielens_data,
    process_mean_age_by_occupation,
    process_top_20_rated_movies,
    process_top_genres_by_user_group,
    process_top_10_similar_movies
)

# Configure logging
logging.basicConfig(level=logging.INFO)

def mock_alert_api(context):
    """Mocks an API call to send an alert on task failure."""
    task_instance = context.get('task_instance')
    log_url = task_instance.log_url
    logging.error(
        f"!!! ALERT: Task Failed! !!!\n"
        f"DAG: {task_instance.dag_id}\n"
        f"Task: {task_instance.task_id}\n"
        f"Execution Time: {context.get('execution_date')}\n"
        f"Log URL: {log_url}\n"
        f"Reason: {context.get('exception')}"
    )

with DAG(
    dag_id='movielens_analysis_pipeline_v1',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule_interval='0 20 * * 1-5',  # 8 PM on weekdays
    catchup=False,
    tags=['data-engineering', 'analysis'],
    doc_md="""
    ### MovieLens 100k Analysis Pipeline
    This pipeline runs after the news sentiment pipeline and performs analysis on the MovieLens 100k dataset.
    - **Task 1**: Downloads the dataset.
    - **Task 2**: Finds the mean age of users in each occupation.
    - **Task 3**: Finds the top 20 highest-rated movies.
    - **Task 4**: Finds the top genres rated by users of each occupation in defined age groups.
    - **Task 5**: Given a movie, finds the top 10 most similar movies based on user ratings.
    """,
    default_args={
        'on_failure_callback': mock_alert_api,
        'retries': 1,
        'retry_delay': timedelta(minutes=2)
    }
) as dag:

    wait_for_pipeline_1 = ExternalTaskSensor(
        task_id='wait_for_news_sentiment_pipeline',
        external_dag_id='news_sentiment_pipeline_v1',
        external_task_id='fetch_process_and_store_news',
        timeout=600,
        allowed_states=['success'],
        mode='poke',
    )
    
    task_1_download_data = PythonOperator(
        task_id='download_movielens_data',
        python_callable=download_movielens_data
    )

    task_2_mean_age = PythonOperator(
        task_id='find_mean_age_by_occupation',
        python_callable=process_mean_age_by_occupation
    )

    task_3_top_movies = PythonOperator(
        task_id='find_top_20_rated_movies',
        python_callable=process_top_20_rated_movies
    )

    task_4_top_genres = PythonOperator(
        task_id='find_top_genres_by_user_group',
        python_callable=process_top_genres_by_user_group
    )

    task_5_similar_movies = PythonOperator(
        task_id='find_top_10_similar_movies',
        python_callable=process_top_10_similar_movies,
        op_kwargs={'target_movie_title': 'Usual Suspects, The (1995)'}
    )
    
    wait_for_pipeline_1 >> task_1_download_data >> [task_2_mean_age, task_3_top_movies, task_4_top_genres, task_5_similar_movies]

