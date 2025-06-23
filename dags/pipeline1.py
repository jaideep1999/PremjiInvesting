import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging
import os

# Set the AIRFLOW_HOME environment variable
os.environ['AIRFLOW_HOME'] = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

from scripts.news_processor import run_news_pipeline

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
    dag_id='news_sentiment_pipeline_v1',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule_interval='0 19 * * 1-5',  # 7 PM on weekdays
    catchup=False,
    tags=['data-engineering', 'web-scraping'],
    doc_md="""
    ### News Sentiment Pipeline
    This pipeline performs the following steps:
    1.  Fetches the 5 latest articles for 'HDFC' and 'Tata Motors' from YourStory and Finshots.
    2.  Cleans and processes the text data, performing deduplication.
    3.  Generates a mock sentiment score for each article.
    4.  Persists the final scores and relevant data into a SQLite database.
    """,
    default_args={
        'on_failure_callback': mock_alert_api
    }
) as dag:
    
    fetch_and_process_news = PythonOperator(
        task_id='fetch_process_and_store_news',
        python_callable=run_news_pipeline,
        doc_md="""
        #### Task: Fetch, Process, and Store News
        This single task encapsulates the entire workflow for Pipeline 1
        to ensure atomicity. It calls a master function which orchestrates
        scraping, cleaning, sentiment analysis, and database persistence.
        """
    )
