This project contains a fully containerized solution for the data engineering assignment, featuring two data pipelines orchestrated with Apache Airflow.

## Overview

- **Pipeline 1 (`news_sentiment_pipeline_v1`):** Scrapes news articles for specified tickers from web sources, calculates a mock sentiment score, and stores the results in a SQLite database.
  - **Schedule:** 7 PM on weekdays.
- **Pipeline 2 (`movielens_analysis_pipeline_v1`):** Analyzes the MovieLens 100k dataset. This pipeline only runs if Pipeline 1 succeeds for the same day.
  - **Schedule:** 8 PM on weekdays.
  - **Tasks:**
    1.  Calculates the mean age of users per occupation.
    2.  Finds the top 20 highest-rated movies.
    3.  Determines top genres by user occupation and age group.
    4.  Finds the top 10 movies similar to a given target movie.

## How to Run

### Prerequisites

-   Docker
-   Docker Compose

### Setup

1.  **Clone the Repository:**
    ```bash
    git clone <your-repo-url>
    cd premji_invest_assignment
    ```

2.  **Run the Setup Script:**
    This script will build the Docker image and start all Airflow services.

    ```bash
    bash setup.sh
    ```

3.  **Access Airflow UI:**
    -   Open your web browser and navigate to `http://localhost:8080`.
    -   Log in with the credentials:
        -   **Username:** `airflow`
        -   **Password:** `airflow`

4.  **Enable and Trigger DAGs:**
    -   In the Airflow UI, you will see two DAGs: `news_sentiment_pipeline_v1` and `movielens_analysis_pipeline_v1`.
    -   By default, they are paused. Click the toggle switch next to their names to enable them.
    -   The pipelines will run on their defined schedules. To run them manually for a demonstration, click the "Play" button next to a DAG and select "Trigger DAG".

## Evidence of a Working Version

You can verify the pipelines are working by:

1.  **Checking DAG Runs:** In the Airflow UI, navigate to the "DAGs" page. A successful run will be marked with a green circle.

    ![Screenshot of Airflow UI with successful DAG runs](https://i.imgur.com/example-dag-run.png) <!-- Replace with actual screenshot URL -->

2.  **Viewing Task Logs:** Click on a DAG, then on a specific run in the "Grid" view. You can then click on any task to view its logs. The logs for the analysis tasks in Pipeline 2 will print the results (e.g., Top 20 movies, similar movies) directly.

    ![Screenshot of Task Logs showing results](https://i.imgur.com/example-task-log.png) <!-- Replace with actual screenshot URL -->

3.  **Inspecting the Database (Optional):** You can inspect the SQLite database created by Pipeline 1.
    ```bash
    # Find the container ID for the scheduler
    docker ps

    # Exec into the container
    docker exec -it <scheduler_container_id> bash

    # Use sqlite3 to query the database
    sqlite3 /opt/airflow/dags/news_sentiment.db "SELECT * FROM sentiment_scores LIMIT 5;"
    ```

## Assumptions and Justifications

-   **Web Scraping:** The selectors used to scrape `YourStory` and `Finshots` are based on their current HTML structure and are subject to change. A production system would require more robust, possibly headless-browser-based scraping and monitoring for website changes.
-   **Sentiment API:** The sentiment analysis is mocked with a random number generator. This correctly simulates the pipeline's structure without requiring a trained ML model.
-   **Movie Similarity:** The similarity `score` in the output is the raw Pearson correlation. The "95% similarity threshold" in the prompt was interpreted as a very high correlation, but since the example output scores are floats like `0.98`, I used the correlation value directly as the score, which is a standard approach. The co-occurrence (strength) is implemented exactly as described.
-   **CI/CD:** A `ci.yml` file for GitHub Actions is not included but would typically involve steps to lint the Python code (e.g., using `flake8`), and build the Docker image to ensure it doesn't break.

