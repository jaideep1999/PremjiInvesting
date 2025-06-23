import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import random
import logging
import os
from hashlib import sha256

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
# Assumption: The AIRFLOW_HOME is set to the project root for access to the dags directory.
# In a Docker setup, this path will be consistent.
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", ".")
DB_PATH = os.path.join(AIRFLOW_HOME, 'dags', 'news_sentiment.db')

TICKERS = ['HDFC', 'Tata Motors']
MAX_ARTICLES = 5

# Assumption: These are simplified search URLs. Real-world sites can be more complex.
# YourStory search seems to be `https://yourstory.com/search?q=<query>`
# Finshots doesn't have an obvious search URL structure visible, so we'll scrape the homepage
# and filter for articles containing the tickers. This is a common workaround.
SOURCES = {
    'yourstory': 'https://yourstory.com/search?q={}',
    'finshots': 'https://finshots.in/'
}

def mock_sentiment_api(text: str) -> float:
    """
    Mocks a sentiment analysis API call.
    Returns a random float between 0 and 1.
    In a real scenario, this would be a POST request to a model endpoint.
    """
    if not isinstance(text, str) or not text.strip():
        return 0.5 # Return neutral for empty text
    return random.uniform(0, 1)

def get_soup(url):
    """Gets a BeautifulSoup object from a URL."""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        return BeautifulSoup(response.text, 'html.parser')
    except requests.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
        return None

def scrape_yourstory(ticker, max_articles):
    """Scrapes articles from YourStory for a given ticker."""
    logging.info(f"Scraping YourStory for '{ticker}'...")
    search_url = SOURCES['yourstory'].format(ticker.replace(' ', '+'))
    soup = get_soup(search_url)
    if not soup:
        return []

    articles = []
    # Assumption: Article links are in `h3` tags with class `title`. This may change.
    for item in soup.find_all('h3', {'class': 'title'}, limit=max_articles):
        link_tag = item.find('a')
        if link_tag and link_tag.has_attr('href'):
            article_url = "https://yourstory.com" + link_tag['href']
            article_soup = get_soup(article_url)
            if article_soup:
                # Assumption: Article body is within a div with class `article-body`.
                body = article_soup.find('div', class_='article-body')
                if body:
                    title = article_soup.title.string.strip() if article_soup.title else "No Title"
                    text = ' '.join(p.get_text() for p in body.find_all('p'))
                    articles.append({'title': title, 'text': text, 'url': article_url})
    logging.info(f"Found {len(articles)} articles from YourStory for '{ticker}'.")
    return articles

def scrape_finshots(ticker, max_articles):
    """Scrapes articles from Finshots for a given ticker."""
    logging.info(f"Scraping Finshots for '{ticker}'...")
    soup = get_soup(SOURCES['finshots'])
    if not soup:
        return []

    articles = []
    # Assumption: We scan post links on the homepage and filter by ticker in title/URL.
    for item in soup.find_all('a', {'class': 'post-card-image-link'}, limit=50): # Look at more links
        if len(articles) >= max_articles:
            break
        
        title_tag = item.find_next('h2', {'class': 'post-card-title'})
        if title_tag and ticker.lower() in title_tag.get_text().lower():
            article_url = item['href']
            article_soup = get_soup(article_url)
            if article_soup:
                # Assumption: Article body is in class `post-content`.
                body = article_soup.find('div', class_='post-content')
                if body:
                    title = article_soup.title.string.strip() if article_soup.title else "No Title"
                    text = ' '.join(p.get_text() for p in body.find_all('p'))
                    articles.append({'title': title, 'text': text, 'url': article_url})
    logging.info(f"Found {len(articles)} articles from Finshots for '{ticker}'.")
    return articles

def clean_and_process_data(articles_df: pd.DataFrame) -> pd.DataFrame:
    """Basic cleaning and deduplication."""
    if articles_df.empty:
        return articles_df
    
    # Clean text
    articles_df['text'] = articles_df['text'].str.replace(r'\s+', ' ', regex=True).str.strip()
    articles_df['title'] = articles_df['title'].str.strip()
    
    # Deduplicate based on a hash of the title and the first 200 chars of text
    articles_df['content_hash'] = articles_df.apply(lambda row: sha256((row['title'] + row['text'][:200]).encode()).hexdigest(), axis=1)
    articles_df.drop_duplicates(subset=['content_hash'], keep='first', inplace=True)
    
    return articles_df.drop(columns=['content_hash'])

def persist_data(df: pd.DataFrame, ticker: str):
    """Persists the final DataFrame to SQLite."""
    if df.empty:
        logging.info(f"No new data to persist for {ticker}.")
        return

    logging.info(f"Persisting {len(df)} articles for {ticker} to database at {DB_PATH}.")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            # Create table if it doesn't exist
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS sentiment_scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                title TEXT NOT NULL UNIQUE,
                article_text TEXT,
                source_url TEXT,
                sentiment_score REAL,
                processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            # Insert data, ignoring conflicts on the unique title
            df.to_sql('sentiment_scores', conn, if_exists='append', index=False,
                      # A bit of a trick for pandas to_sql to handle conflicts
                      method=lambda table, conn, keys, data_iter: 
                        pd.io.sql.execute('INSERT OR IGNORE INTO {} ({}) VALUES ({})'.format(
                            table.name,
                            ', '.join(keys),
                            ', '.join(['?'] * len(keys))
                        ), conn, data_iter)
            )
        logging.info(f"Successfully persisted data for {ticker}.")
    except sqlite3.Error as e:
        logging.error(f"Database error for {ticker}: {e}")

def run_news_pipeline():
    """Main function to run the entire news pipeline."""
    logging.info("Starting News Sentiment Pipeline...")
    all_articles = []

    for ticker in TICKERS:
        logging.info(f"--- Processing Ticker: {ticker} ---")
        yourstory_articles = scrape_yourstory(ticker, MAX_ARTICLES)
        finshots_articles = scrape_finshots(ticker, MAX_ARTICLES)
        
        ticker_articles = yourstory_articles + finshots_articles
        if not ticker_articles:
            logging.warning(f"No articles found for ticker: {ticker}")
            continue

        df = pd.DataFrame(ticker_articles)
        df['ticker'] = ticker
        
        logging.info(f"Cleaning data for {ticker}...")
        df_clean = clean_and_process_data(df)

        logging.info(f"Generating sentiment scores for {ticker}...")
        df_clean['sentiment_score'] = df_clean['text'].apply(mock_sentiment_api)

        final_df = df_clean[['ticker', 'title', 'text', 'url', 'sentiment_score']].rename(
            columns={'text': 'article_text', 'url': 'source_url'}
        )
        
        persist_data(final_df, ticker)

    logging.info("News Sentiment Pipeline finished successfully.")
