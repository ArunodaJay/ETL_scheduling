import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import random
import psycopg2
from psycopg2 import sql, extras
import logging
from dotenv import load_dotenv

# --- Load Environment Variables from .env file ---
load_dotenv()

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Config ---
BREVO_API_KEY = os.environ.get("BREVO_API_KEY")
BREVO_BASE_URL = "https://api.brevo.com/v3/smtp/statistics/events"
INITIAL_START_DATE = "2025-11-20" 

# --- Supabase / Postgres Config ---
SUPABASE_HOST = os.environ.get("SUPABASE_HOST")
SUPABASE_DB = os.environ.get("SUPABASE_DB")
SUPABASE_PORT = os.environ.get("SUPABASE_PORT", "5432")
SUPABASE_USER = os.environ.get("SUPABASE_USER")
SUPABASE_PASSWORD = os.environ.get("SUPABASE_PASSWORD")

TABLE_NAME = "brevo_email_stats"

# --- Validation ---
if not BREVO_API_KEY or not SUPABASE_HOST or not SUPABASE_PASSWORD:
    logger.error("Missing required environment variables. Please check your .env file.")
    exit(1)

# --- Helper Functions ---

def get_db_connection():
    """Establishes a connection to the Supabase PostgreSQL database."""
    try:
        # Standard connection - Works perfectly with the Pooler URL (Port 6543)
        conn = psycopg2.connect(
            host=SUPABASE_HOST,
            database=SUPABASE_DB,
            user=SUPABASE_USER,
            password=SUPABASE_PASSWORD,
            port=SUPABASE_PORT
        )
        return conn
    except Exception as e:
        logger.error(f"Could not connect to Supabase: {e}")
        raise

def fetch_brevo_data(day_str):
    """Fetch Brevo events with Exponential Backoff."""
    try:
        url = f"{BREVO_BASE_URL}?startDate={day_str}&endDate={day_str}&limit=500&offset=0"
        headers = {"accept": "application/json", "api-key": BREVO_API_KEY}
        all_events = []
        offset = 0
        
        max_pages = 1000 
        page_count = 0

        while page_count < max_pages:
            paginated_url = f"{url.split('&offset=')[0]}&offset={offset}"
            
            # Retry Logic Variables
            attempts = 0
            max_attempts = 5
            wait_time = 60 # Start with 60 seconds

            success = False
            while attempts < max_attempts:
                try:
                    r = requests.get(paginated_url, headers=headers, timeout=60)
                    
                    # 1. HANDLE RATE LIMITS (429)
                    if r.status_code == 429:
                        attempts += 1
                        logger.warning(f"Rate limit hit (429). Attempt {attempts}/{max_attempts}.")
                        
                        # Try to read the error message from Brevo
                        try:
                            logger.info(f"Brevo Message: {r.text}")
                        except:
                            pass

                        # Double the wait time for next retry (Exponential Backoff)
                        logger.info(f"Sleeping for {wait_time}s before retrying...")
                        time.sleep(wait_time)
                        wait_time *= 2 # 60 -> 120 -> 240 -> 480...
                        continue
                    
                    # 2. HANDLE OTHER ERRORS
                    if r.status_code != 200:
                        logger.error(f"API Error {r.status_code}: {r.text}")
                        return all_events # Stop and return what we have so far
                    
                    # If success
                    success = True
                    break

                except requests.exceptions.RequestException as e:
                    logger.warning(f"Network error: {e}. Retrying...")
                    time.sleep(10)
                    attempts += 1

            if not success:
                logger.error(f"Max retries reached for {day_str}. Skipping to next day.")
                break

            data = r.json()
            events = data.get("events", [])
            
            if not events:
                break 
                
            all_events.extend(events)
            
            if len(events) < 500:
                break 
                
            offset += 500
            page_count += 1
            time.sleep(1) # Increase polite delay slightly
            
        return all_events
        
    except Exception as e:
        logger.error(f"Failed to fetch Brevo data for {day_str}: {e}")
        return []

def get_metrics_df(day_str):
    """Transform Brevo events to a daily metrics DataFrame."""
    events = fetch_brevo_data(day_str)
    
    default_data = {
        "date": [pd.to_datetime(day_str).date()], 
        "sent": [0], 
        "delivered": [0], 
        "opened": [0], 
        "clicks": [0]
    }

    if not events:
        logger.warning(f"No events found for {day_str}, returning zero-metrics.")
        return pd.DataFrame(default_data)
    
    df = pd.DataFrame(events)

    if 'date' not in df.columns or 'event' not in df.columns:
        logger.error(f"Response for {day_str} is missing required columns.")
        return pd.DataFrame(default_data)

    df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
    grouped = df.groupby(['date', 'event']).size().reset_index(name='count')

    df_metrics = grouped.pivot(index='date', columns='event', values='count').fillna(0).reset_index()

    event_cols = ["delivered", "hardBounce", "softBounce", "blocked", "invalid", "opened", "clicks"]
    for col in event_cols:
        if col not in df_metrics.columns:
            df_metrics[col] = 0

    df_metrics["sent"] = (
        df_metrics["delivered"] + df_metrics["hardBounce"] + 
        df_metrics["softBounce"] + df_metrics["blocked"] + 
        df_metrics["invalid"]
    )

    df_metrics = df_metrics.rename(columns={
        "delivered": "delivered",
        "opened": "opened",
        "clicks": "clicks"
    })
    
    final_cols = ["date", "sent", "delivered", "opened", "clicks"]
    df_metrics = df_metrics[final_cols]
    
    for col in ["sent", "delivered", "opened", "clicks"]:
        df_metrics[col] = df_metrics[col].astype(int)
        
    return df_metrics

def ensure_table_exists(conn):
    """Creates the table in Supabase if it doesn't exist."""
    cur = conn.cursor()
    try:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                date DATE PRIMARY KEY,
                sent INTEGER DEFAULT 0,
                delivered INTEGER DEFAULT 0,
                opened INTEGER DEFAULT 0,
                clicks INTEGER DEFAULT 0,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """)
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Error ensuring table exists: {e}")
        raise
    finally:
        cur.close()

def get_last_loaded_date(conn):
    """Return max DATE in the target table or None."""
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT to_regclass('public.{TABLE_NAME}');")
        if cur.fetchone()[0] is None:
            return None
        
        cur.execute(f"SELECT MAX(date) FROM {TABLE_NAME}")
        result = cur.fetchone()[0]
        if result:
            logger.info(f"Last loaded date found: {result}")
            return result
        return None
    except Exception as e:
        logger.error(f"Error getting last date: {e}")
        return None
    finally:
        cur.close()

def load_data_to_supabase(conn, df):
    """Upsert data into Supabase using ON CONFLICT."""
    if df.empty:
        return
        
    cur = conn.cursor()
    try:
        data_tuples = [tuple(x) for x in df.to_numpy()]
        
        query = f"""
            INSERT INTO {TABLE_NAME} (date, sent, delivered, opened, clicks)
            VALUES %s
            ON CONFLICT (date) DO UPDATE SET
                sent = EXCLUDED.sent,
                delivered = EXCLUDED.delivered,
                opened = EXCLUDED.opened,
                clicks = EXCLUDED.clicks,
                updated_at = NOW();
        """
        
        extras.execute_values(cur, query, data_tuples)
        conn.commit()
        logger.info(f"Successfully upserted {len(df)} rows to {TABLE_NAME}.")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to load data to Supabase: {e}")
        raise
    finally:
        cur.close()

# --- Main ETL ---
def main():
    logger.info("Starting Brevo to Supabase ETL.")
    conn = None 
    try:
        conn = get_db_connection()
        ensure_table_exists(conn)
        
        last_date = get_last_loaded_date(conn)
        
        if last_date is None:
            # Parse your INITIAL_START_DATE string to a date object
            start_date = datetime.strptime(INITIAL_START_DATE, "%Y-%m-%d").date()
            logger.info(f"Table empty. Starting from scratch: {start_date}")
        else:
            start_date = last_date + timedelta(days=1)
            
        end_date = datetime.now().date() - timedelta(days=1)

        if start_date > end_date:
            logger.info("Data is already up to date. Exiting.")
            return

        logger.info(f"Fetching data from {start_date} to {end_date}.")
        
        current = start_date
        while current <= end_date:
            day_str = current.strftime("%Y-%m-%d")
            logger.info(f"Processing {day_str}...")
            
            # 1. Get Data for ONE day
            df_day = get_metrics_df(day_str)
            
            # 2. Save IMMEDIATELY to Supabase
            # This ensures that if the script crashes on day 5, days 1-4 are already saved.
            if not df_day.empty:
                load_data_to_supabase(conn, df_day)
            
            logger.info("Taking a 10s break to respect API limits...")
            time.sleep(10)
            # Move to next day
            current += timedelta(days=1)
            
    except Exception as e:
        logger.critical(f"Critical error: {e}", exc_info=True)
        
    finally:
        if conn:
            conn.close()
            logger.info("Supabase connection closed.")

if __name__ == "__main__":
    main()