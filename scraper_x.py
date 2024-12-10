import subprocess
import pandas as pd
import os
from sqlalchemy import create_engine
import traceback
from datetime import datetime, timedelta

def build_search_keyword(category_id):
    """Bangun search keyword berdasarkan kategori dan tanggal"""
    category_map = {
        1: "teknologi",
        2: "ekonomi",
        3: "politik"
    }
    
    # Pastikan kategori valid
    if category_id not in category_map:
        raise ValueError("Invalid category_id. Valid values are 1 (Technology), 2 (Economy), or 3 (Politics).")
    
    # Dapatkan keyword berdasarkan kategori
    keyword = category_map[category_id]

    # Hitung rentang tanggal
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)

    # Format tanggal untuk query
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    # Bangun search keyword
    search_keyword = f"{keyword} since:{start_date_str} until:{end_date_str} lang:id"
    return search_keyword

def scrape_twitter_data(search_keyword, filepath, limit, auth_token):
    try:
        csv_directory = os.path.join(os.getcwd(), "tweets-data")
        filepath = os.path.join(csv_directory, filepath)

        if not os.path.exists(csv_directory):
            os.makedirs(csv_directory)

        command = [
                    "npx", "tweet-harvest@2.6.1",
                    "-o", filepath,
                    "-s", search_keyword,
                    "--tab", "LATEST",
                    "-l", str(limit),
                    "--token", auth_token
                ]
        print(f"Running command: {command}")  
        subprocess.run(command, check=True, shell=True)

        print(f"Checking for file: {filepath}")
        if not os.path.exists(filepath):
            raise RuntimeError(f"File {filepath} was not created.")
        
        print(f"File {filepath} created successfully.")
        return filepath
    except Exception as e:
        traceback.print_exc()
        raise RuntimeError(f"Error during scraping: {str(e)}")


def save_to_database(df, db_config, category_id):
    """
    Save scraped data to the database with a specific category ID.
    
    Args:
        df (pd.DataFrame): DataFrame containing scraped data.
        db_config (dict): Database configuration dictionary.
        category_id (int): ID of the category to associate with the data.
    """
    try:
        db_url = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"

        engine = create_engine(db_url)

        required_columns = [
            "full_text", "favorite_count", "quote_count", "reply_count",
            "retweet_count", "username", "location", "created_at"
        ]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        df_selected = df[required_columns].copy()

        df_selected["idx_datasets"] = None  
        df_selected["main_categories_idmain_categories"] = category_id 
        df_selected["created_at"] = pd.to_datetime(df_selected["created_at"], format="%a %b %d %H:%M:%S %z %Y").dt.strftime("%Y-%m-%d %H:%M:%S")

        df_selected.to_sql("x_datasets", con=engine, if_exists="append", index=False)
        return {"message": "Data saved successfully"}
    except Exception as e:
        traceback.print_exc()
        raise RuntimeError(f"Error saving data to database: {str(e)}")

