from sqlalchemy import create_engine
import pandas as pd
from rake_nltk import Rake
import traceback
from database import DB_CONFIG, get_db_connection

engine = create_engine(f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}")

r = Rake(punctuations=['.', ',', ';', ':', '"', '!', '?', ']', '[', '(', ')', '...', '-'], max_length=7)

def preprocess_text(text):
    """Preprocess text by removing mentions, links, and extra spaces."""
    import re
    text = re.sub(r"@\w+", "", text)  
    text = re.sub(r"http\S+|www\.\S+", "", text) 
    text = re.sub(r"\s+", " ", text).strip()  
    return text

def extract_keywords_with_scores(text):
    """Extract keywords and their scores using Rake."""
    r.extract_keywords_from_text(text)
    return r.get_ranked_phrases_with_scores()

def fetch_data_from_all_sources():
    """Fetch data from all datasets (x_datasets, web_datasets, youtube_datasets)."""
    query_x = "SELECT idx_datasets AS id, full_text FROM x_datasets"
    query_web = "SELECT idweb_datasets AS id, full_text FROM web_datasets"
    query_youtube = "SELECT idyoutube_datasets AS id, full_text FROM youtube_datasets"

    df_x = pd.read_sql(query_x, engine)
    df_x["platform"] = "x_datasets"

    df_web = pd.read_sql(query_web, engine)
    df_web["platform"] = "web_datasets"

    df_youtube = pd.read_sql(query_youtube, engine)
    df_youtube["platform"] = "youtube_datasets"

    return pd.concat([df_x, df_web, df_youtube], ignore_index=True)

def insert_main_topics_to_db_with_platform_check(csv_data):
    """Insert main topics into the database with platform checks."""
    try:
        with engine.connect() as conn:
            existing_ids_query = """
                SELECT x_datasets_idx_datasets, web_datasets_idweb_datasets, youtube_datasets_idyoutube_datasets
                FROM main_topics
            """
            existing_ids = pd.read_sql(existing_ids_query, conn)

        existing_x_ids = existing_ids["x_datasets_idx_datasets"].dropna().tolist()
        existing_web_ids = existing_ids["web_datasets_idweb_datasets"].dropna().tolist()
        existing_youtube_ids = existing_ids["youtube_datasets_idyoutube_datasets"].dropna().tolist()

        new_data = csv_data[~(
            (csv_data["platform"] == "x_datasets") & (csv_data["id"].isin(existing_x_ids)) |
            (csv_data["platform"] == "web_datasets") & (csv_data["id"].isin(existing_web_ids)) |
            (csv_data["platform"] == "youtube_datasets") & (csv_data["id"].isin(existing_youtube_ids))
        )]

        if not new_data.empty:
            df_to_insert = []
            for _, row in new_data.iterrows():
                data = {
                    "topics_name": row["phrase"],
                    "frequency": row["frequency"],
                    "x_datasets_idx_datasets": None,
                    "web_datasets_idweb_datasets": None,
                    "youtube_datasets_idyoutube_datasets": None,
                }
                if row["platform"] == "x_datasets":
                    data["x_datasets_idx_datasets"] = row["id"]
                elif row["platform"] == "web_datasets":
                    data["web_datasets_idweb_datasets"] = row["id"]
                elif row["platform"] == "youtube_datasets":
                    data["youtube_datasets_idyoutube_datasets"] = row["id"]

                df_to_insert.append(data)

            pd.DataFrame(df_to_insert).to_sql("main_topics", con=engine, if_exists="append", index=False)
    except Exception as e:
        print(f"Error during database insert: {e}")
        traceback.print_exc()

def extract_topics():
    try:
        print("Fetching data from all sources...")
        data = fetch_data_from_all_sources()

        print("Preprocessing text...")
        data["preprocessed_text"] = data["full_text"].apply(preprocess_text)

        print("Extracting keywords with Rake...")
        data["rake_results"] = data["preprocessed_text"].apply(extract_keywords_with_scores)

        rake_results = []
        for _, row in data.iterrows():
            for score, phrase in row["rake_results"]:
                rake_results.append({
                    "id": row["id"],
                    "score": score,
                    "phrase": phrase,
                    "platform": row["platform"]
                })

        rake_df = pd.DataFrame(rake_results)

        if "frequency" not in rake_df.columns:
            rake_df["frequency"] = 1

        rake_df = rake_df.groupby(["phrase", "platform"]).agg({"id": "first", "frequency": "sum"}).reset_index()
        rake_df["global_id"] = range(1, len(rake_df) + 1)

        print("Inserting main topics into database...")
        insert_main_topics_to_db_with_platform_check(rake_df)

        return {"message": "Topic extraction and saving completed successfully."}
    except Exception as e:
        traceback.print_exc()
        return {"error": str(e)}
