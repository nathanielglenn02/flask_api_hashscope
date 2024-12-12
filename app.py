from flask_bcrypt import Bcrypt
from flask import Flask, jsonify, request
from models.users import register_user, login_user
from models.categories import get_all_categories
from models.topics import get_main_topics
from models.platforms import get_platform_data
import secrets
import string
from werkzeug.security import check_password_hash
from scraper_x import scrape_twitter_data, save_to_database, build_search_keyword
from database import DB_CONFIG
import os
import pandas as pd
from scraper_web import scrape_google_news
from datetime import datetime
import traceback
from sqlalchemy import create_engine
from scraper_youtube import scrape_youtube_comments
from topics_extraction import extract_topics
from ai_topic_prediction import predict_future_topics_api


app = Flask(__name__)
bcrypt = Bcrypt(app)

TWITTER_AUTH_TOKEN = "452993740006163e4a0ad979f52880f01d094556"


# Endpoint 1: Register User
@app.route('/api/register', methods=['POST'])
def register():
    data = request.json
    hashed_password = bcrypt.generate_password_hash(data['password']).decode('utf-8')  # Hash password
    register_user(data['name'], data['email'], hashed_password)
    return jsonify({'message': 'User registered successfully'}), 201

# Endpoint 2: Login User
@app.route('/api/login', methods=['POST'])
def login():
    data = request.json
    email = data.get('email')
    password = data.get('password')

    if not email or not password:
        return jsonify({'message': 'Email and password are required'}), 400

    user = login_user(email, password) 
    if user:
        token = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(12))
        return jsonify({
            'message': 'Login successful',
            'token': token,
            'user': {
                'name': user['name'],
                'email': user['email']
            }
        }), 200
    else:
        return jsonify({'message': 'Invalid credentials'}), 401



# Endpoint 3: Get All Categories
@app.route('/api/categories', methods=['GET'])
def categories():
    categories = get_all_categories()
    return jsonify(categories)

# Endpoint 4: Get Main Topics by Category
@app.route('/api/main_topics/<int:category_id>', methods=['GET'])
def main_topics_by_category(category_id):
    """
    Endpoint untuk mendapatkan main topics berdasarkan ID kategori dengan paginasi.
    """
    try:
        # Ambil parameter 'page' dan 'limit' dari query string
        page = request.args.get('page', 1, type=int)  # Default halaman pertama
        limit = request.args.get('limit', 10, type=int)  # Default 10 item per halaman

        # Ambil data main topics dengan paging
        topics = get_main_topics(category_id, page, limit)
        
        if not topics:
            return jsonify({"message": "No topics found for this category"}), 404

        return jsonify(topics)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Endpoint 5: Get Platform Data
@app.route('/api/platform_data', methods=['GET'])
def platform_data():
    """
    Endpoint untuk mendapatkan data platform berdasarkan kategori, topik utama, dan rentang tanggal (opsional).
    """
    try:
        platform = request.args.get('platform')
        category_id = request.args.get('category_id', type=int)
        main_topic_id = request.args.get('main_topic_id', type=int)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')

        if not platform or not category_id or not main_topic_id:
            return jsonify({"error": "platform, category_id, and main_topic_id are required"}), 400

        data = get_platform_data(platform, category_id, main_topic_id, start_date, end_date)
        if data is None:
            return jsonify({"error": "Invalid platform"}), 400

        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint 6 : Scrape Twitter Data
@app.route('/api/scrape', methods=['POST'])
def scrape_data():
    try:
        data = request.json
        category_id = data.get('category_id')
        filename = data.get('filename', 'x-technology.csv')
        limit = data.get('limit', 100)

        if not category_id:
            return jsonify({'error': 'category_id is required'}), 400

        search_keyword = build_search_keyword(category_id)
        
        filepath = scrape_twitter_data(search_keyword, filename, limit, TWITTER_AUTH_TOKEN)

        df = pd.read_csv(filepath)

        if df is None:
            print("DataFrame is None after scraping.")
            return jsonify({'error': 'No data scraped.'}), 400

        print("Scraped data ready for saving:")
        print(df.head())
        
        save_to_database(df, DB_CONFIG, category_id)

        return jsonify({'message': 'Scraping and saving data completed successfully.'}), 200
    except ValueError as ve:
        return jsonify({'error': str(ve)}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# Endpoint 7: Scrape Google News
@app.route('/api/scrape_news', methods=['POST'])
def scrape_news():
    try:
        data = request.json
        category = data.get('category', 'technology') 
        filename = data.get('filename', f"{category}_news.csv")
        max_results = data.get('max_results', 100)

        if not category:
            return jsonify({'error': 'category is required'}), 400

        csv_file_path = scrape_google_news(category, max_results, filename)

        df = pd.read_csv(csv_file_path)

        df_selected = df[['full_text', 'source', 'created_at', 'url']].copy()
        df_selected["idweb_datasets"] = None
        df_selected["main_categories_idmain_categories"] = data.get('category_id', 1)  # ID kategori dari user
        df_selected["created_at"] = pd.to_datetime(df_selected["created_at"], format="%a, %d %b %Y %H:%M:%S %Z").dt.strftime("%Y-%m-%d %H:%M:%S")
        print("Saving to database...")

        db_connection_url = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
        engine = create_engine(db_connection_url)

        df_selected.to_sql("web_datasets", con=engine, if_exists="append", index=False)

        return jsonify({'message': 'Scraping and saving data completed successfully'}), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

# Endpoint 8: Scrape YouTube Comments
@app.route('/api/scrape_youtube', methods=['POST'])
def scrape_youtube():
    try:
        data = request.json
        search_keyword = data.get('search_keyword', 'politics')
        max_results = data.get('max_results', 50)
        max_comments = data.get('max_comments', 1500)

        scrape_youtube_comments(search_keyword, max_results, max_comments, DB_CONFIG)

        return jsonify({'message': 'Scraping and saving YouTube comments completed successfully'}), 200
    except ValueError as ve:
        return jsonify({'error': str(ve)}), 400
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

# Endpoint 9: Topics Extraction
@app.route('/api/extract_topics', methods=['POST'])
def extract_topics_route():
    return extract_topics()

# Endpoint 10: Predict Extraction
@app.route('/api/predict_topics', methods=['GET'])
def predict_topics():
    return predict_future_topics_api()


if __name__ == '__main__':
    app.run(debug=True)
