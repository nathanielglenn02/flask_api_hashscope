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
        return jsonify({'message': 'Login successful', 'token': token}), 200
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


if __name__ == '__main__':
    app.run(debug=True)
