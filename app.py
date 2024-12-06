from flask import Flask, jsonify, request
from models.users import register_user, login_user
from models.categories import get_all_categories
from models.topics import get_main_topics
from models.platforms import get_platform_data
import secrets
import string

app = Flask(__name__)

# Endpoint 1: Register User
@app.route('/api/register', methods=['POST'])
def register():
    data = request.json
    register_user(data['name'], data['email'], data['password'])
    return jsonify({'message': 'User registered successfully'}), 201

# Endpoint 2: Login User
@app.route('/api/login', methods=['POST'])
def login():
    data = request.json
    user = login_user(data['email'], data['password'])
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
    Endpoint untuk mendapatkan main topics berdasarkan ID kategori.
    """
    try:
        topics = get_main_topics(category_id)
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



if __name__ == '__main__':
    app.run(debug=True)
