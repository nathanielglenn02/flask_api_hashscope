from database import get_db_connection

def get_platform_data(platform, category_id, main_topic_id, start_date=None, end_date=None):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    table_map = {
        'X': 'x_datasets',
        'YouTube': 'youtube_datasets',
        'Web': 'web_datasets',
    }
    table = table_map.get(platform)
    if not table:
        return None

    query = f"SELECT * FROM {table} WHERE main_categories_idmain_categories = %s AND main_topics_idmain_topics = %s"
    params = [category_id, main_topic_id]

    if start_date and end_date:
        query += " AND created_at BETWEEN %s AND %s"
        params.extend([start_date, end_date])

    cursor.execute(query, params)
    data = cursor.fetchall()
    conn.close()
    return data
