from database import get_db_connection

def get_platform_data(platform, category_id, main_topic_id, start_date=None, end_date=None):
    """
    Mendapatkan data platform (X, Web, YouTube) berdasarkan kategori, topik utama, dan rentang tanggal (opsional).
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    table_map = {
        'X': {
            'table': 'x_datasets',
            'join_column': 'x_datasets_idx_datasets',
            'id_column': 'idx_datasets'
        },
        'Web': {
            'table': 'web_datasets',
            'join_column': 'web_datasets_idweb_datasets',
            'id_column': 'idweb_datasets'
        },
        'YouTube': {
            'table': 'youtube_datasets',
            'join_column': 'youtube_datasets_idyoutube_datasets',
            'id_column': 'idyoutube_datasets'
        }
    }

    platform_data = table_map.get(platform)
    if not platform_data:
        return None

    table = platform_data['table']
    join_column = platform_data['join_column']
    id_column = platform_data['id_column']

    query = f"""
    SELECT d.* 
    FROM {table} d
    JOIN main_topics mt ON mt.{join_column} = d.{id_column}
    WHERE d.main_categories_idmain_categories = %s 
      AND mt.idmain_topics = %s
    """
    params = [category_id, main_topic_id]

    if start_date and end_date:
        query += " AND d.created_at BETWEEN %s AND %s"
        params.extend([start_date, end_date])

    cursor.execute(query, params)
    data = cursor.fetchall()
    conn.close()
    return data
