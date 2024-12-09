from database import get_db_connection

def get_main_topics(category_id, page, limit):
    """
    Mengambil main topics berdasarkan ID kategori dengan pagination.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    # Menghitung offset berdasarkan halaman dan limit
    offset = (page - 1) * limit

    query = """
    SELECT 
        MAX(mt.idmain_topics) AS topic_id,  -- Mengambil salah satu ID topik
        mt.topics_name, 
        COUNT(mt.topics_name) AS frequency
    FROM 
        main_topics mt
    JOIN 
        x_datasets xd ON mt.x_datasets_idx_datasets = xd.idx_datasets
    WHERE 
        xd.main_categories_idmain_categories = %s
    GROUP BY 
        mt.topics_name
    ORDER BY 
        frequency DESC
    LIMIT %s OFFSET %s;
    """
    
    # Eksekusi query dengan parameter ID kategori, limit, dan offset
    cursor.execute(query, (category_id, limit, offset))
    topics = cursor.fetchall()
    conn.close()
    return topics
