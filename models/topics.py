from database import get_db_connection

def get_main_topics(category_id):
    """
    Mengambil main topics berdasarkan ID kategori (contoh: Technology, Economy, Politics).
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

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
        mt.topics_name  -- Hanya mengelompokkan berdasarkan nama topik
    ORDER BY 
        frequency DESC;
    """
    
    cursor.execute(query, (category_id,))
    topics = cursor.fetchall()
    conn.close()
    return topics
