from database import get_db_connection

def get_main_topics(category_id, page, limit):
    """
    Mengambil main topics berdasarkan ID kategori dengan pagination.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    offset = (page - 1) * limit

    query = """
    SELECT 
        MAX(mt.idmain_topics) AS topic_id,  -- Mengambil salah satu ID topik
        mt.topics_name, 
        COUNT(mt.topics_name) AS frequency
    FROM 
        main_topics mt
    LEFT JOIN 
        x_datasets xd ON mt.x_datasets_idx_datasets = xd.idx_datasets
    LEFT JOIN 
        web_datasets wd ON mt.web_datasets_idweb_datasets = wd.idweb_datasets
    LEFT JOIN 
        youtube_datasets yd ON mt.youtube_datasets_idyoutube_datasets = yd.idyoutube_datasets
    WHERE 
        xd.main_categories_idmain_categories = %s
        OR wd.main_categories_idmain_categories = %s
        OR yd.main_categories_idmain_categories = %s
    GROUP BY 
        mt.topics_name
    ORDER BY 
        frequency DESC
    LIMIT %s OFFSET %s;
    """
    
    cursor.execute(query, (category_id, category_id, category_id, limit, offset))
    topics = cursor.fetchall()
    conn.close()
    return topics
