from database import get_db_connection

def get_main_topics(category_id):
    """
    Mengambil main topics berdasarkan ID kategori (contoh: Technology, Economy, Politics).
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    query = """
    SELECT 
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
        frequency DESC;
    """
    
    # Eksekusi query dengan parameter ID kategori
    cursor.execute(query, (category_id,))
    topics = cursor.fetchall()
    conn.close()
    return topics
