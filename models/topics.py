from database import get_db_connection

def get_main_topics(category_id):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM main_topics WHERE main_categories_idmain_categories = %s ORDER BY frequency DESC", (category_id,))
    topics = cursor.fetchall()
    conn.close()
    return topics
