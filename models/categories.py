from database import get_db_connection

def get_all_categories():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM main_categories")
    categories = cursor.fetchall()
    conn.close()
    return categories
