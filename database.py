import mysql.connector

# Konfigurasi koneksi database
DB_CONFIG = {
    'user': 'admin_user',
    'password': '123456',
    'host': '34.50.67.209',
    'database': 'db_hashscope',
    'auth_plugin': 'mysql_native_password'
}

def get_db_connection():
    """Buka koneksi ke database"""
    return mysql.connector.connect(**DB_CONFIG)
