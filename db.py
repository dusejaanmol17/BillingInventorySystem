import psycopg2

def get_connection():
    conn = psycopg2.connect(
        host="localhost",
        database="MusicWorldBillingDB",
        user="postgres",
        password="Anmol@2406"
    )
    return conn
