
import os
import psycopg2
from main import get_db_connection

# Force DB URL if not set
if not os.getenv("DATABASE_URL"):
    import dotenv
    dotenv.load_dotenv()

def check_data():
    conn = get_db_connection()
    c = conn.cursor()
    
    print("--- Checking registration_date format ---")
    c.execute("SELECT registration_date FROM applications WHERE registration_date IS NOT NULL LIMIT 20")
    rows = c.fetchall()
    for r in rows:
        print(f"Date: {r[0]}")
        
    # Check if any legitimate dates might fail cast
    # Regex for YYYY-MM-DD
    c.execute("SELECT count(*) FROM applications WHERE registration_date !~ '^\d{4}-\d{2}-\d{2}' AND registration_date IS NOT NULL")
    bad_dates = c.fetchone()[0]
    print(f"Non-standard dates found: {bad_dates}")
    
    print("\n--- Checking raw_json format ---")
    # Just check if it looks like json
    c.execute("SELECT raw_json FROM applications LIMIT 1")
    row = c.fetchone()
    if row:
        print(f"JSON Sample (start): {row[0][:50]}...")
        
    conn.close()

if __name__ == "__main__":
    check_data()
