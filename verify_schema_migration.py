
import os
import psycopg2
from main import setup_database, get_db_connection

# Force DB URL if not set
if not os.getenv("DATABASE_URL"):
    import dotenv
    dotenv.load_dotenv()

def verify_migration():
    print("--- 1. Triggering Migration via setup_database() ---")
    setup_database()
    
    conn = get_db_connection()
    c = conn.cursor()
    
    print("\n--- 2. Checking Column Types ---")
    c.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'applications' 
        AND column_name IN ('registration_date', 'raw_json')
    """)
    rows = c.fetchall()
    
    expected = {
        'registration_date': 'date',
        'raw_json': 'jsonb'
    }
    
    for col, dtype in rows:
        print(f"Column: {col}, Type: {dtype}")
        if expected.get(col) == dtype:
            print(f"PASS: {col} is {dtype}")
        else:
            print(f"FAIL: {col} expected {expected.get(col)}, got {dtype}")
            
    print("\n--- 3. Verifying Data Access ---")
    # Verify we can read a row
    try:
        c.execute("SELECT registration_date, raw_json FROM applications LIMIT 1")
        row = c.fetchone()
        if row:
            print(f"Read success. Date: {row[0]}, JSON Type: {type(row[1])}")
        else:
            print("Table empty (but read success)")
    except Exception as e:
        print(f"FAIL: Error reading data: {e}")

    conn.close()

if __name__ == "__main__":
    verify_migration()
