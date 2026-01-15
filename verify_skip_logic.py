
import os
import main
import psycopg2
from main import setup_database, get_db_connection, save_application, save_document_record, save_condition_record

# Force DB URL if not set
if not os.getenv("DATABASE_URL"):
    import dotenv
    dotenv.load_dotenv()

# Mock hydrate_application to just print and not fail/call API
def mock_hydrate(app_id, lpa="dunlaoghaire"):
    print(f"MOCK_HYDRATE_CALLED: {app_id}")

main.hydrate_application = mock_hydrate

def verify_logic():
    print("--- 1. Setup Test Data ---")
    setup_database()
    conn = get_db_connection()
    c = conn.cursor()
    test_lpa = "test_skip_logic"
    
    # Clear old test data
    c.execute("DELETE FROM documents WHERE lpa = %s", (test_lpa,))
    c.execute("DELETE FROM conditions WHERE lpa = %s", (test_lpa,))
    c.execute("DELETE FROM applications WHERE lpa = %s", (test_lpa,))
    conn.commit()
    
    # App 1: Hydrated via timestamp
    save_application({"id": 101, "reference": "APP1", "registrationDate": "2025-01-01"}, lpa=test_lpa)
    c.execute("UPDATE applications SET last_hydrated_at = NOW() WHERE id = 101 AND lpa = %s", (test_lpa,))
    
    # App 2: Hydrated via Document (Timestamp NULL)
    save_application({"id": 102, "reference": "APP2", "registrationDate": "2025-01-01"}, lpa=test_lpa)
    # Add doc
    c.execute("INSERT INTO documents (app_id, lpa, filename) VALUES (102, %s, 'test.pdf')", (test_lpa,))
    
    # App 3: Hydrated via Condition (Timestamp NULL)
    save_application({"id": 103, "reference": "APP3", "registrationDate": "2025-01-01"}, lpa=test_lpa)
    # Add condition
    c.execute("INSERT INTO conditions (app_id, lpa, order_num) VALUES (103, %s, 1)", (test_lpa,))
    
    # App 4: Not Hydrated (Timestamp NULL, No Docs, No Conds)
    save_application({"id": 104, "reference": "APP4", "registrationDate": "2025-01-01"}, lpa=test_lpa)
    
    conn.commit()
    conn.close()
    
    print("\n--- 2. Running hydrate_all_applications(skip_hydrated=True) ---")
    # This should ONLY call mock_hydrate for 104
    main.hydrate_all_applications(skip_hydrated=True, lpa_filter=test_lpa)
    
    print("\n--- 3. Running hydrate_all_applications(skip_hydrated=False) ---")
    # This should call for ALL 4 (or at least 101, 102, 103 if 104 was 'processed' but we mocked it so it didn't update timestamp)
    # Since mock doesn't update timestamp, 104 is still unhydrated. 
    # But skip_hydrated=False means run EVERYTHING.
    main.hydrate_all_applications(skip_hydrated=False, lpa_filter=test_lpa)

if __name__ == "__main__":
    verify_logic()
