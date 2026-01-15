import sqlite3
from collections import defaultdict

DB_PATH = "applications.db"

def verify_stats():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT decision, COUNT(*) FROM applications GROUP BY decision")
    rows = c.fetchall()
    conn.close()
    
    # Classification Logic
    # We want to isolate "Planning Permission" applications from "Admin" tasks.
    
    substantive_keywords = [
        'GRANT PERMISSION', 'REFUSE PERMISSION', 'GRANT RETENTION', 'REFUSE RETENTION',
        'GRANT OUTLINE', 'REFUSE OUTLINE', 'SPLIT DECISION', 
        'DECLARE APPLICATION INVALID', 'INVALID APPLICATION', 'DECLARE INVALID',
        'WITHDRAWN', 'WITHDRAW APPLICATION'
    ]
    
    admin_keywords = [
        'COMPLIANCE', 'CERTIFICATE OF EXEMPTION', 'EXTENSION OF DURATION', 
        'SECTION 254', 'S5', 'Exempted Development', 'FIRE CERT', 'Section 96'
    ]
    
    total_substantive = 0
    total_invalid = 0
    total_admin = 0
    
    print(f"{'Decision Category':<50} | {'Count':<6} | {'Type'}")
    print("-" * 75)
    
    for decision, count in rows:
        if not decision: 
            continue
            
        d_upper = decision.upper()
        
        # Determine Type
        is_substantive = any(k in d_upper for k in substantive_keywords)
        is_admin = any(k in d_upper for k in admin_keywords)
        
        # Override: Some 'Invalid' decisions might be site notice specific but are substantive
        if 'INVALID' in d_upper:
            is_substantive = True
            is_admin = False
            
        # Override: Compliance/Exemption is definitely Admin
        if is_admin:
            is_substantive = False
            
        type_label = "OTHER"
        if is_substantive:
            type_label = "SUBSTANTIVE"
            total_substantive += count
            if 'INVALID' in d_upper:
                total_invalid += count
        elif is_admin:
            type_label = "ADMIN"
            total_admin += count
            
        print(f"{decision[:50]:<50} | {count:<6} | {type_label}")

    print("-" * 75)
    print(f"\n--- Final Verification ---")
    print(f"Total Applications in DB: {sum(r[1] for r in rows)}")
    print(f"Administrative Records (Excluded): {total_admin}")
    print(f"Substantive Planning Applications: {total_substantive}")
    print(f"Total Declared Invalid: {total_invalid}")
    
    if total_substantive > 0:
        rate = (total_invalid / total_substantive) * 100
        print(f"\nInvalidation Rate: {rate:.2f}%")
    else:
        print("No substantive applications found.")

if __name__ == "__main__":
    verify_stats()
