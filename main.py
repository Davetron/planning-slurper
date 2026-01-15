import requests
import json
import time
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import dotenv
from datetime import datetime, timedelta
import concurrent.futures

dotenv.load_dotenv()

# --- Configuration & Constants ---
# DB_PATH = "applications.db" # No longer used
DOWNLOAD_BASE_DIR = "/Users/david/Documents/dlrcc_planning_applications"
API_BASE_URL = "https://planningapi.agileapplications.ie/api"
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set. Please create a .env file.")

# --- Database Setup & Management ---

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def setup_database():
    """Initializes the database schema."""
    conn = get_db_connection()
    c = conn.cursor()

    
    # Create Schema for PostgreSQL
    _create_schema(c)
    
    conn.commit()
    conn.close()

def _create_schema(c):
    # 1. Applications Table (Composite PK)
    c.execute('''CREATE TABLE IF NOT EXISTS applications
                 (id INTEGER, 
                  lpa TEXT,
                  reference TEXT, 
                  registration_date DATE, 
                  description TEXT, 
                  raw_json JSONB,
                  location TEXT,
                  decision TEXT,
                  status TEXT,
                  grid_x DOUBLE PRECISION,
                  grid_y DOUBLE PRECISION,
                  last_hydrated_at TIMESTAMP,
                  PRIMARY KEY (id, lpa))''')
    
    try:
        c.execute("ALTER TABLE applications ADD COLUMN IF NOT EXISTS last_hydrated_at TIMESTAMP")
        
        # Migration: text -> date
        c.execute("ALTER TABLE applications ALTER COLUMN registration_date TYPE DATE USING registration_date::date")
        
        # Migration: text -> jsonb
        c.execute("ALTER TABLE applications ALTER COLUMN raw_json TYPE JSONB USING raw_json::jsonb")
        
    except psycopg2.Error as e:
         print(f"Migration notice: {e}")
         # Continue, likely already exists or other non-fatal
         pass
                  
    # 2. Documents Table (Composite FK)
    c.execute('''CREATE TABLE IF NOT EXISTS documents
                 (id SERIAL PRIMARY KEY, 
                  app_id INTEGER, 
                  lpa TEXT,
                  filename TEXT, 
                  local_path TEXT,
                  document_hash TEXT,
                  raw_json TEXT,
                  doc_id TEXT,
                  description TEXT,
                  media_description TEXT,
                  received_date TEXT,
                  media_id INTEGER,
                  FOREIGN KEY(app_id, lpa) REFERENCES applications(id, lpa))''')
    
    # 3. Conditions Table (Composite FK)
    c.execute('''CREATE TABLE IF NOT EXISTS conditions
                 (id SERIAL PRIMARY KEY, 
                  app_id INTEGER, 
                  lpa TEXT,
                  order_num INTEGER,
                  short_desc TEXT,
                  long_desc TEXT,
                  code TEXT,
                  code_desc TEXT,
                  complied_id INTEGER,
                  complied_desc TEXT,
                  complied_date TEXT,
                  raw_json TEXT,
                  FOREIGN KEY(app_id, lpa) REFERENCES applications(id, lpa))''')

# --- Data Access Object (DAO) Layer ---

def save_application(app_data, lpa="dunlaoghaire"):
    """Upserts an application record."""
    conn = get_db_connection()
    c = conn.cursor()
    
    app_id = app_data.get('id')
    reference = app_data.get('reference') or app_data.get('applicationReference')
    reg_date = app_data.get('registrationDate')
    description = app_data.get('proposal') or app_data.get('description')
    
    location = app_data.get('location')
    decision = app_data.get('decisionText')
    status = app_data.get('status')
    
    # Grid Reference Parsing
    grid_ref = app_data.get('gridReference')
    grid_x = None
    grid_y = None
    
    # Try parsing "X, Y" string
    if grid_ref and ',' in grid_ref:
        try:
            parts = grid_ref.split(',')
            grid_x = float(parts[0].strip())
            grid_y = float(parts[1].strip())
        except ValueError: pass
            
    # Fallback to easting/northing
    if grid_x is None and 'easting' in app_data and 'northing' in app_data:
        try:
            grid_x = float(app_data['easting'])
            grid_y = float(app_data['northing'])
        except (ValueError, TypeError): pass
    
    c.execute('''INSERT INTO applications 
                 (id, reference, registration_date, description, raw_json, 
                  location, decision, status, grid_x, grid_y, lpa)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                 ON CONFLICT (id, lpa) DO UPDATE SET
                    reference = EXCLUDED.reference,
                    registration_date = EXCLUDED.registration_date,
                    description = EXCLUDED.description,
                    raw_json = EXCLUDED.raw_json,
                    location = EXCLUDED.location,
                    decision = EXCLUDED.decision,
                    status = EXCLUDED.status,
                    grid_x = EXCLUDED.grid_x,
                    grid_y = EXCLUDED.grid_y''', 
              (app_id, reference, reg_date, description, json.dumps(app_data), 
               location, decision, status, grid_x, grid_y, lpa))
    
    conn.commit()
    conn.close()

def save_document_metadata(app_id, doc_data, lpa="dunlaoghaire"):
    """Saves or updates document metadata."""
    conn = get_db_connection()
    c = conn.cursor()
    
    filename = doc_data.get('name') or doc_data.get('originalFileName')
    doc_hash = doc_data.get('documentHash')
    raw_json = json.dumps(doc_data)
    
    # Mapped Fields
    doc_id = str(doc_data.get('documentId')) if doc_data.get('documentId') else None
    desc = doc_data.get('description')
    media_desc = doc_data.get('mediaDescription')
    received_date = doc_data.get('receivedDate')
    media_id = doc_data.get('mediaId')
    
    # Check for existing document by different means to deduplicate
    # Uniqueness isn't guaranteed by hash alone potentially, but let's try to match existing logic
    # In Postgres, we can't easily do "INSERT IF NOT EXISTS by non-unique key", so we check first.
    
    existing_id = None
    if doc_hash:
        c.execute("SELECT id FROM documents WHERE document_hash = %s", (doc_hash,))
        row = c.fetchone()
        if row: existing_id = row[0]
        
    if not existing_id:
        c.execute("SELECT id FROM documents WHERE app_id = %s AND lpa = %s AND filename = %s", (app_id, lpa, filename))
        row = c.fetchone()
        if row: existing_id = row[0]
    
    if not existing_id:
        c.execute('''INSERT INTO documents (
                        app_id, lpa, filename, document_hash, raw_json, local_path, 
                        doc_id, description, media_description, received_date, media_id)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', 
                     (app_id, lpa, filename, doc_hash, raw_json, None,
                      doc_id, desc, media_desc, received_date, media_id))
    else:
        c.execute('''UPDATE documents 
                     SET document_hash = %s, raw_json = %s, doc_id = %s, description = %s, 
                         media_description = %s, received_date = %s, media_id = %s
                     WHERE id = %s''', 
                     (doc_hash, raw_json, doc_id, desc, media_desc, received_date, media_id, existing_id))
             
    conn.commit()
    conn.close()

def save_condition_record(app_id, cond_data, lpa="dunlaoghaire"):
    """Saves or updates a condition record."""
    conn = get_db_connection()
    c = conn.cursor()
    
    raw_json = json.dumps(cond_data)
    order_num = cond_data.get('orderNumber')
    
    existing_id = None
    # Uniqueness check: App ID + LPA + Order Num
    c.execute("SELECT id FROM conditions WHERE app_id = %s AND lpa = %s AND order_num = %s", (app_id, lpa, order_num))
    row = c.fetchone()
    if row: existing_id = row[0]
    
    fields = (
        cond_data.get('shortPrescription'),
        cond_data.get('longPrescription'),
        cond_data.get('prescriptionCode'),
        cond_data.get('prescriptionCodeDescription'),
        cond_data.get('compliedId'),
        cond_data.get('compliedStatusDescription'),
        cond_data.get('compliedDate'),
        raw_json
    )
    
    if not existing_id:
         c.execute('''INSERT INTO conditions (
                        short_desc, long_desc, code, code_desc, 
                        complied_id, complied_desc, complied_date, raw_json,
                        app_id, lpa, order_num)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                      (*fields, app_id, lpa, order_num))
    else:
         c.execute('''UPDATE conditions SET
                        short_desc = %s, long_desc = %s, code = %s, code_desc = %s,
                        complied_id = %s, complied_desc = %s, complied_date = %s, raw_json = %s
                      WHERE id = %s''',
                      (*fields, existing_id))
    
    conn.commit()
    conn.close()

def save_document_record(app_id, filename, local_path, lpa="dunlaoghaire"):
    """Updates the local_path for a downloaded document."""
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT id FROM documents WHERE app_id = %s AND lpa = %s AND filename = %s", (app_id, lpa, filename))
    row = c.fetchone()
    
    if row:
        c.execute("UPDATE documents SET local_path = %s WHERE id = %s", (local_path, row[0]))
    else:
        # Should usually exist from metadata fetch, but if not:
        c.execute("INSERT INTO documents (app_id, lpa, filename, local_path) VALUES (%s, %s, %s, %s)", 
                  (app_id, lpa, filename, local_path))
    conn.commit()
    conn.close()

# --- API Client Layer ---

def get_lpa_code(lpa_name):
    """
    Fetches the LPA code from the identity API.
    E.g., 'fingal' -> 'FG', 'dunlaoghaire' -> 'DLR'
    """
    url = f"https://identity.agileapplications.ie/api/client/get?url={lpa_name}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data.get('code')
    except Exception as e:
        print(f"Error fetching LPA code for {lpa_name}: {e}", flush=True)
        return None

def fetch_planning_applications(limit=None, date_from='2025-01-09', date_to='2026-01-08', skip_existing=False, lpa="dunlaoghaire"):
    """
    Fetches planning applications from the API.
    Returns: List of application dictionaries.
    """
    lpa_code = get_lpa_code(lpa)
    if not lpa_code:
        print(f"Could not retrieve LPA code for {lpa}. Aborting.", flush=True)
        return []

    url = f'{API_BASE_URL}/application/search'
    
    # Check existing IDs if skipping
    existing_ids = set()
    if skip_existing:
        try:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT id FROM applications WHERE lpa = %s", (lpa,))
            existing_ids = {row[0] for row in c.fetchall()}
            conn.close()
            print(f"Found {len(existing_ids)} existing applications for {lpa} in DB.", flush=True)
        except Exception as e:
            print(f"Error checking existing IDs: {e}", flush=True)
    
    # Default search range: Recent year
    # Note: These dates could be parameterized if needed
    params = {
        'applicationDateFrom': date_from,
        'applicationDateTo': date_to,
        'openApplications': 'false'
    }

    headers = {
        'accept': 'application/json, text/plain, */*',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        'x-client': lpa_code,
        'x-product': 'CITIZENPORTAL',
        'x-service': 'PA'
    }

    try:
        print(f"Fetching data for {lpa} (Code: {lpa_code})...", flush=True)
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        results = []
        if isinstance(data, list):
            results = data
        elif isinstance(data, dict) and 'results' in data:
            results = data['results']
            
        if results:
            print(f"Successfully fetched {len(results)} applications.", flush=True)
            
            # Filter existing
            if skip_existing:
                original_count = len(results)
                results = [app for app in results if app.get('id') not in existing_ids]
                skipped = original_count - len(results)
                if skipped > 0:
                    print(f"Skipping {skipped} existing applications.", flush=True)

            if limit:
                print(f"Limiting to first {limit} applications.", flush=True)
                results = results[:limit]
            
            for app in results:
                save_application(app, lpa=lpa)
            print(f"Saved {len(results)} applications to database.", flush=True)
            return results
        else:
            print("No results found.", flush=True)
            return []

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}", flush=True)
        return []

def hydrate_application(app_id, lpa="dunlaoghaire"):
    """Fetches and saves full details, documents, and conditions for a single app."""
    # print(f"Hydrating App {app_id}...", flush=True)
    
    lpa_code = get_lpa_code(lpa)
    if not lpa_code:
        print(f"Could not retrieve LPA code for {lpa}. Skipping hydration.", flush=True)
        return

    headers = {'x-client': lpa_code, 'x-product': 'CITIZENPORTAL', 'x-service': 'PA'}
    
    try:
        # 1. Details
        r = requests.get(f"{API_BASE_URL}/application/{app_id}", headers=headers)
        if r.status_code == 200:
            save_application(r.json(), lpa=lpa)
        
        # 2. Documents
        r = requests.get(f"{API_BASE_URL}/application/{app_id}/document", headers=headers)
        if r.status_code == 200:
            docs = r.json()
            # print(f"  - Saving metadata for {len(docs)} documents.", flush=True)
            for doc in docs:
                save_document_metadata(app_id, doc, lpa=lpa)

        # 3. Conditions
        r = requests.get(f"{API_BASE_URL}/application/{app_id}/conditions", headers=headers)
        if r.status_code == 200:
            conds = r.json().get('applicationPrescriptions', [])
            if conds:
                # print(f"  - Saving {len(conds)} conditions.", flush=True)
                for c in conds:
                    save_condition_record(app_id, c, lpa=lpa)

        # 4. Mark as hydrated
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("UPDATE applications SET last_hydrated_at = NOW() WHERE id = %s AND lpa = %s", (app_id, lpa))
        conn.commit()
        conn.close()

    except Exception as e:
        print(f"Error hydrating app {app_id}: {e}", flush=True)

def download_document(doc_hash, save_dir, filename):
    """Downloads a specific document."""
    url = f"{API_BASE_URL}/application/document/DLR/{doc_hash}"
    headers = {'x-client': 'DLR', 'x-product': 'CITIZENPORTAL', 'x-service': 'PA'}
    
    try:
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)
            
        filepath = os.path.join(save_dir, filename)
        
        with requests.get(url, headers=headers, stream=True) as r:
            r.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"Saved to {filepath}", flush=True)
        return filepath
    except Exception as e:
        print(f"Download failed for {doc_hash}: {e}", flush=True)
        return None

# --- Application Logic & Orchestration ---

def search_applications(date_from=None, date_to=None, decision=None, status=None, location_keyword=None, 
                        min_grid_x=None, max_grid_x=None, min_grid_y=None, max_grid_y=None):
    """Queries the database for applications matching criteria."""
    conn = get_db_connection()
    c = conn.cursor()
    
    query = "SELECT id, reference, registration_date, decision, status, location, description, grid_x, grid_y, lpa FROM applications WHERE 1=1"
    params = []
    
    if date_from:
        query += " AND registration_date >= %s"
        params.append(date_from)
    if date_to:
        query += " AND registration_date <= %s"
        params.append(date_to)
    if decision:
        query += " AND decision LIKE %s"
        params.append(f"%{decision}%")
    if status:
        query += " AND status LIKE %s"
        params.append(f"%{status}%")
    if location_keyword:
        query += " AND location LIKE %s"
        params.append(f"%{location_keyword}%")
    
    if min_grid_x is not None:
        query += " AND grid_x >= %s"
        params.append(min_grid_x)
    if max_grid_x is not None:
        query += " AND grid_x <= %s"
        params.append(max_grid_x)
    if min_grid_y is not None:
        query += " AND grid_y >= %s"
        params.append(min_grid_y)
    if max_grid_y is not None:
        query += " AND grid_y <= %s"
        params.append(max_grid_y)
        
    query += " ORDER BY registration_date DESC LIMIT 20"
    
    print(f"Executing Search: {query} with params {params}", flush=True)
    c.execute(query, params)
    results = c.fetchall()
    conn.close()
    return results

def hydrate_all_applications(limit=None, skip_hydrated=False, lpa_filter=None):
    """Batch processes applications to fetch full details."""
    conn = get_db_connection()
    c = conn.cursor()
    
    query = "SELECT a.id, a.lpa FROM applications a"
    params = []
    where_clauses = []

    if skip_hydrated:
        # Check explicit last_hydrated_at timestamp OR existence of linked data
        complex_clause = """(
            last_hydrated_at IS NULL
            AND NOT EXISTS (SELECT 1 FROM documents d WHERE d.app_id = a.id AND d.lpa = a.lpa)
            AND NOT EXISTS (SELECT 1 FROM conditions c WHERE c.app_id = a.id AND c.lpa = a.lpa)
        )"""
        where_clauses.append(complex_clause)
    
    if lpa_filter:
        where_clauses.append("lpa = %s")
        params.append(lpa_filter)

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    
    # Needs GROUP BY or DISTINCT if we join, though strictly if 1:many filtered by NULL it's fine.
    # But LEFT JOIN might multiply rows if not careful? 
    # Actually if d.id IS NULL, it means there are NO matches, so row count is preserved (1 per app that has 0 docs).
    # If using NOT EXISTS it's cleaner reading but LEFT JOIN is fine.
    
    print(f"Fetching applications for hydration (Skip Hydrated: {skip_hydrated})...", flush=True)
    c.execute(query, params)
    rows = c.fetchall()
    
    conn.close()
    
    total = len(rows)
    print(f"Found {total} applications needing hydration.", flush=True)

    processed = 0
    for i, row in enumerate(rows):
        if limit and processed >= limit:
            print(f"Limit of {limit} reached.", flush=True)
            break
            
        app_id = row[0]
        lpa = row[1]
        
        # Already filtered in SQL
        print(f"[{i+1}/{total}] Hydrating {app_id} ({lpa})", end="\r", flush=True)
        hydrate_application(app_id, lpa=lpa)
        time.sleep(0.5) 
        processed += 1

        processed += 1

def get_latest_application_date(lpa):
    """Retrieves the latest registration date for a given LPA."""
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute("SELECT MAX(registration_date) FROM applications WHERE lpa = %s", (lpa,))
        result = c.fetchone()
        if result and result[0]:
            return result[0]
    except Exception as e:
        print(f"Error getting latest date: {e}", flush=True)
    finally:
        conn.close()
    return None

def run_sync_job(limit=100, date_from=None, date_to=None, lpa="dunlaoghaire"):
    """
    Main Workflow:
    1. Fetches applications (incrementally if dates not provided).
    2. Hydrates them.
    """
    # ensure DB structure
    setup_database()
    
    # Determine dates if not provided
    if not date_to:
        date_to = datetime.now().strftime('%Y-%m-%d')
        
    if not date_from:
        latest = get_latest_application_date(lpa)
        if latest:
            # Depending on how the API works, we might want to start FROM that day or day after.
            # Usually safe to overlap a bit, or use the exact day.
            # Given "updates only from now on" implying we want new stuff.
            # Let's assume registration_date is a string 'YYYY-MM-DD'.
            print(f"Found existing data for {lpa} up to {latest}.", flush=True)
            date_from = latest 
        else:
            date_from = '2024-01-01' # Fallback default
            print(f"No existing data for {lpa}. Defaulting to {date_from}.", flush=True)
    
    print(f"--- Starting Sync Job (Limit: {limit}, LPA: {lpa}, Date Range: {date_from} -> {date_to}) ---", flush=True)
    
    # Always skip existing to avoid re-fetching what we have
    skip_mode = True 
    
    fetch_planning_applications(limit=limit, date_from=date_from, date_to=date_to, skip_existing=skip_mode, lpa=lpa)
    hydrate_all_applications(limit=None, skip_hydrated=skip_mode, lpa_filter=lpa) 
    print("--- Sync Job Complete ---", flush=True)

# --- Entry Point ---

# --- Analysis Imports ---
from analyze_agents import analyze_agents
from analyze_churn_agents import analyze_churn_agents
from analyze_invalid import analyze_detailed_failures
from analyze_lifecycle import analyze_lifecycle
from analyze_spread import analyze_spread

import argparse
import sys

# ... previous imports ...

def run_sync_stage():
    """
    Executes the synchronization stage for all LPAs.
    """
    print("=== Starting Sync Stage ===", flush=True)
    lpas = ["dunlaoghaire", "fingal", "dublincity", "southdublin"]
    
    # Run setup once to avoid race conditions on table creation
    setup_database()
    
    print(f"Syncing {len(lpas)} LPAs in parallel...", flush=True)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(lpas)) as executor:
        futures = {executor.submit(run_sync_job, limit=None, lpa=lpa): lpa for lpa in lpas}
        
        for future in concurrent.futures.as_completed(futures):
            lpa = futures[future]
            try:
                future.result()
                print(f"=== Finished Syncing {lpa} ===", flush=True)
            except Exception as e:
                print(f"generated an exception during sync for {lpa}: {e}", flush=True)

def run_analysis_stage():
    """
    Executes the analysis stage and writes output to JSON.
    """
    print("\n=== Starting Analysis Stage ===", flush=True)
    
    analysis_map = {
        "agents_latest.json": analyze_agents,
        "churn_latest.json": analyze_churn_agents,
        "failures_latest.json": analyze_detailed_failures,
        "lifecycle_latest.json": analyze_lifecycle,
        "spread_latest.json": analyze_spread
    }
    
    # Output directory
    out_dir = "out"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
        
    timestamp = datetime.now().isoformat()
    
    for filename, func in analysis_map.items():
        print(f"Running {func.__name__}...", flush=True)
        try:
            data = func()
            result = {
                "timestamp": timestamp,
                "data": data
            }
            
            out_path = os.path.join(out_dir, filename)
            print(f"Writing to {out_path}", flush=True)
            with open(out_path, 'w') as f:
                json.dump(result, f, indent=2)
                
        except Exception as e:
            print(f"Error running {func.__name__}: {e}", flush=True)
    
    print("Analysis Complete.", flush=True)

def run_pipeline(skip_sync=False, skip_analysis=False):
    """
    Runs the pipeline based on flags.
    """
    if not skip_sync:
        run_sync_stage()
    else:
        print("Skipping Sync Stage.")
        
    if not skip_analysis:
        run_analysis_stage()
    else:
        print("Skipping Analysis Stage.")

# --- Entry Point ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Planning Slurper Pipeline")
    parser.add_argument("--analyze-only", action="store_true", help="Run only the analysis stage")
    parser.add_argument("--sync-only", action="store_true", help="Run only the sync stage")
    
    args = parser.parse_args()
    
    if args.analyze_only:
        run_pipeline(skip_sync=True)
    elif args.sync_only:
        run_pipeline(skip_analysis=True)
    else:
        run_pipeline()

