"""One-off parallel hydration for backfilled Dublin City applications."""

import concurrent.futures
import time
from main import get_db_connection, hydrate_application

def get_unhydrated_dcc():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("""
        SELECT a.id FROM applications a
        WHERE a.lpa = 'dublincity'
          AND a.last_hydrated_at IS NULL
    """)
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows]

def hydrate_one(app_id):
    try:
        hydrate_application(app_id, lpa='dublincity')
        return app_id, True
    except Exception as e:
        return app_id, False

if __name__ == "__main__":
    app_ids = get_unhydrated_dcc()
    total = len(app_ids)
    print(f"Found {total} unhydrated Dublin City applications")

    if total == 0:
        print("Nothing to do.")
        exit(0)

    workers = 8
    print(f"Hydrating with {workers} parallel workers (no sleep)...")

    done = 0
    failed = 0
    start = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(hydrate_one, aid): aid for aid in app_ids}
        for future in concurrent.futures.as_completed(futures):
            app_id, success = future.result()
            done += 1
            if not success:
                failed += 1
            if done % 100 == 0:
                elapsed = time.time() - start
                rate = done / elapsed
                remaining = (total - done) / rate if rate > 0 else 0
                print(f"[{done}/{total}] {rate:.1f} apps/s, ~{remaining/60:.0f} min remaining, {failed} failed", flush=True)

    elapsed = time.time() - start
    print(f"\nDone. {done} apps in {elapsed/60:.1f} minutes ({done/elapsed:.1f} apps/s). {failed} failed.")
