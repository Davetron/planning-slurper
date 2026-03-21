"""
Backfill geom column in applications table.

Two strategies:
  1. ITM→WGS84 conversion from grid_x/grid_y columns (fast, free)
  2. Google Geocoding API for rows with location text but no coords

Usage:
  python backfill_geom.py --coords-only [--dry-run]
  python backfill_geom.py --geocode-only [--dry-run]
  python backfill_geom.py [--dry-run]   # run both
"""

import argparse
import os
import sys

import psycopg2
from dotenv import load_dotenv
from pyproj import Transformer

load_dotenv()

# ITM (EPSG:2157) → WGS84 (EPSG:4326)
_ITM_TO_WGS84 = Transformer.from_crs("EPSG:2157", "EPSG:4326", always_xy=False)

# Dublin bounding box in ITM metres (easting, northing)
_DUBLIN_E_MIN = 690_000
_DUBLIN_E_MAX = 740_000
_DUBLIN_N_MIN = 710_000
_DUBLIN_N_MAX = 750_000


def itm_to_wgs84(easting: float, northing: float) -> tuple[float, float]:
    """Convert Irish Transverse Mercator coordinates to WGS84 lat/lon.

    Args:
        easting: ITM easting (metres)
        northing: ITM northing (metres)

    Returns:
        (latitude, longitude) in decimal degrees
    """
    lat, lon = _ITM_TO_WGS84.transform(easting, northing)
    return lat, lon


def is_valid_dublin_grid(grid_x, grid_y) -> bool:
    """Return True if the ITM coordinates fall within the Dublin bounding box."""
    if grid_x is None or grid_y is None:
        return False
    try:
        x = float(grid_x)
        y = float(grid_y)
    except (TypeError, ValueError):
        return False
    return (
        _DUBLIN_E_MIN <= x <= _DUBLIN_E_MAX
        and _DUBLIN_N_MIN <= y <= _DUBLIN_N_MAX
    )


def get_db_connection():
    """Open a psycopg2 connection using DATABASE_URL from the environment."""
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL environment variable is not set")
    return psycopg2.connect(db_url)


def backfill_from_grid(conn, dry_run: bool = False) -> int:
    """Populate geom from grid_x/grid_y for rows that have coords but no geom.

    Commits every 1000 rows (unless dry_run).

    Returns:
        Number of rows updated (or that would be updated in dry_run mode).
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, lpa, grid_x, grid_y
        FROM applications
        WHERE geom IS NULL
          AND grid_x IS NOT NULL
          AND grid_y IS NOT NULL
        ORDER BY id, lpa
        """
    )
    rows = cur.fetchall()
    print(f"[coords] Rows with grid coords but no geom: {len(rows)}")

    updated = 0
    skipped = 0
    batch = []

    for app_id, lpa, grid_x, grid_y in rows:
        if not is_valid_dublin_grid(grid_x, grid_y):
            skipped += 1
            continue

        lat, lon = itm_to_wgs84(float(grid_x), float(grid_y))
        batch.append((lon, lat, app_id, lpa))
        updated += 1

        if len(batch) >= 1000:
            if not dry_run:
                cur.executemany(
                    """
                    UPDATE applications
                    SET geom = ST_SetSRID(ST_MakePoint(%s, %s), 4326)
                    WHERE id = %s AND lpa = %s
                    """,
                    batch,
                )
                conn.commit()
            print(f"[coords] {'(dry-run) ' if dry_run else ''}Updated {updated} rows so far …")
            batch = []

    # Final partial batch
    if batch:
        if not dry_run:
            cur.executemany(
                """
                UPDATE applications
                SET geom = ST_SetSRID(ST_MakePoint(%s, %s), 4326)
                WHERE id = %s AND lpa = %s
                """,
                batch,
            )
            conn.commit()

    cur.close()
    print(
        f"[coords] {'(dry-run) ' if dry_run else ''}Done. "
        f"Updated: {updated}, Skipped (out-of-bounds): {skipped}"
    )
    return updated


def backfill_from_geocoding(conn, dry_run: bool = False) -> int:
    """Geocode rows that have a location string but no geom and no grid coords.

    Commits every 100 rows (unless dry_run).

    Returns:
        Number of rows updated (or that would be updated in dry_run mode).
    """
    import googlemaps  # local import — optional dependency

    api_key = os.environ.get("GOOGLE_GEOCODING_API_KEY")
    if not api_key:
        raise RuntimeError("GOOGLE_GEOCODING_API_KEY environment variable is not set")

    gmaps = googlemaps.Client(key=api_key)

    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, lpa, location
        FROM applications
        WHERE geom IS NULL
          AND (grid_x IS NULL OR grid_y IS NULL)
          AND location IS NOT NULL
          AND location <> ''
        ORDER BY id, lpa
        """
    )
    rows = cur.fetchall()
    print(f"[geocode] Rows with location text but no geom: {len(rows)}")

    updated = 0
    failed = 0

    for i, (app_id, lpa, location) in enumerate(rows):
        query = f"{location}, Ireland"
        try:
            results = gmaps.geocode(query, components={"country": "IE"})
        except Exception as exc:
            print(f"[geocode] ERROR geocoding id={app_id} lpa={lpa}: {exc}")
            failed += 1
            continue

        if not results:
            failed += 1
            continue

        loc = results[0]["geometry"]["location"]
        lat = loc["lat"]
        lon = loc["lng"]

        if not dry_run:
            cur.execute(
                """
                UPDATE applications
                SET geom = ST_SetSRID(ST_MakePoint(%s, %s), 4326)
                WHERE id = %s AND lpa = %s
                """,
                (lon, lat, app_id, lpa),
            )
            if (i + 1) % 100 == 0:
                conn.commit()
                print(f"[geocode] Committed {i + 1} rows processed, {updated} updated …")

        updated += 1

    if not dry_run:
        conn.commit()

    cur.close()
    print(
        f"[geocode] {'(dry-run) ' if dry_run else ''}Done. "
        f"Updated: {updated}, Failed/not-found: {failed}"
    )
    return updated


def main():
    parser = argparse.ArgumentParser(description="Backfill geom column in applications table")
    parser.add_argument("--coords-only", action="store_true", help="Only run ITM→WGS84 conversion")
    parser.add_argument("--geocode-only", action="store_true", help="Only run Google geocoding")
    parser.add_argument("--dry-run", action="store_true", help="Compute but do not write to DB")
    args = parser.parse_args()

    run_coords = args.coords_only or (not args.coords_only and not args.geocode_only)
    run_geocode = args.geocode_only or (not args.coords_only and not args.geocode_only)

    conn = get_db_connection()
    try:
        if run_coords:
            backfill_from_grid(conn, dry_run=args.dry_run)
        if run_geocode:
            backfill_from_geocoding(conn, dry_run=args.dry_run)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
