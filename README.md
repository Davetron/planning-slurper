# Planning Slurper

A data pipeline that scrapes planning applications from Irish local planning authority (LPA) APIs in the Dublin area, stores them in PostgreSQL, and runs analysis to identify patterns in application outcomes.

## Features

- **Multi-LPA Support**: Syncs data from 4 Dublin councils (Dun Laoghaire-Rathdown, Fingal, Dublin City, South Dublin)
- **Incremental Sync**: Only fetches new applications since last run
- **Application Hydration**: Enriches applications with documents and planning conditions
- **Analysis Reports**: Generates JSON reports on agents, churn, failures, lifecycle, and geographic spread
- **Automated Daily Scrape**: GitHub Actions workflow for scheduled data collection

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Create a `.env` file with your database connection:
   ```
   DATABASE_URL=postgresql://user:pass@host/db
   ```

3. Run the pipeline:
   ```bash
   python main.py
   ```

## Usage

```bash
# Full pipeline (sync + analysis)
python main.py

# Sync only (fetch new applications)
python main.py --sync-only

# Analysis only (generate reports from existing data)
python main.py --analyze-only
```

## Output

Analysis reports are written to `out/`:
- `agents_latest.json` - Agent/architect invalidation statistics
- `churn_latest.json` - Application churn patterns
- `failures_latest.json` - Detailed failure analysis
- `lifecycle_latest.json` - Application lifecycle metrics
- `spread_latest.json` - Geographic distribution

## Database Schema

- **applications** - Planning applications with composite primary key (id, lpa)
- **documents** - Application documents
- **conditions** - Planning conditions attached to applications

## License

MIT
