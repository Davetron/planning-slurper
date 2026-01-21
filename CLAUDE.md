# CLAUDE.md

## Project Overview

Planning Slurper is a data pipeline that scrapes planning applications from Irish local planning authority APIs (Dublin area councils), stores them in PostgreSQL, and runs analysis to identify patterns in application outcomes.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run full pipeline (sync + analysis)
python main.py

# Run sync only (fetch new applications)
python main.py --sync-only

# Run analysis only
python main.py --analyze-only

# Run tests
pytest tests/
```

## Architecture

### Core Files
- `main.py` - Entry point, orchestration, database schema, API client, sync pipeline
- `shared_utils.py` - Common text normalization and location matching utilities
- `analyze_*.py` - Analysis modules (agents, churn, invalid, lifecycle, spread)

### Data Flow
1. **Sync Stage**: Fetches applications from 4 LPAs in parallel (dunlaoghaire, fingal, dublincity, southdublin)
2. **Hydration**: Enriches each application with documents and conditions
3. **Analysis Stage**: Generates JSON reports in `out/` directory

### Database Schema (PostgreSQL/Neon)
- `applications` - Planning applications (composite PK: id, lpa)
- `documents` - Application documents (FK to applications)
- `conditions` - Planning conditions (FK to applications)

### External APIs
- `planningapi.agileapplications.ie` - Planning data API
- `identity.agileapplications.ie` - LPA code lookup

## Conventions

- LPA codes: DLR, FG, DC, SD (mapped from full names via identity API)
- Analysis output goes to `out/*.json` with timestamp wrapper
- Environment: `DATABASE_URL` required in `.env`

## Environment Setup

Create a `.env` file:
```
DATABASE_URL=postgresql://...
```

## Commit Rules

**IMPORTANT:** Before completing any task, you MUST run `/commit` to commit your changes.

- Only commit files YOU modified in this session - never commit unrelated changes
- Use atomic commits with descriptive messages
- If there are no changes to commit, skip this step
- Do not push unless explicitly asked
