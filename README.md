# Playvox → BigQuery QA & Workforce Analytics Pipeline

A production-style data pipeline that extracts Quality Assurance and
workforce management data (agent evaluations, coaching sessions,
calibrations, scorecards, and the underlying interaction log) from the
**Playvox API** and loads it into **Google BigQuery**, turning QA process
data into a queryable analytics layer.

---

## What problem this solves

Contact centers and BPOs (this project was built against a QA/testing
operation) run their quality process inside Playvox: agents are
evaluated against scorecards, low scores trigger coaching, and calibration
sessions keep multiple QA analysts scoring consistently. That data is
operationally useful inside Playvox's own UI, but it's hard to analyze
*across* those objects there - e.g. "which scorecard sections predict a
coaching session," "is analyst A scoring systematically higher than
analyst B," "what's the QA pass-rate trend by team over the last
quarter." This pipeline lands all of it in BigQuery so those questions
become a SQL query (and a Looker Studio / Tableau dashboard) instead of
manual cross-referencing.

**Corporate value, concretely:**
- **QA trend reporting** — pass rates, average scores, and error patterns
  by team, agent, or scorecard section over any time window.
- **Coaching effectiveness** — join `coachings` to `evaluations` to see
  whether coaching actually moves an agent's subsequent scores.
- **Calibration/consistency monitoring** — compare analysts' scores on
  the same interaction to catch scoring drift before it skews the whole
  QA program.
- **A single historical record** — Playvox's own UI is built for
  day-to-day operations, not long-range trend analysis; this pipeline is
  the system of record for "what did QA performance look like six months
  ago."

---

## Why this project matters

QA and coaching data is where a support/BPO operation actually finds out
whether its quality process is working — but that only holds if the
underlying data pipeline is trustworthy. This project shows the skill
set behind that: reading eight inconsistent, partially-duplicated scripts,
identifying which patterns were justified and which were accidental
complexity, and consolidating both into something a business could
actually schedule and rely on.

**Principles applied:**
- **Consolidate before you clean** — eight scripts reimplementing the
  same fetch/upload logic were merged into one shared client and loader
  first, so every subsequent fix applied everywhere at once instead of
  needing to be repeated eight times.
- **Match the load strategy to the data's actual behavior** — incremental
  upserts for data that changes in place, full refresh for low-volume
  nested data, rather than one strategy applied everywhere out of habit.
- **Remove complexity that isn't earning its keep** — a local SQLite
  database used purely for ID-to-name lookups was replaced with a single
  BigQuery query, because the added moving part wasn't solving a real
  problem at this data volume.
- **Read the code, don't just run it** — several of the improvements
  below came from tracing what each script actually did line by line,
  not from assuming the original logic was correct.

---

## Features

- **Eight independent pipelines** — users, teams, roles, interactions,
  scorecards, calibrations, coachings, evaluations — each runnable on its
  own or together, with dependency-aware ordering handled automatically.
- **One shared API client** replacing eight near-duplicate fetch loops,
  with consistent pagination, retry/backoff, and rate-limit handling.
- **Two loading strategies, chosen deliberately per table** (see
  [`docs/schema.md`](docs/schema.md) for the full breakdown):
  incremental `MERGE` for tabular, frequently-touched data; full-refresh
  JSON load for low-volume, deeply nested data.
- **In-memory name enrichment** — coaching and evaluation records resolve
  agent/coach/team IDs to human-readable names via a single BigQuery
  lookup query, no local database required.
- **Environment-based configuration** — zero secrets in source code.
- **Structured logging** throughout.

### Bugs fixed during the rebuild

Writing clean code for a new project is one skill; reading someone else's
inconsistent, partially-working scripts and finding the defects hiding
inside them is a different one — closer to what maintaining a real
production pipeline actually looks like. These were found by tracing the
original logic, not assumed:

- **`interactions`**: the original script appended each *page* of API
  results as a single row (so one row held up to 100 interactions nested
  inside it) instead of one row per interaction. Fixed by extracting
  `result` before storing.
- **`users` / `evaluations` uploads**: both computed a `new_data` /
  `updated_data` split for logging, then uploaded the *entire* dataset
  with `WRITE_APPEND` regardless — duplicating every existing row on
  every run. Replaced with a proper `MERGE` upsert.
- **`teams`**: fetched pages concurrently via `asyncio`, but capped
  itself at a hardcoded `max_pages=5` (500 records) with no warning if
  more existed — silently dropping data past that point. Replaced with
  the same fully-paginated sync client used everywhere else.
- **`roles`**: stored a column literally named `default`, a reserved
  word in most SQL dialects. Renamed to `is_default`.

---

## Architecture

```
                ┌────────────────────┐
                │    Playvox API      │
                │ (users, teams,      │
                │  roles, interactions,│
                │  scorecards,        │
                │  calibrations,      │
                │  coachings,         │
                │  evaluations)       │
                └─────────┬───────────┘
                          │  REST (paginated via next_page, rate-limited)
                          ▼
                ┌────────────────────┐
                │   PlayvoxClient     │  src/playvox_client.py
                │   (extract layer)   │
                └─────────┬───────────┘
                          │  raw JSON
                          ▼
                ┌────────────────────┐
                │  Pipeline modules   │  src/pipelines/*.py
                │  (transform layer): │
                │  clean, flatten,    │
                │  filter by date,    │
                │  enrich with names  │
                └─────────┬───────────┘
                          │  pandas DataFrame / JSON records
                          ▼
                ┌────────────────────┐
                │   BigQueryLoader    │  src/bigquery_loader.py
                │   (load layer):     │
                │   merge or          │
                │   full-refresh JSON │
                └─────────┬───────────┘
                          │
                          ▼
                ┌────────────────────┐
                │   Google BigQuery   │
                │   playvox dataset   │
                └─────────────────────┘
```

### Why BigQuery?

Serverless (no infrastructure to run for what is, at this data volume, a
modest daily sync), cheap at this scale, and connects natively to the BI
tools (Looker Studio, Tableau) a QA/operations team would already be
using to build dashboards on top of this data.

### How the Playvox API works in this project

Playvox paginates list endpoints via a `next_page` boolean in the JSON
response body (rather than an HTTP `Link` header), so `PlayvoxClient`
follows that field until it's `false`. Authentication is HTTP Basic Auth
using an API key formatted as `key_id:secret`. Rate limiting is
communicated via `X-RateLimit-Remaining` / `X-RateLimit-Reset` response
headers on `429` responses, which the client respects directly instead of
sleeping a fixed guess.

---

## Technologies Used

| Layer | Tool |
|---|---|
| Language | Python 3.10+ |
| Data manipulation | pandas |
| Data warehouse | Google BigQuery |
| Source API | Playvox REST API v1 |
| HTTP client | requests (custom retry/backoff/rate-limit logic) |
| Date parsing | python-dateutil |
| Configuration | python-dotenv + environment variables |
| Auth | GCP Service Account (BigQuery), Playvox API key |

---

## Installation

```bash
git clone https://github.com/<your-username>/playvox-bigquery-pipeline.git
cd playvox-bigquery-pipeline

python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

pip install -r requirements.txt
```

## Configuration

1. Copy the example environment file:

   ```bash
   cp .env.example .env
   ```

2. Populate `.env`:

   | Variable | Description |
   |---|---|
   | `PLAYVOX_DOMAIN` | Your Playvox API host, e.g. `yourcompany.playvox.eu` |
   | `PLAYVOX_API_KEY` | Playvox API key, formatted `key_id:secret` (Settings → API) |
   | `BQ_PROJECT_ID` | Your GCP project ID |
   | `BQ_DATASET_ID` | BigQuery dataset name (defaults to `playvox`) |
   | `BQ_*_TABLE` | Per-table name overrides (optional) |
   | `GOOGLE_APPLICATION_CREDENTIALS` | Path to a GCP service account JSON key with BigQuery Data Editor + Job User roles |
   | `INCREMENTAL_WINDOW_DAYS` | How many days back the incremental pipelines sync (default `3`) |

3. Create the target BigQuery dataset if it doesn't exist yet:

   ```bash
   bq mk --dataset "$BQ_PROJECT_ID:$BQ_DATASET_ID"
   ```

   Tables are created automatically on first load.

## Running the Project

```bash
# Run every pipeline, in dependency order (users/teams before coachings/evaluations)
python main.py

# Run a specific subset
python main.py users teams
python main.py evaluations
```

### Example workflow

```bash
# crontab: sync the low-volume/base tables daily, refresh the QA fact
# tables (evaluations, coachings) a few times a day
0 2 * * *    cd /path/to/project && venv/bin/python main.py users teams roles         >> logs/daily.log 2>&1
0 */6 * * *  cd /path/to/project && venv/bin/python main.py interactions scorecards calibrations coachings evaluations >> logs/refresh.log 2>&1
```

---

## Project Structure

```
.
├── main.py                       # CLI entrypoint - dependency-ordered pipeline runner
├── config.py                     # Environment-driven configuration
├── requirements.txt
├── .env.example
├── src/
│   ├── playvox_client.py         # Extract layer: pagination, retries, rate limiting
│   ├── bigquery_loader.py        # Load layer: merge & full-refresh JSON strategies
│   ├── pipelines/
│   │   ├── users_pipeline.py
│   │   ├── teams_pipeline.py
│   │   ├── roles_pipeline.py
│   │   ├── interactions_pipeline.py
│   │   ├── scorecards_pipeline.py
│   │   ├── calibrations_pipeline.py
│   │   ├── coachings_pipeline.py
│   │   └── evaluations_pipeline.py
│   └── utils/
│       ├── logger.py
│       └── filters.py            # Shared "last N days" record filtering
└── docs/
    └── schema.md                  # Column-level BigQuery schema + entity relationships
```

---

## BigQuery Schema

See [`docs/schema.md`](docs/schema.md) for the full column-level schema
of every table, the load strategy used for each, and how the tables
relate to each other (it's effectively a small star schema: `evaluations`
and `coachings` as fact tables, `users`/`teams`/`scorecards` as
dimensions).

## API Resources Used

| Playvox Endpoint | Pipeline |
|---|---|
| `GET /users` | `users_pipeline.py` |
| `GET /teams` | `teams_pipeline.py` |
| `GET /roles` | `roles_pipeline.py` |
| `GET /interactions` | `interactions_pipeline.py` |
| `GET /scorecards` | `scorecards_pipeline.py` |
| `GET /calibrations` | `calibrations_pipeline.py` |
| `GET /coachings` | `coachings_pipeline.py` |
| `GET /evaluations` | `evaluations_pipeline.py` |

---

## Future Improvements

- **Data validation** (e.g. `pandera`) before load, to catch schema drift
  or unexpected nulls in required fields like `agent_id` or `score`.
- **Orchestration** (Airflow/Dagster) to express the users/teams →
  coachings/evaluations dependency explicitly instead of relying on
  argument order in `main.py`.
- **dbt models** on top of the raw tables for QA trend and coaching-
  effectiveness reporting, keeping SQL transformation logic out of Python.
- **Unit tests** for each pipeline's `transform()` function — already
  isolated from I/O specifically to make this straightforward.
- **True incremental extraction via API filters** (if Playvox exposes an
  `updated_since` parameter) instead of fetching every page and filtering
  client-side, which gets more expensive as historical volume grows.
- **Containerization** (Dockerfile + Cloud Run job) for portable,
  schedulable deployment.

---

## License

Released under the [MIT License](LICENSE).
