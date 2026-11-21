# TC-Analog Monitor

**Multi-source ingestion and analysis system for global tropical-cyclone monitoring.**
Data Analysis Programming · Semester 2026-II · Universidad Distrital Francisco José de Caldas ·
Instructor: Eng. Carlos Andrés Sierra, M.Sc.

## Hazard
Tropical cyclones (tropical depressions, tropical storms, hurricanes/typhoons/cyclones), **global scope**,
historical window **1980–present**. Severity = maximum sustained wind in **knots** (secondary: minimum central
pressure in hPa). Team output: **comparison of the current storm with similar historical storms** (Storm Analog
Similarity, SAS). The system monitors and compares; it does **not** forecast.

## Team 4
| Student ID | Name |
| --- | --- |
| 20222020108 | Omar Peña |
| 20222020134 | Rosa López |
| 20222020139 | Brayan Ramírez |

## Sources
| ID | Role | Source | Format | Watermark |
| --- | --- | --- | --- | --- |
| S1 | Historical | [IBTrACS v04r01](https://www.ncei.noaa.gov/node/1660) `since1980` (NOAA NCEI) | CSV | ETag / Last-Modified |
| S2 | Historical | GDACS tropical-cyclone events, archive 2015–present (`gdacsapi` SEARCH) | GeoJSON | last completed month |
| S3 | Incremental | IBTrACS v04r01 `ACTIVE` (storms active in the last 7 days) | CSV | ETag / Last-Modified |
| S4 | Incremental | GDACS tropical-cyclone events, last 7 days | GeoJSON | `datemodified` |
| S5 | Optional | NHC [`CurrentStorms.json`](https://www.nhc.noaa.gov/CurrentStorms.json) (Atlantic, E/C Pacific) | JSON | `lastUpdate` |

Full details, licenses and attribution: [`config/sources.yaml`](config/sources.yaml).

**Attribution.** IBTrACS: Knapp et al. (2010), *BAMS* 91, 363–376, doi:10.1175/2009BAMS2755.1; Gahtan et al. (2024),
IBTrACS v04r01, NOAA NCEI, doi:10.25921/82ty-9e16. GDACS: Global Disaster Alert and Coordination System, a cooperation
framework of the UN and the European Commission. NHC: NOAA National Hurricane Center.

## Pipeline (Workshops 2–3)
Medallion data lake orchestrated by **Apache Airflow 2.10.5** in Docker Compose (LocalExecutor, PostgreSQL metadata DB):

| Layer | Folder | Produced by | Format |
| --- | --- | --- | --- |
| Bronze | `datalake_bronze/` | `bronze_ingestion` DAG (hourly, minute 0): GDACS API (7-day window) + IBTrACS ACTIVE file (conditional GET) | raw JSON, `<source>_YYYYMMDD_HHMMSS.json` |
| Silver | `datalake_silver/` | `silver_processing` DAG (every 15 min, FileSensor on Bronze): Pandas cleaning, dedup, schema | Parquet, `tropical_cyclones_YYYYMMDD_HHMMSS.parquet` |
| Gold | `datalake_gold/` | `gold_processing` DAG (hourly, minute 40): **PySpark local mode** over `datalake_silver/` | Parquet, `governance_YYYYMMDD_HHMMSS.parquet`, `storytelling_YYYYMMDD_HHMMSS.parquet` |

* **Governance** — over 100 KPI values per run on completeness, uniqueness, validity, conformity, timeliness,
  consistency, efficiency and volume, each with target and status.
* **Storytelling** — every current storm, its **top-5 historical analogs** from IBTrACS since 1980 (Storm Analog
  Similarity), what those analogs did in the next 72 h, alerts, source comparison, basin climatology and a narrative.
  Analogs describe resemblance; they are not a forecast.

```bat
docker compose build airflow-init
docker compose up airflow-init
docker compose up -d
```
Airflow UI: <http://localhost:8080> (`airflow` / `airflow`). Unpause the three DAGs. **Trigger Gold manually:**
UI → `gold_processing` → ▶ *Trigger DAG*, or `docker compose exec airflow-scheduler airflow dags trigger gold_processing`.
Details: [`workshop_2/README.md`](workshop_2/README.md) (stack) and [`workshop_3/README.md`](workshop_3/README.md) (Gold).

## Repository structure
```
.
├── docker-compose.yml        # Airflow (webserver, scheduler, triggerer) + PostgreSQL metadata DB
├── airflow/                  # Dockerfile, requirements-airflow.txt, dags/, logs/, plugins/
├── datalake_bronze/          # raw JSON per source and run (+ _pending/_claimed manifests, _state/, _errors/)
├── datalake_silver/          # Parquet per Silver run (+ _manifest.jsonl, _rejected/, _reference/ 1980- baseline)
├── datalake_gold/            # governance_*.parquet and storytelling_*.parquet (PySpark)
├── ingest.py                 # CLI entry point (stub): --mode historical | incremental
├── config/sources.yaml       # one entry per source: URL, format, schedule, watermark, license
├── src/tcmon/                # ingestion package
│   ├── http_client.py        # polite client: UA, throttling, retries, conditional GET, raw + .meta.json
│   ├── sources/              # ibtracs.py, gdacs.py, nhc.py
│   ├── normalization/        # units.py, canonical.py (canonical event model)
│   ├── pipeline/             # bronze.py, silver.py, schema.py, paths.py, reference.py
│   ├── gold/                 # spark.py, inventory.py, governance.py, analogs.py, storytelling.py (Workshop 3)
│   └── storage/              # landing zone paths (DB writer in Workshop 2)
├── data/raw/                 # raw landing zone (git-ignored)
├── sql/001_schema_draft.sql  # schema draft (common + domain tables)
├── dashboard/app.py          # Plotly Dash stub (localhost:8050)
├── tests/                    # pytest; tests/fixtures/ = recorded responses
├── notebooks/                # 01 source profiling, 02 Silver, 03 data-quality findings, 04 Gold preview
├── docs/architecture.md      # architecture overview + diagram
├── docs/report/              # Workshop 1 report: IEEE-format PDF + LaTeX source
├── workshop_1/               # mapping, ER diagram, ingestion contract, samples, evidence, tools
├── workshop_2/               # report, preprocessing specification, evidence
├── workshop_3/               # report, data-quality findings, Gold specification, evidence
├── .env.example
└── requirements.txt
```

## Quick start
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env                     # set TCMON_USER_AGENT with a contact e-mail
python -m pytest -q                      # 28 tests, offline (incl. recorded real responses)
python ingest.py --list-sources
python ingest.py --mode historical --dry-run
python workshop_1/tools/collect_samples.py   # real queries -> workshop_1/samples, data/raw
jupyter notebook notebooks/01_source_profiling.ipynb
python dashboard/app.py                  # http://localhost:8050 (stub)
```

## Status
| Workshop | Scope | Status |
| --- | --- | --- |
| 1 | Hazard definition, four sources validated, profiling, canonical mapping, ingestion contract, project definition | done — evidence captured 14 Nov 2026 (`workshop_1/evidence/`) |
| 2 | Docker + Airflow stack, Bronze ingestion DAG, Silver preprocessing DAG with FileSensor | done |
| 3 | Pipeline improvements from real findings, PySpark Gold layer, governance KPIs, storytelling with historical analogs | done (branch `workshop-3`) |
| 4 | Temporal and geographic analysis, SAS indicator, dashboard | planned |
