# Workshop 3 — Silver-to-Gold Pipeline, Governance KPIs and Storytelling

| Deliverable | Location |
| --- | --- |
| Report (IEEE format, PDF) | [`workshop_3/report/group4_workshop3.pdf`](report/group4_workshop3.pdf) (LaTeX sources in `report/latex/`) |
| Data-quality findings and improvements (before/after) | [`data_quality_findings.md`](data_quality_findings.md) |
| Gold specification: KPI catalogue, storytelling sections ↔ user stories, SAS | [`gold_specification.md`](gold_specification.md) |
| Gold DAG (PySpark, local mode) | [`airflow/dags/gold_processing_dag.py`](../airflow/dags/gold_processing_dag.py) |
| Gold modules | [`src/tcmon/gold/`](../src/tcmon/gold/) (`spark.py`, `inventory.py`, `governance.py`, `analogs.py`, `storytelling.py`) |
| Historical reference builder | [`src/tcmon/pipeline/reference.py`](../src/tcmon/pipeline/reference.py) |
| Improved Bronze/Silver code | `src/tcmon/http_client.py`, `src/tcmon/pipeline/{bronze,silver,paths}.py`, both DAGs |
| Updated image | [`airflow/Dockerfile`](../airflow/Dockerfile) (OpenJDK 17 + PySpark), [`docker-compose.yml`](../docker-compose.yml) (Spark settings) |
| Data-quality notebook (Pandas) | [`notebooks/03_data_quality_findings.ipynb`](../notebooks/03_data_quality_findings.ipynb) |
| Gold preview notebook | [`notebooks/04_gold_preview.ipynb`](../notebooks/04_gold_preview.ipynb) |
| Sample Gold files | `datalake_gold/governance_20261121_040036.parquet`, `datalake_gold/storytelling_20261121_040036.parquet` |
| Tests | `tests/test_workshop3.py` (offline), `tests/test_gold_spark.py` (PySpark, in the container) |
| Evidence | `workshop_3/evidence/`: `http/` (GDACS HTTP 400 body), `screenshots/` (Airflow and Docker), `logs/` (Spark memory error, narratives, tests) |

## Run it (Windows `cmd`, repository folder, Docker Desktop running)

```bat
git checkout workshop-3
docker compose down
docker compose build airflow-init
docker compose up airflow-init
docker compose up -d
docker compose ps
```

The image now contains Java 17 and PySpark (first build ≈ 10 min, ≈ 1 GB more). Then, in the Airflow UI
(<http://localhost:8080>, `airflow` / `airflow`): unpause `bronze_ingestion`, `silver_processing` and
`gold_processing`.

**Trigger Gold by hand:** UI → `gold_processing` → ▶ *Trigger DAG*, or

```bat
docker compose exec airflow-scheduler airflow dags trigger gold_processing
```

The first Gold run downloads IBTrACS since1980 (≈ 145 MB, ≈ 2 min) and builds the reference (≈ 1–2 min). To build
it before the first run: `docker compose exec airflow-scheduler python -m tcmon.pipeline.reference`.

**Tests and notebooks inside the container**

```bat
docker compose exec airflow-scheduler python -m pytest -q /opt/airflow/tests
docker compose exec airflow-scheduler jupyter nbconvert --to notebook --execute --inplace /opt/airflow/notebooks/03_data_quality_findings.ipynb
docker compose exec airflow-scheduler jupyter nbconvert --to notebook --execute --inplace /opt/airflow/notebooks/04_gold_preview.ipynb
```

## Branching
Workshop 3 is developed on the branch `workshop-3` and merged into `main` before submission, so that `main`
always holds the last delivered and reviewed workshop.
