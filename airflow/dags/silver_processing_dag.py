"""
### Silver processing DAG - TC-Analog Monitor (Team 4)

A **FileSensor** watches `datalake_bronze/_pending/` for the run manifest that the
Bronze DAG writes after both raw JSON files are complete. When one arrives, the
batch is claimed, cleaned with Pandas and written as
`datalake_silver/tropical_cyclones_YYYYMMDD_HHMMSS.parquet` (timestamp of this
processing run). A batch with no new or changed record writes no file; the
run is still recorded in `datalake_silver/_manifest.jsonl` (Workshop 3).

| Task | What it does |
| --- | --- |
| `wait_for_bronze_files` | FileSensor, `poke_interval` 60 s, `timeout` 14 min, `mode="reschedule"`, `soft_fail=True` |
| `claim_bronze_batch` | Moves the pending manifest(s) to `_claimed/<run_ts>/` (re-runs reuse them); the Silver file takes the later of the run timestamp and the newest claimed Bronze run |
| `preprocess_to_parquet` | Conform, types, nulls, text, outliers, dedup, schema enforcement, Parquet + lineage |
| `verify_parquet` | Reads the file back with Pandas and checks schema and row count |

Schedule: every 15 minutes. Inside each run the sensor polls every minute, so a
Bronze batch reaches Silver in at most ~15 minutes, well within the hourly
ingestion period. Runs with no new Bronze file end as *skipped*, not failed.
"""
from __future__ import annotations

import logging
import sys
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import get_current_context
from airflow.sensors.filesystem import FileSensor

sys.path.insert(0, "/opt/airflow/src")
from tcmon.pipeline import paths, silver  # noqa: E402
from tcmon.pipeline.schema import COLUMNS  # noqa: E402

log = logging.getLogger("airflow.task")

DEFAULT_ARGS = {
    "owner": "team4",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),
}


@dag(
    dag_id="silver_processing",
    description="FileSensor on datalake_bronze/ -> preprocessing -> Parquet in datalake_silver/",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["silver", "preprocessing", "parquet", "tropical-cyclones"],
    doc_md=__doc__,
)
def silver_processing():

    wait_for_bronze_files = FileSensor(
        task_id="wait_for_bronze_files",
        fs_conn_id="fs_datalake",                       # path = /opt/airflow (see docker-compose.yml)
        filepath="datalake_bronze/_pending/bronze_run_*.json",
        poke_interval=60,
        timeout=14 * 60,
        mode="reschedule",
        soft_fail=True,
    )

    @task
    def claim_bronze_batch() -> dict:
        run_ts = paths.ts_string(paths.run_moment(get_current_context()))
        manifests = silver.claim_batch(run_ts)
        files = silver.bronze_files_of(manifests)
        if not files:
            raise AirflowSkipException("No pending Bronze batch")
        # The FileSensor may wait up to 14 min, so the Bronze run it finds can be newer than
        # the Silver run itself. The Silver file is named with the later of the two moments,
        # so a file never carries a timestamp older than its input (KPI F5). Re-runs claim the
        # same manifests, hence the name stays deterministic.
        newest_bronze = max(m.stem.removeprefix("bronze_run_") for m in manifests)
        silver_ts = max(run_ts, newest_bronze)
        log.info("Silver run %s (file %s) will process %s", run_ts, silver_ts, files)
        return {"silver_ts": silver_ts, "bronze_files": files}

    @task
    def preprocess_to_parquet(batch: dict) -> dict:
        try:
            return silver.process(batch["bronze_files"], batch["silver_ts"])
        except Exception:
            log.exception("Silver preprocessing failed for %s", batch)
            raise

    @task
    def verify_parquet(summary: dict) -> dict:
        import pandas as pd
        if not summary.get("silver_file"):
            log.info("Silver run %s: no new or changed records, no Parquet file written (see _manifest.jsonl)",
                     summary["silver_run_ts"])
            return {"file": None, "rows": 0, "columns": len(COLUMNS)}
        path = paths.silver_dir() / summary["silver_file"]
        df = pd.read_parquet(path)
        if list(df.columns) != COLUMNS or len(df) != summary["rows"]:
            raise ValueError(f"{path.name}: unexpected schema or row count")
        log.info("Verified %s: %s rows x %s columns\n%s", path.name, len(df), len(df.columns),
                 df.head(10).to_string() if len(df) else "(no new or changed records)")
        return {"file": path.name, "rows": len(df), "columns": len(df.columns)}

    batch = claim_bronze_batch()
    wait_for_bronze_files >> batch
    verify_parquet(preprocess_to_parquet(batch))


silver_processing()
