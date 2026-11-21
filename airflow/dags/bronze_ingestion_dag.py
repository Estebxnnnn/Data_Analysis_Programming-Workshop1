"""
### Bronze ingestion DAG - TC-Analog Monitor (Team 4)

Extracts the two incremental tropical-cyclone sources validated in Workshop 1 and
persists every run as raw JSON in `datalake_bronze/`:

| Task | What it does |
| --- | --- |
| `run_timestamp` | Run timestamp (UTC): end of the data interval for scheduled runs, trigger time for manual runs |
| `extract_api_gdacs` | GDACS event API (REST/GeoJSON), tropical cyclones of the last 7 days, all pages |
| `extract_file_ibtracs` | IBTrACS v04r01 ACTIVE subset (CSV file over HTTPS), conditional GET with stored ETag/Last-Modified |
| `validate_*` | Basic schema check; an invalid batch is never persisted |
| `persist_*` | `gdacs_tc_YYYYMMDD_HHMMSS.json`, `ibtracs_active_YYYYMMDD_HHMMSS.json` (IBTrACS only when the file changed) |
| `signal_silver` | Writes `_pending/bronze_run_YYYYMMDD_HHMMSS.json` LAST; the Silver FileSensor watches it |

Schedule: hourly (GDACS polling period of the Workshop 1 ingestion contract).
Idempotency: file names come from the run timestamp, so re-running a run rewrites its own files.
Every extraction attempt is logged in `datalake_bronze/_state/ingestion_log_<source>.jsonl`
(input of the Gold governance KPIs).
"""
from __future__ import annotations

import logging
import sys
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, "/opt/airflow/src")
from tcmon.pipeline import bronze, paths  # noqa: E402

log = logging.getLogger("airflow.task")

DEFAULT_ARGS = {
    "owner": "team4",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=10),
}


@dag(
    dag_id="bronze_ingestion",
    description="Extract GDACS (API) and IBTrACS ACTIVE (file) into datalake_bronze/ as raw JSON",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "ingestion", "tropical-cyclones"],
    doc_md=__doc__,
)
def bronze_ingestion():

    @task
    def run_timestamp() -> str:
        ctx = get_current_context()
        ts = paths.ts_string(paths.run_moment(ctx))
        log.info("Bronze run timestamp: %s (run type %s, logical date %s)", ts,
                 ctx["dag_run"].run_type, ctx["logical_date"])
        return ts

    @task
    def extract_api_gdacs(run_ts: str) -> dict:
        day = paths.run_moment(get_current_context()).date()
        try:
            return bronze.extract_gdacs(run_ts, day)
        except Exception:
            log.exception("GDACS extraction failed")
            raise

    @task
    def extract_file_ibtracs(run_ts: str) -> dict:
        try:
            return bronze.extract_ibtracs_active(run_ts)
        except Exception:
            log.exception("IBTrACS ACTIVE extraction failed")
            raise

    @task
    def validate_gdacs(extracted: dict) -> dict:
        return {"extracted": extracted, "report": bronze.validate(extracted)}

    @task
    def validate_ibtracs(extracted: dict) -> dict:
        return {"extracted": extracted, "report": bronze.validate(extracted)}

    @task
    def persist_gdacs(validated: dict, run_ts: str) -> str:
        return bronze.persist(validated["extracted"], validated["report"], run_ts)

    @task
    def persist_ibtracs(validated: dict, run_ts: str) -> str:
        return bronze.persist(validated["extracted"], validated["report"], run_ts)

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def signal_silver(run_ts: str) -> str | None:
        """Runs even if one source failed: the other source still reaches Silver,
        and the run is then marked failed so the problem stays visible."""
        written = sorted(p.name for p in paths.bronze_dir().glob(f"*_{run_ts}.json"))
        manifest = bronze.signal_silver(run_ts, written)
        expected = bronze.expected_files(run_ts)       # IBTrACS is not expected when it was not modified
        if set(written) != expected:
            raise AirflowException(f"Incomplete Bronze run {run_ts}: written={written}")
        return manifest

    ts = run_timestamp()
    gdacs_file = persist_gdacs(validate_gdacs(extract_api_gdacs(ts)), ts)
    ibtracs_file = persist_ibtracs(validate_ibtracs(extract_file_ibtracs(ts)), ts)
    done = signal_silver(ts)
    [gdacs_file, ibtracs_file] >> done


bronze_ingestion()
