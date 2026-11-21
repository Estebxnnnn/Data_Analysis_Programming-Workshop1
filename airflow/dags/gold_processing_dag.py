"""
### Gold processing DAG - TC-Analog Monitor (Team 4)

Reads the whole Silver layer with **PySpark in local mode**
(`spark.read.parquet("datalake_silver/")`) and writes two Gold tables:

| File | Content |
| --- | --- |
| `datalake_gold/governance_YYYYMMDD_HHMMSS.parquet` | Data-governance KPIs (completeness, uniqueness, validity, conformity, timeliness, consistency, efficiency, volume) |
| `datalake_gold/storytelling_YYYYMMDD_HHMMSS.parquet` | Current storms, their top-5 historical analogs (Storm Analog Similarity), what the analogs did next, alerts, source comparison, basin climatology and a narrative per storm |

| Task | What it does |
| --- | --- |
| `run_timestamp` | Gold run timestamp (UTC): end of the data interval for scheduled runs, trigger time for manual runs |
| `check_silver_inputs` | Lists the Silver Parquet files; skips the run if there is none yet |
| `ensure_reference` | IBTrACS since1980 baseline in `datalake_silver/_reference/` (conditional GET, rebuilt when NCEI publishes a new file, checked at most once a week) |
| `build_governance` | PySpark aggregation -> `governance_*.parquet` |
| `build_storytelling` | PySpark aggregation incl. the analog search -> `storytelling_*.parquet` |
| `verify_gold` | Reads both files back, checks schema and content, applies the retention rule |

Schedule: hourly at minute 40, after the hourly Bronze run (minute 0) has reached
Silver (FileSensor, at most ~30 min). Manual trigger: Airflow UI -> `gold_processing`
-> Trigger DAG, or `airflow dags trigger gold_processing` inside the scheduler container.
"""
from __future__ import annotations

import logging
import os
import sys
import time
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import get_current_context

sys.path.insert(0, "/opt/airflow/src")
from tcmon.pipeline import paths  # noqa: E402

log = logging.getLogger("airflow.task")

GOLD_RETENTION_DAYS = float(os.getenv("TCMON_GOLD_RETENTION_DAYS", "14"))

DEFAULT_ARGS = {
    "owner": "team4",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=20),
}


@dag(
    dag_id="gold_processing",
    description="PySpark (local mode): datalake_silver/ -> governance KPIs + storytelling (historical analogs) in datalake_gold/",
    schedule="40 * * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["gold", "pyspark", "governance", "storytelling", "tropical-cyclones"],
    doc_md=__doc__,
)
def gold_processing():

    @task
    def run_timestamp() -> str:
        ts = paths.ts_string(paths.run_moment(get_current_context()))
        log.info("Gold run timestamp: %s", ts)
        return ts

    @task
    def check_silver_inputs() -> dict:
        files = sorted(p.name for p in paths.silver_dir().glob(f"{paths.SILVER_TOPIC}_*.parquet"))
        if not files:
            raise AirflowSkipException("No Silver Parquet file yet: run bronze_ingestion and silver_processing first")
        log.info("%s Silver files available: %s ... %s", len(files), files[0], files[-1])
        return {"silver_files": len(files), "first": files[0], "last": files[-1]}

    @task(execution_timeout=timedelta(minutes=30), retries=0)
    def ensure_reference() -> dict:
        """The analogs need the 1980-2025 archive. A failure here must not stop the
        governance KPIs, so it is reported in the result instead of failing the run."""
        from tcmon.pipeline import reference
        try:
            info = reference.ensure()
            log.info("Reference: rows=%s storms=%s rebuilt=%s", info.get("rows"), info.get("storms"), info.get("rebuilt"))
            return {"available": True, "rows": info.get("rows"), "storms": info.get("storms"),
                    "rebuilt": info.get("rebuilt")}
        except Exception as exc:      # network or disk problem: storytelling runs without analogs
            log.exception("Reference archive unavailable")
            return {"available": reference.parquet_path().exists(), "error": f"{type(exc).__name__}: {exc}"[:300]}

    @task
    def build_governance(gold_ts: str, inputs: dict) -> dict:
        from tcmon.gold import governance
        from tcmon.gold.spark import get_spark, write_single_parquet
        t0 = time.monotonic()
        spark = get_spark("tcmon-gold-governance")
        try:
            sdf = governance.build(spark, gold_ts).cache()
            n = sdf.count()
            out = write_single_parquet(sdf, paths.gold_file("governance", gold_ts))
            status = {r["status"]: r["count"] for r in sdf.groupBy("status").count().collect()}
            log.info("Governance KPIs: %s rows, status %s\n%s", n, status,
                     sdf.filter("status in ('fail', 'warn')").select("kpi_id", "kpi_name", "scope_value", "value",
                                                                        "unit", "status").toPandas().to_string())
        finally:
            spark.stop()
        return {"file": out.name, "rows": n, "status": status, "seconds": round(time.monotonic() - t0, 1)}

    @task
    def build_storytelling(gold_ts: str, ref: dict) -> dict:
        from tcmon.gold import storytelling
        from tcmon.gold.spark import get_spark, write_single_parquet
        t0 = time.monotonic()
        spark = get_spark("tcmon-gold-storytelling")
        try:
            sdf = storytelling.build(spark, gold_ts, reference_available=bool(ref.get("available"))).cache()
            n = sdf.count()
            out = write_single_parquet(sdf, paths.gold_file("storytelling", gold_ts))
            per_section = {r["section"]: r["count"] for r in sdf.groupBy("section").count().collect()}
            for r in sdf.filter("section = 'narrative'").select("value_text").collect():
                log.info("NARRATIVE: %s", r["value_text"])
        finally:
            spark.stop()
        return {"file": out.name, "rows": n, "sections": per_section, "seconds": round(time.monotonic() - t0, 1)}

    @task
    def verify_gold(gov: dict, story: dict) -> dict:
        import pandas as pd

        from tcmon.gold.governance import KPI_COLUMNS
        from tcmon.gold.storytelling import STORY_COLUMNS
        checks = {}
        for info, cols in ((gov, KPI_COLUMNS), (story, STORY_COLUMNS)):
            df = pd.read_parquet(paths.gold_dir() / info["file"])
            missing = [c for c in cols + ["gold_run_ts", "computed_at_utc"] if c not in df.columns]
            if missing or len(df) != info["rows"] or df.empty:
                raise ValueError(f"{info['file']}: missing={missing} rows={len(df)} expected={info['rows']}")
            checks[info["file"]] = {"rows": len(df), "columns": len(df.columns)}
            log.info("Verified %s: %s rows x %s columns\n%s", info["file"], len(df), len(df.columns),
                     df.head(15).to_string())
        sections = set(story["sections"])
        for required in ("current_status", "intensity_track", "narrative"):
            if required not in sections:
                log.warning("Storytelling section %s is empty in this run", required)
        # retention: Gold files older than TCMON_GOLD_RETENTION_DAYS are removed
        cutoff = time.time() - GOLD_RETENTION_DAYS * 86400
        removed = [p.name for p in paths.gold_dir().glob("*_????????_??????.parquet") if p.stat().st_mtime < cutoff]
        for name in removed:
            (paths.gold_dir() / name).unlink(missing_ok=True)
        if removed:
            log.info("Retention: removed %s Gold files older than %s days", len(removed), GOLD_RETENTION_DAYS)
        return {"checks": checks, "removed": len(removed)}

    ts = run_timestamp()
    inputs = check_silver_inputs()
    ref = ensure_reference()
    inputs >> ref
    gov = build_governance(ts, inputs)
    story = build_storytelling(ts, ref)
    verify_gold(gov, story)


gold_processing()
