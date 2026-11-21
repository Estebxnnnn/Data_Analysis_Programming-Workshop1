"""Data-lake layout and file nomenclature.

Bronze : datalake_bronze/<source>_YYYYMMDD_HHMMSS.json        (raw, one file per source per run)
         datalake_bronze/_pending/bronze_run_YYYYMMDD_HHMMSS.json   (run manifest, written LAST;
                                                                     watched by the Silver FileSensor)
         datalake_bronze/_claimed/<silver_ts>/bronze_run_*.json     (manifest claimed by a Silver run)
Silver : datalake_silver/tropical_cyclones_YYYYMMDD_HHMMSS.parquet (timestamp = Silver processing run)
         datalake_silver/_rejected/tropical_cyclones_rejected_YYYYMMDD_HHMMSS.json
         datalake_silver/_manifest.jsonl                           (lineage: Silver file -> Bronze files)
         datalake_bronze/_state/      conditional-GET validators and per-source ingestion log
         datalake_bronze/_errors/     error bodies of failed requests (evidence, moved out of _staging)
         datalake_bronze/_reference/  raw IBTrACS since1980 CSV (git-ignored, 145 MB)
Silver : datalake_silver/_reference/ibtracs_since1980.parquet   historical baseline for analogs
         datalake_silver/_reference/reference_stats.json          climatological outlier fences
Gold   : datalake_gold/governance_YYYYMMDD_HHMMSS.parquet
         datalake_gold/storytelling_YYYYMMDD_HHMMSS.parquet

Folders whose name starts with "_" are ignored by spark.read.parquet("datalake_silver/").

All timestamps in file names are UTC. Scheduled runs use the END of the data
interval (the moment the run actually starts), manual runs the trigger time, so a
file name never predates the data inside it (Workshop 3 fix). Re-running the
same run rewrites the same file (idempotency).
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

TS_FORMAT = "%Y%m%d_%H%M%S"
SILVER_TOPIC = "tropical_cyclones"


def datalake_root() -> Path:
    """Root that holds the datalake_* folders (/opt/airflow inside Docker)."""
    env = os.getenv("TCMON_DATALAKE_ROOT")
    return Path(env) if env else Path(__file__).resolve().parents[3]


def bronze_dir() -> Path:
    return datalake_root() / "datalake_bronze"


def silver_dir() -> Path:
    return datalake_root() / "datalake_silver"


def gold_dir() -> Path:
    return datalake_root() / "datalake_gold"


def pending_dir() -> Path:
    return bronze_dir() / "_pending"


def claimed_dir() -> Path:
    return bronze_dir() / "_claimed"


def staging_dir() -> Path:
    return bronze_dir() / "_staging"


def state_dir() -> Path:
    return bronze_dir() / "_state"


def errors_dir() -> Path:
    return bronze_dir() / "_errors"


def bronze_reference_dir() -> Path:
    return bronze_dir() / "_reference"


def reference_dir() -> Path:
    return silver_dir() / "_reference"


def run_moment(context) -> datetime:
    """Moment that names the files of an Airflow run.

    Workshop 2 used logical_date, which for a scheduled run is the START of the data
    interval: the hourly Bronze run that started at 23:53 was named 220000 and a
    Silver file could carry an older timestamp than its Bronze input. Scheduled runs
    now use data_interval_end; manual runs keep logical_date (= trigger time)."""
    dag_run = context.get("dag_run")
    run_type = getattr(getattr(dag_run, "run_type", None), "value", getattr(dag_run, "run_type", None))
    if run_type == "scheduled" and context.get("data_interval_end") is not None:
        return context["data_interval_end"]
    return context["logical_date"]


def ts_string(moment: datetime) -> str:
    """UTC timestamp used in every file name."""
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc).strftime(TS_FORMAT)


def bronze_file(source: str, run_ts: str) -> Path:
    return bronze_dir() / f"{source}_{run_ts}.json"


def bronze_manifest(run_ts: str) -> Path:
    return pending_dir() / f"bronze_run_{run_ts}.json"


def silver_file(run_ts: str) -> Path:
    return silver_dir() / f"{SILVER_TOPIC}_{run_ts}.parquet"


def rejected_file(run_ts: str) -> Path:
    return silver_dir() / "_rejected" / f"{SILVER_TOPIC}_rejected_{run_ts}.json"


def gold_file(kind: str, run_ts: str) -> Path:
    """kind = governance | storytelling"""
    return gold_dir() / f"{kind}_{run_ts}.parquet"


def ensure_layout() -> None:
    for d in (bronze_dir(), silver_dir(), gold_dir(), pending_dir(), claimed_dir(), staging_dir(),
              silver_dir() / "_rejected", state_dir(), errors_dir(), reference_dir()):
        d.mkdir(parents=True, exist_ok=True)


def atomic_write_bytes(path: Path, data: bytes) -> Path:
    """Write to a temporary file in the same folder, then rename: readers never see half a file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    return path


def atomic_write_json(path: Path, obj) -> Path:
    return atomic_write_bytes(path, json.dumps(obj, indent=1, ensure_ascii=False, default=str).encode("utf-8"))


def read_json(path: Path, default=None):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def append_jsonl(path: Path, record: dict) -> None:
    """Append one line (single small write; each source has its own log file)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, default=str) + "\n")


def read_jsonl(path: Path) -> list[dict]:
    if not Path(path).exists():
        return []
    out = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if line.strip():
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out
