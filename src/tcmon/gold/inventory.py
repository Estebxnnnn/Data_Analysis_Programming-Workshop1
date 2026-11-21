"""Metadata of the data lake used by the governance KPIs.

These are small inventories (one row per file or per run), read with the standard
library and pyarrow footers; the aggregation itself is done in Spark
(governance.py). Nothing here reads the data records of Bronze files.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

from ..pipeline import bronze, paths

BRONZE_NAME = re.compile(r"^(?P<source>[a-z0-9_]+?)_(?P<ts>\d{8}_\d{6})\.json$")
SILVER_NAME = re.compile(rf"^{paths.SILVER_TOPIC}_(?P<ts>\d{{8}}_\d{{6}})\.parquet$")
ERROR_NAME = re.compile(r"^(?P<source>[a-z0-9_]+?)_(?P<ts>\d{8}_\d{6})(_p\d+)?\.\w+\.http(?P<status>\d{3})$")


def ts_to_dt(ts: str | None) -> datetime | None:
    if not ts:
        return None
    return datetime.strptime(ts, paths.TS_FORMAT).replace(tzinfo=timezone.utc)


def iso_to_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def bronze_inventory() -> list[dict]:
    """One row per stored Bronze file (envelope header only)."""
    rows = []
    for f in sorted(paths.bronze_dir().glob("*.json")):
        m = BRONZE_NAME.match(f.name)
        if not m:
            continue
        env = json.loads(f.read_text(encoding="utf-8"))
        http = env.get("http") or []
        rows.append({
            "source": env.get("source", m["source"]), "run_ts": m["ts"], "file": f.name,
            "run_time_utc": ts_to_dt(m["ts"]), "ingested_at_utc": iso_to_dt(env.get("ingested_at_utc")),
            "record_count": int(env.get("record_count") or 0), "file_bytes": f.stat().st_size,
            "download_bytes": int(sum(h.get("size_bytes") or 0 for h in http)),
            "elapsed_s": float(sum(h.get("elapsed_s") or 0 for h in http)), "requests": len(http),
            "sha256": ";".join(h.get("sha256") or "" for h in http),
            "etag": (http[0].get("headers") or {}).get("ETag") if http else None,
            "last_modified": (http[0].get("headers") or {}).get("Last-Modified") if http else None,
            "name_ok": True,
        })
    return rows


def attempts() -> list[dict]:
    """Every extraction attempt: stored files, 'not modified' answers and failures.

    Source: _state/ingestion_log_<source>.jsonl (Workshop 3 onwards) plus, for older
    runs, the stored Bronze files and the error bodies left in _staging/ or _errors/."""
    seen, rows = set(), []
    for src in (bronze.GDACS_SOURCE, bronze.IBTRACS_SOURCE):
        for r in paths.read_jsonl(bronze.ingestion_log(src)):
            key = (r["source"], r["run_ts"], r["outcome"])
            seen.add(key)
            rows.append({"source": r["source"], "run_ts": r["run_ts"], "run_time_utc": ts_to_dt(r["run_ts"]),
                         "outcome": r["outcome"], "bytes": int(r.get("bytes") or 0),
                         "elapsed_s": float(r.get("elapsed_s") or 0),
                         "http_status": ",".join(str(s) for s in r.get("http_status") or []),
                         "origin": "ingestion_log"})
    for b in bronze_inventory():
        if (b["source"], b["run_ts"], "stored") not in seen:
            rows.append({"source": b["source"], "run_ts": b["run_ts"], "run_time_utc": b["run_time_utc"],
                         "outcome": "stored", "bytes": b["download_bytes"], "elapsed_s": b["elapsed_s"],
                         "http_status": "200", "origin": "bronze_file"})
    for folder in (paths.errors_dir(), paths.staging_dir()):
        for f in sorted(folder.glob("*.http*")):
            m = ERROR_NAME.match(f.name)
            if not m or f.name.endswith(".meta.json"):
                continue
            if (m["source"], m["ts"], "failed") in seen:
                continue
            seen.add((m["source"], m["ts"], "failed"))
            meta = paths.read_json(Path(str(f) + ".meta.json"), {}) or {}
            rows.append({"source": m["source"], "run_ts": m["ts"], "run_time_utc": ts_to_dt(m["ts"]),
                         "outcome": "failed", "bytes": f.stat().st_size, "elapsed_s": float(meta.get("elapsed_s") or 0),
                         "http_status": m["status"], "origin": f"{folder.name}/"})
    return rows


def silver_manifest() -> list[dict]:
    rows = []
    for r in paths.read_jsonl(paths.silver_dir() / "_manifest.jsonl"):
        d = r.get("dedup") or {}
        bronze_ts = [BRONZE_NAME.match(b)["ts"] for b in r.get("bronze_files", []) if BRONZE_NAME.match(b)]
        rows.append({
            "silver_run_ts": r["silver_run_ts"], "silver_file": r.get("silver_file"),
            "run_time_utc": ts_to_dt(r["silver_run_ts"]), "processed_at_utc": iso_to_dt(r.get("processed_at_utc")),
            "bronze_files": len(r.get("bronze_files", [])),
            "newest_bronze_time_utc": ts_to_dt(max(bronze_ts)) if bronze_ts else None,
            "rows": int(r.get("rows") or 0), "rejected": int(r.get("rejected") or 0),
            "input": int(d.get("input") or 0), "within_batch_duplicates": int(d.get("within_batch_duplicates") or 0),
            "cross_batch_duplicates": int(d.get("cross_batch_duplicates") or 0),
            "revisions": int(d.get("revisions") or 0),
            "fence_basis": (r.get("outlier_fences") or {}).get("basis", "batch (Workshop 2)"),
        })
    return rows


def bronze_ingested_at() -> dict[str, datetime]:
    return {b["file"]: b["ingested_at_utc"] for b in bronze_inventory()}


def silver_lineage() -> list[dict]:
    """(silver_run_ts, bronze file, bronze ingested_at) pairs for latency KPIs."""
    ing = bronze_ingested_at()
    out = []
    for r in paths.read_jsonl(paths.silver_dir() / "_manifest.jsonl"):
        for b in r.get("bronze_files", []):
            out.append({"silver_run_ts": r["silver_run_ts"], "bronze_file": b,
                        "source": BRONZE_NAME.match(b)["source"] if BRONZE_NAME.match(b) else None,
                        "bronze_ingested_at_utc": ing.get(b),
                        "processed_at_utc": iso_to_dt(r.get("processed_at_utc"))})
    return out


def silver_file_checks() -> list[dict]:
    """Schema conformity of every Silver file, from the Parquet footer (no data read)."""
    import pyarrow.parquet as pq
    from ..pipeline.schema import arrow_schema

    expected = arrow_schema()
    rows = []
    for f in sorted(paths.silver_dir().glob("*.parquet")):
        pf = pq.ParquetFile(f)
        sch = pf.schema_arrow
        same_names = sch.names == expected.names
        same_types = same_names and all(sch.field(n).type == expected.field(n).type for n in expected.names)
        rows.append({"file": f.name, "name_ok": bool(SILVER_NAME.match(f.name)), "rows": pf.metadata.num_rows,
                     "columns": len(sch.names), "schema_ok": bool(same_names and same_types),
                     "has_lineage": b"tcmon_lineage" in (sch.metadata or {}), "file_bytes": f.stat().st_size})
    return rows
