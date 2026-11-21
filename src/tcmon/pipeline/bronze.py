"""Bronze layer: extract, validate (basic schema check) and persist raw JSON.

Each source is first downloaded byte-for-byte into datalake_bronze/_staging/
(with the HTTP metadata of the request), then validated, then wrapped in a JSON
envelope and written as <source>_YYYYMMDD_HHMMSS.json. Records are kept as the
source published them: IBTrACS values stay text (including the single-space
blanks), GDACS features stay unchanged GeoJSON.

Workshop 3 changes (each one answers a finding in workshop_3/data_quality_findings.md):
* IBTrACS ACTIVE uses a conditional GET (If-None-Match / If-Modified-Since) with the
  validators kept in _state/ibtracs_active.json. When the file has not changed (HTTP
  304, or HTTP 200 with the same SHA-256), no Bronze file is written: in Workshop 2
  the same 570 KB file was stored every hour although NCEI updates it about three
  times a week.
* A GDACS "Execution Timeout" (HTTP 400) is retried by the HTTP client; if the source
  still fails, its error body is moved from _staging/ to _errors/ so that _staging/
  only ever holds files of a run in progress.
* Every extraction appends one line to _state/ingestion_log_<source>.jsonl (status,
  bytes, elapsed time, records, file). The Gold governance KPIs are computed from it.
"""
from __future__ import annotations

import csv
import io
import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from ..http_client import PoliteClient
from ..sources import gdacs, ibtracs
from . import paths

log = logging.getLogger(__name__)

IBTRACS_SOURCE = "ibtracs_active"
GDACS_SOURCE = "gdacs_tc"
GDACS_WINDOW_DAYS = 7          # same window as the IBTrACS ACTIVE subset
GDACS_MAX_PAGES = 10

IBTRACS_REQUIRED = ["SID", "SEASON", "BASIN", "NAME", "ISO_TIME", "LAT", "LON", "TRACK_TYPE",
                    "USA_WIND", "WMO_WIND", "USA_PRES", "WMO_PRES"]
GDACS_REQUIRED = ["eventtype", "eventid", "episodeid", "fromdate", "todate", "datemodified",
                  "alertlevel", "severitydata"]


def _client() -> PoliteClient:
    return PoliteClient(min_interval=1.0, timeout=90, max_retries=3)


# ------------------------------------------------------------------- state
def state_file(source: str) -> Path:
    return paths.state_dir() / f"{source}.json"


def ingestion_log(source: str) -> Path:
    return paths.state_dir() / f"ingestion_log_{source}.jsonl"


def log_attempt(source: str, run_ts: str, outcome: str, http: list[dict], records: int | None = None,
                bronze_file: str | None = None, error: str | None = None) -> None:
    """One line per extraction attempt; read by the Gold governance KPIs."""
    paths.append_jsonl(ingestion_log(source), {
        "source": source, "run_ts": run_ts, "outcome": outcome,
        "logged_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "http_status": [h.get("status") for h in http],
        "bytes": int(sum(h.get("size_bytes") or 0 for h in http)),
        "elapsed_s": round(float(sum(h.get("elapsed_s") or 0 for h in http)), 3),
        "requests": len(http), "records": records, "bronze_file": bronze_file, "error": error,
    })


def _quarantine(staged_or_error: list[str | None]) -> list[str]:
    """Move error bodies (and their .meta.json) from _staging/ to _errors/."""
    moved = []
    for p in staged_or_error:
        if not p:
            continue
        for f in (Path(p), Path(str(p) + ".meta.json")):
            if f.exists():
                target = paths.errors_dir() / f.name
                f.replace(target)
                moved.append(target.name)
    return moved


def sweep_staging() -> list[str]:
    """Move error bodies left in _staging/ by earlier runs (e.g. by Workshop 2) to _errors/."""
    leftovers = [str(f) for f in paths.staging_dir().glob("*.http[0-9][0-9][0-9]")]
    return _quarantine(leftovers)


# ----------------------------------------------------------------- extract
def extract_ibtracs_active(run_ts: str) -> dict:
    """Conditional download of the IBTrACS ACTIVE CSV (file over HTTPS) into the staging area."""
    paths.ensure_layout()
    sweep_staging()
    state = paths.read_json(state_file(IBTRACS_SOURCE), {}) or {}
    dest = paths.staging_dir() / f"{IBTRACS_SOURCE}_{run_ts}.csv"
    res = _client().fetch_to_file(ibtracs.url_for("ACTIVE"), dest,
                                  etag=state.get("etag"), last_modified=state.get("last_modified"))
    http = [res.to_dict()]
    if res.status == 304 or (res.status == 200 and res.sha256 and res.sha256 == state.get("sha256")):
        reason = "HTTP 304" if res.status == 304 else "HTTP 200, same SHA-256 as the last stored file"
        for f in (Path(res.path or dest), Path(str(res.path or dest) + ".meta.json")):
            f.unlink(missing_ok=True)
        log.info("IBTrACS ACTIVE not modified since %s (%s); no Bronze file this run",
                 state.get("last_modified"), reason)
        return {"source": IBTRACS_SOURCE, "staged": [], "http": http, "not_modified": True,
                "unchanged_since": state.get("bronze_file")}
    if res.status != 200 or not res.path:
        moved = _quarantine([res.path])
        log_attempt(IBTRACS_SOURCE, run_ts, "failed", http, error=f"HTTP {res.status}; body in _errors/{moved}")
        raise RuntimeError(f"IBTrACS ACTIVE download failed: HTTP {res.status} ({res.url})")
    log.info("IBTrACS ACTIVE: HTTP %s, %s bytes, Last-Modified=%s, ETag=%s", res.status, res.size_bytes,
             res.headers.get("Last-Modified"), res.headers.get("ETag"))
    return {"source": IBTRACS_SOURCE, "staged": [res.path], "http": http, "not_modified": False}


def extract_gdacs(run_ts: str, run_day: date | None = None) -> dict:
    """Query the GDACS event API for tropical cyclones of the last 7 days (all pages)."""
    paths.ensure_layout()
    sweep_staging()
    run_day = run_day or datetime.now(timezone.utc).date()
    start = run_day - timedelta(days=GDACS_WINDOW_DAYS)
    staged, http = [], []
    for page in range(1, GDACS_MAX_PAGES + 1):
        dest = paths.staging_dir() / f"{GDACS_SOURCE}_{run_ts}_p{page:02d}.json"
        try:
            res = _client().fetch_to_file(gdacs.SEARCH_URL, dest, params=gdacs.search_params(start, run_day, page))
        except Exception as exc:                   # network error after all retries
            _quarantine(staged)
            log_attempt(GDACS_SOURCE, run_ts, "failed", http, error=f"{type(exc).__name__}: {exc}"[:500])
            raise
        http.append(res.to_dict())
        if res.status == 204:                      # documented end of results (Workshop 1)
            log.info("GDACS page %s: HTTP 204, end of results", page)
            for f in (Path(res.path or dest), Path(str(res.path or dest) + ".meta.json")):
                f.unlink(missing_ok=True)
            break
        if res.status != 200 or not res.path:
            moved = _quarantine([res.path] + staged)
            log_attempt(GDACS_SOURCE, run_ts, "failed", http,
                        error=f"HTTP {res.status} on page {page}; body in _errors/{moved}")
            raise RuntimeError(f"GDACS request failed: HTTP {res.status} ({res.url})")
        staged.append(res.path)
        n = len(gdacs.load_features(res.path))
        log.info("GDACS page %s: HTTP 200, %s features", page, n)
        if n < 100:                                # page size is 100: a short page is the last one
            break
    return {"source": GDACS_SOURCE, "staged": staged, "http": http,
            "window": {"fromdate": start.isoformat(), "todate": run_day.isoformat()}}


# ---------------------------------------------------------------- validate
def _ibtracs_rows(path: str) -> tuple[list[dict], dict | None, list[str]]:
    text = Path(path).read_text(encoding="utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    units = None
    if rows and not _looks_like_time(rows[0].get("ISO_TIME", "")):   # line 2 = units row (Workshop 1, DQ2)
        units, rows = rows[0], rows[1:]
    return rows, units, reader.fieldnames or []


def _looks_like_time(value: str) -> bool:
    try:
        datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S")
        return True
    except ValueError:
        return False


def validate(extracted: dict) -> dict:
    """Basic schema check. Raises ValueError so that an invalid batch is never persisted."""
    source = extracted["source"]
    report = {"source": source, "checked_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    if extracted.get("not_modified"):
        report.update(records=0, missing_fields=[], passed=True, skipped="source not modified")
        return report
    if source == IBTRACS_SOURCE:
        rows, units, header = _ibtracs_rows(extracted["staged"][0])
        missing = [c for c in IBTRACS_REQUIRED if c not in header]
        bad_time = sum(not _looks_like_time(r.get("ISO_TIME", "")) for r in rows)
        bad_pos = sum(not (r.get("LAT", "").strip() and r.get("LON", "").strip()) for r in rows)
        report.update(columns=len(header), records=len(rows), units_row=units is not None,
                      missing_fields=missing, invalid_time=bad_time, missing_position=bad_pos)
    else:
        feats = [f for p in extracted["staged"] for f in gdacs.load_features(p)]
        missing = sorted({k for f in feats for k in GDACS_REQUIRED if k not in f.get("properties", {})})
        not_point = sum((f.get("geometry") or {}).get("type") != "Point" for f in feats)
        report.update(records=len(feats), missing_fields=missing, non_point_geometry=not_point)
    report["passed"] = not report["missing_fields"]
    if not report["passed"]:
        raise ValueError(f"{source}: schema check failed, missing fields {report['missing_fields']}")
    if report["records"] == 0:
        log.warning("%s: valid response with 0 records (no active storms)", source)
    log.info("Validation %s: %s", source, report)
    return report


# ----------------------------------------------------------------- persist
def persist(extracted: dict, report: dict, run_ts: str) -> str:
    """Write <source>_YYYYMMDD_HHMMSS.json (atomic) and remove the staged files.

    Returns the file name, or "" when the source was not modified (nothing to store)."""
    source = extracted["source"]
    if extracted.get("not_modified"):
        log_attempt(source, run_ts, "not_modified", extracted["http"], records=0)
        state = paths.read_json(state_file(source), {}) or {}
        state.update(last_checked_run_ts=run_ts,
                     last_checked_at_utc=datetime.now(timezone.utc).isoformat(timespec="seconds"))
        paths.atomic_write_json(state_file(source), state)
        return ""
    if source == IBTRACS_SOURCE:
        rows, units, header = _ibtracs_rows(extracted["staged"][0])
        payload = {"format": "csv_rows", "columns": header, "units_row": units, "records": rows}
    else:
        feats = [f for p in extracted["staged"] for f in gdacs.load_features(p)]
        payload = {"format": "geojson_features", "window": extracted.get("window"), "records": feats}
    envelope = {
        "source": source,
        "run_ts": run_ts,
        "ingested_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "http": extracted["http"],
        "validation": report,
        "record_count": len(payload["records"]),
        **payload,
    }
    out = paths.atomic_write_json(paths.bronze_file(source, run_ts), envelope)
    for p in extracted["staged"]:
        for f in (Path(p), Path(str(p) + ".meta.json")):
            f.unlink(missing_ok=True)
    log_attempt(source, run_ts, "stored", extracted["http"], records=envelope["record_count"], bronze_file=out.name)
    if source == IBTRACS_SOURCE:                 # validators are saved only after the file is safely stored
        h = extracted["http"][0]
        paths.atomic_write_json(state_file(source), {
            "etag": h.get("headers", {}).get("ETag"), "last_modified": h.get("headers", {}).get("Last-Modified"),
            "sha256": h.get("sha256"), "bronze_file": out.name, "stored_run_ts": run_ts,
            "last_checked_run_ts": run_ts,
            "last_checked_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")})
    log.info("Bronze file written: %s (%s records)", out.name, envelope["record_count"])
    return out.name


def expected_files(run_ts: str) -> set[str]:
    """Bronze files a complete run must have: GDACS always, IBTrACS only if it changed."""
    expected = {f"{GDACS_SOURCE}_{run_ts}.json"}
    ib = [r for r in paths.read_jsonl(ingestion_log(IBTRACS_SOURCE)) if r.get("run_ts") == run_ts]
    if not (ib and ib[-1].get("outcome") == "not_modified"):
        expected.add(f"{IBTRACS_SOURCE}_{run_ts}.json")
    return expected


def signal_silver(run_ts: str, bronze_files: list[str]) -> str | None:
    """Write the run manifest LAST: its arrival tells the Silver FileSensor that the batch is complete."""
    files = [f for f in bronze_files if f]
    if not files:
        log.warning("No Bronze file was written in run %s; nothing to signal", run_ts)
        return None
    manifest = {"run_ts": run_ts, "bronze_files": files,
                "written_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    out = paths.atomic_write_json(paths.bronze_manifest(run_ts), manifest)
    log.info("Run manifest written: %s -> %s", out.name, files)
    return out.name


def load_envelope(name: str) -> dict:
    return json.loads((paths.bronze_dir() / name).read_text(encoding="utf-8"))
