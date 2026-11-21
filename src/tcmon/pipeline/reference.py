"""Historical reference layer: IBTrACS v04r01 since1980 as a Silver-quality Parquet file.

The Storm Analog Similarity indicator (Workshop 1, Section "Indicator Proposal")
compares each active storm with all storms of the same basin since 1980. That
archive is a static file of about 145 MB that NCEI republishes a few times a
week, so it is NOT part of the hourly Bronze/Silver flow:

    datalake_bronze/_reference/ibtracs_since1980.csv            raw download (git-ignored)
    datalake_bronze/_state/ibtracs_since1980.json               ETag / Last-Modified / SHA-256
    datalake_silver/_reference/ibtracs_since1980.parquet        typed, one row per fix
    datalake_silver/_reference/reference_stats.json             climatological outlier fences

The same conforming rules as the Silver layer are applied (canonical.ibtracs_to_events:
USA_WIND, else WMO_WIND; longitudes in [-180, 180); spur flag). The file is read in
chunks so the build stays below ~1 GB of memory inside the Airflow container.

Command line (inside the container):
    python -m tcmon.pipeline.reference            # download if changed, then build
    python -m tcmon.pipeline.reference --force    # rebuild even if nothing changed
"""
from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from ..http_client import PoliteClient
from ..normalization import canonical
from ..sources import ibtracs
from . import paths

log = logging.getLogger(__name__)

SOURCE = "ibtracs_since1980"
CHUNK_ROWS = 50_000
REF_COLUMNS = ["sid", "season", "basin", "name", "iso_time", "lat", "lon", "wind_kt", "wind_avg_period",
               "pressure_hpa", "sshs_category", "nature", "track_type", "is_spur", "dist2land_km",
               "storm_speed_kt", "storm_dir_deg"]


def raw_csv() -> Path:
    return paths.bronze_reference_dir() / f"{SOURCE}.csv"


def parquet_path() -> Path:
    return paths.reference_dir() / f"{SOURCE}.parquet"


def stats_path() -> Path:
    return paths.reference_dir() / "reference_stats.json"


def state_path() -> Path:
    return paths.state_dir() / f"{SOURCE}.json"


# ------------------------------------------------------------------ download
def download(force: bool = False) -> dict:
    """Conditional GET of the since1980 CSV. Returns {'changed': bool, ...}."""
    paths.ensure_layout()
    paths.bronze_reference_dir().mkdir(parents=True, exist_ok=True)
    state = paths.read_json(state_path(), {}) or {}
    have_file = raw_csv().exists()
    tmp = paths.bronze_reference_dir() / f".{SOURCE}.download"
    client = PoliteClient(min_interval=1.0, timeout=600, max_retries=3)
    res = client.fetch_to_file(ibtracs.url_for("since1980"), tmp,
                               etag=None if (force or not have_file) else state.get("etag"),
                               last_modified=None if (force or not have_file) else state.get("last_modified"))
    Path(str(tmp) + ".meta.json").unlink(missing_ok=True)
    if res.status == 304:
        log.info("IBTrACS since1980 not modified (Last-Modified %s)", state.get("last_modified"))
        return {"changed": False, "status": 304, "last_modified": state.get("last_modified")}
    if res.status != 200 or not res.path:
        Path(res.path or tmp).unlink(missing_ok=True)
        raise RuntimeError(f"IBTrACS since1980 download failed: HTTP {res.status}")
    changed = force or not have_file or res.sha256 != state.get("sha256")
    Path(res.path).replace(raw_csv())
    new_state = {"etag": res.headers.get("ETag"), "last_modified": res.headers.get("Last-Modified"),
                 "sha256": res.sha256, "size_bytes": res.size_bytes, "elapsed_s": res.elapsed_s,
                 "downloaded_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    paths.atomic_write_json(state_path(), new_state)
    log.info("IBTrACS since1980: HTTP 200, %.1f MB in %.0f s, changed=%s", res.size_bytes / 1e6, res.elapsed_s, changed)
    return {"changed": changed, "status": 200, **new_state}


# --------------------------------------------------------------------- build
def _conform(chunk: pd.DataFrame) -> pd.DataFrame:
    typed = ibtracs.coerce_types(chunk)
    typed = typed[typed["ISO_TIME"].notna()]                     # drops the units row (line 2)
    if typed.empty:
        return pd.DataFrame(columns=REF_COLUMNS)
    typed = typed.reset_index(drop=True)
    ev = canonical.ibtracs_to_events(typed, SOURCE, "ref")      # same index as `typed`
    out = pd.DataFrame({
        "sid": typed["SID"].astype("string"),
        "season": pd.to_numeric(typed["SEASON"], errors="coerce").astype("Int16"),
        "basin": typed["BASIN"].fillna("UN").astype("string"),    # read as text: "NA" stays North Atlantic
        "name": typed["NAME"].astype("string"),
        "iso_time": ev["timestamp"],
        "lat": pd.to_numeric(ev["latitude"], errors="coerce").astype("float64"),
        "lon": pd.to_numeric(ev["longitude"], errors="coerce").astype("float64"),
        "wind_kt": pd.to_numeric(ev["severity"], errors="coerce").astype("float64"),
        "wind_avg_period": pd.Series(np.where(typed["USA_WIND"].notna(), "1-min",
                                     np.where(typed["WMO_WIND"].notna(), "10-min agency", "unknown"))).astype("string"),
        "pressure_hpa": pd.to_numeric(ev["pressure_hpa"], errors="coerce").astype("float64"),
        "sshs_category": pd.Series(ev["sshs_category"]).astype("Int8"),
        "nature": typed["NATURE"].astype("string"),
        "track_type": typed["TRACK_TYPE"].astype("string"),
        "is_spur": ev["is_spur"].astype(bool),
        "dist2land_km": pd.to_numeric(ev["dist2land_km"], errors="coerce").astype("float64"),
        "storm_speed_kt": pd.to_numeric(ev["storm_speed_kt"], errors="coerce").astype("float64"),
        "storm_dir_deg": pd.to_numeric(ev["storm_dir_deg"], errors="coerce").astype("float64"),
    })
    return out[out["lat"].between(-90, 90) & out["lon"].notna()]


def read_reference_csv(path: Path) -> pd.DataFrame:
    header = pd.read_csv(path, nrows=0).columns
    use = [c for c in ibtracs.USED_COLUMNS if c in header]
    parts = []
    for chunk in pd.read_csv(path, dtype=str, keep_default_na=False, usecols=use,
                             chunksize=CHUNK_ROWS, low_memory=False):
        parts.append(_conform(chunk))
        log.info("Reference chunk conformed: %s rows so far", sum(len(p) for p in parts))
    df = pd.concat(parts, ignore_index=True)
    df["iso_time"] = pd.to_datetime(df["iso_time"], utc=True)
    return df.drop_duplicates(["sid", "iso_time"]).sort_values(["sid", "iso_time"]).reset_index(drop=True)


def compute_stats(df: pd.DataFrame) -> dict:
    """Climatological distributions used as outlier fences by the Silver layer."""
    main = df[(df["track_type"] == "main") & ~df["is_spur"]]
    syn = main[main["iso_time"].dt.hour % 6 == 0].sort_values(["sid", "iso_time"])
    hours = syn.groupby("sid")["iso_time"].diff().dt.total_seconds() / 3600
    dw = (syn.groupby("sid")["wind_kt"].diff() / hours * 6).where(hours == 6).dropna()
    sp = main["storm_speed_kt"].dropna()
    q = dw.quantile([0.25, 0.5, 0.75, 0.005, 0.995])
    return {
        "built_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rows": int(len(df)), "storms": int(df["sid"].nunique()),
        "seasons": [int(df["season"].min()), int(df["season"].max())],
        "dwind6_n": int(len(dw)), "dwind6_q1": float(q.loc[0.25]), "dwind6_median": float(q.loc[0.5]),
        "dwind6_q3": float(q.loc[0.75]), "dwind6_p005": float(q.loc[0.005]), "dwind6_p995": float(q.loc[0.995]),
        "speed_n": int(len(sp)), "speed_mean": float(sp.mean()), "speed_std": float(sp.std(ddof=0)),
        "speed_p999": float(sp.quantile(0.999)),
    }


def build(csv_path: Path | None = None) -> dict:
    import pyarrow as pa
    import pyarrow.parquet as pq

    csv_path = csv_path or raw_csv()
    df = read_reference_csv(csv_path)
    stats = compute_stats(df)
    stats["source_state"] = paths.read_json(state_path(), {})
    table = pa.Table.from_pandas(df[REF_COLUMNS], preserve_index=False)
    table = table.replace_schema_metadata({**(table.schema.metadata or {}),
                                           b"tcmon_reference": json.dumps(stats, default=str).encode()})
    buf = pa.BufferOutputStream()
    # microsecond timestamps: Spark 3.5 cannot read Parquet TIMESTAMP(NANOS), pandas' default
    pq.write_table(table, buf, compression="snappy", coerce_timestamps="us", allow_truncated_timestamps=True)
    paths.atomic_write_bytes(parquet_path(), buf.getvalue().to_pybytes())
    paths.atomic_write_json(stats_path(), stats)
    log.info("Reference built: %s rows, %s storms, seasons %s", stats["rows"], stats["storms"], stats["seasons"])
    return stats


def ensure(max_age_days: float = 7, force: bool = False) -> dict:
    """Make sure the reference exists and is at most `max_age_days` old (checked with a
    conditional GET, so an unchanged archive costs one small request)."""
    stats = paths.read_json(stats_path())
    last = stats and (stats.get("checked_at_utc") or stats.get("built_at_utc"))
    fresh = bool(last) and parquet_path().exists() and \
        datetime.now(timezone.utc) - datetime.fromisoformat(last) < timedelta(days=max_age_days)
    if fresh and not force:
        return {"available": True, "rebuilt": False, **stats}
    dl = download(force=force)
    if dl["changed"] or force or not parquet_path().exists():
        return {"available": True, "rebuilt": True, **build()}
    stats = paths.read_json(stats_path(), {}) or {}
    stats["checked_at_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    paths.atomic_write_json(stats_path(), stats)
    return {"available": True, "rebuilt": False, **stats}


if __name__ == "__main__":       # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--csv", help="build from an already downloaded CSV (no network)")
    a = ap.parse_args()
    if a.csv:
        print(json.dumps(build(Path(a.csv)), indent=1, default=str))
    else:
        print(json.dumps(ensure(force=a.force), indent=1, default=str))
