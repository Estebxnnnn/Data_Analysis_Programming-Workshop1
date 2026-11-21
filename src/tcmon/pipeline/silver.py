"""Silver layer: preprocessing of Bronze batches into typed, deduplicated Parquet.

Every rule below is grounded in the Workshop 1 profiling of the real samples
(DQ1-DQ8 in the Workshop 1 report); see workshop_2/preprocessing_spec.md.

Steps, in order:
 1. conform     map each source to the Silver columns (reuses tcmon.normalization)
 2. types       enforce dtypes (UTC timestamps, floats, nullable ints/bools, categories)
 3. nulls       reject rows without time/position; coalesce, derive or flag the rest
 4. text        normalize storm names (case, accents, punctuation, GDACS prefix/suffix)
 5. outliers    physical-range check (value -> NULL + flag) and IQR / z-score flags
                against climatological fences (Workshop 3; reference_stats.json)
 6. dedup       within the batch (event_id) and across batches (event_id + record_hash)
 7. schema      enforce the Silver schema and write Parquet with lineage metadata
                (no file when the batch has no new or changed record - Workshop 3)
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import shutil
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from ..normalization import canonical
from ..normalization.units import haversine_km
from ..sources import gdacs, ibtracs
from . import paths
from .schema import BUSINESS_COLUMNS, COLUMNS, PANDAS_DTYPES, REQUIRED, SILVER_SCHEMA

log = logging.getLogger(__name__)

# physical plausibility bounds (IBTrACS documentation; observed ranges in Workshop 1)
BOUNDS = {
    "wind_kt": (0.0, 200.0),          # max observed 185 kt (WMO_WIND); no real storm above 200 kt
    "peak_wind_kt": (0.0, 200.0),
    "pressure_hpa": (850.0, 1050.0),  # documented valid range
    "storm_speed_kt": (0.0, 70.0),
    "storm_dir_deg": (0.0, 360.0),
    "dist2land_km": (0.0, 5000.0),
}
SPELLED_NUMBERS = {"ONE", "TWO", "THREE", "FOUR", "FIVE", "SIX", "SEVEN", "EIGHT", "NINE", "TEN",
                   "ELEVEN", "TWELVE", "THIRTEEN", "FOURTEEN", "FIFTEEN", "SIXTEEN", "SEVENTEEN",
                   "EIGHTEEN", "NINETEEN", "TWENTY"}
PLACEHOLDER_NAMES = {"UNNAMED", "NOT_NAMED", "NOT NAMED", "NONAME", ""}
GDACS_PREFIX = re.compile(r"^(TROPICAL\s+CYCLONE|TROPICAL\s+STORM|TROPICAL\s+DEPRESSION|HURRICANE|TYPHOON|CYCLONE)\s+", re.I)
YEAR_SUFFIX = re.compile(r"-\d{2}$")


# ======================================================================= batch
def claim_batch(silver_ts: str) -> list[Path]:
    """Move pending run manifests to _claimed/<silver_ts>/ and return them.

    Re-running the same Silver run finds its manifests already claimed and
    reprocesses exactly the same Bronze files (idempotency)."""
    target = paths.claimed_dir() / silver_ts
    if target.exists() and any(target.glob("bronze_run_*.json")):
        return sorted(target.glob("bronze_run_*.json"))
    pending = sorted(paths.pending_dir().glob("bronze_run_*.json"))
    if not pending:
        return []
    target.mkdir(parents=True, exist_ok=True)
    claimed = []
    for m in pending:
        dest = target / m.name
        shutil.move(str(m), dest)
        claimed.append(dest)
    log.info("Claimed %s Bronze run manifest(s) for Silver run %s", len(claimed), silver_ts)
    return claimed


def bronze_files_of(manifests: list[Path]) -> list[str]:
    files: list[str] = []
    for m in manifests:
        files += json.loads(m.read_text(encoding="utf-8"))["bronze_files"]
    return sorted(dict.fromkeys(files))


# ===================================================================== conform
def _sha_of(envelope: dict) -> str:
    shas = [h.get("sha256") for h in envelope.get("http", []) if h.get("sha256")]
    return ";".join(shas)


def conform_ibtracs(env: dict, bronze_name: str) -> pd.DataFrame:
    raw = pd.DataFrame(env["records"], columns=env.get("columns") or None).astype("string").fillna("")
    if raw.empty:
        return pd.DataFrame(columns=COLUMNS)
    typed = ibtracs.coerce_types(raw.astype(object))
    ev = canonical.ibtracs_to_events(typed, env["source"], bronze_name)
    usa_wind = typed["USA_WIND"].notna()
    return pd.DataFrame({
        "event_id": ev["event_id"], "source": env["source"], "event_type": ev["event_type"],
        "storm_id": typed["SID"], "storm_name_raw": typed["NAME"], "basin": typed["BASIN"],
        "timestamp_utc": ev["timestamp"], "latitude": ev["latitude"], "longitude": ev["longitude"],
        "wind_kt": ev["severity"],
        "wind_avg_period": np.where(usa_wind, "1-min", np.where(typed["WMO_WIND"].notna(), "10-min agency", "unknown")),
        "sshs_category": ev["sshs_category"], "pressure_hpa": ev["pressure_hpa"], "peak_wind_kt": np.nan,
        "nature": typed["NATURE"], "track_type": typed["TRACK_TYPE"],
        "is_provisional": ev["is_provisional"], "is_spur": ev["is_spur"],
        "storm_speed_kt": ev["storm_speed_kt"], "storm_dir_deg": ev["storm_dir_deg"],
        "dist2land_km": ev["dist2land_km"],
        "alert_level": None, "alert_score": np.nan, "is_current": None, "gdacs_episode_id": None,
        "date_modified_utc": pd.NaT,
        "bronze_file": bronze_name, "bronze_sha256": _sha_of(env),
    })


def conform_gdacs(env: dict, bronze_name: str) -> pd.DataFrame:
    frame = gdacs.features_to_frame(env["records"])
    if frame.empty:
        return pd.DataFrame(columns=COLUMNS)
    ev = canonical.gdacs_to_events(frame, bronze_name)       # also drops duplicate (eventid, episodeid)
    frame = frame.drop_duplicates(subset=["eventid", "episodeid"]).reset_index(drop=True)
    is_current = frame.get("iscurrent", pd.Series(index=frame.index, dtype=object)).astype(str).str.lower()
    return pd.DataFrame({
        "event_id": ev["event_id"], "source": env["source"], "event_type": ev["event_type"],
        "storm_id": frame["eventid"].astype(str), "storm_name_raw": frame.get("name", frame.get("eventname")),
        "basin": None,
        "timestamp_utc": ev["timestamp"], "latitude": ev["latitude"], "longitude": ev["longitude"],
        "wind_kt": np.nan,                                     # GDACS severity is a peak, not current intensity
        "wind_avg_period": "unknown", "sshs_category": None, "pressure_hpa": np.nan,
        "peak_wind_kt": ev["peak_wind_kt"],
        "nature": None, "track_type": None, "is_provisional": False, "is_spur": False,
        "storm_speed_kt": np.nan, "storm_dir_deg": np.nan, "dist2land_km": np.nan,
        "alert_level": frame.get("alertlevel"), "alert_score": pd.to_numeric(frame.get("alertscore"), errors="coerce"),
        "is_current": is_current.map({"true": True, "false": False}),
        "gdacs_episode_id": pd.to_numeric(frame["episodeid"], errors="coerce"),
        "date_modified_utc": pd.to_datetime(frame.get("datemodified"), errors="coerce").dt.tz_localize("UTC"),
        "bronze_file": bronze_name, "bronze_sha256": _sha_of(env),
    })


# ======================================================================= clean
def normalize_name(raw) -> tuple[str | None, bool]:
    """Text normalization for storm names: Unicode NFKD, upper case, GDACS prefix and
    '-YY' suffix removed, punctuation except hyphen removed, spaces collapsed."""
    if raw is None or (isinstance(raw, float) and np.isnan(raw)) or pd.isna(raw):
        return None, True
    text = unicodedata.normalize("NFKD", str(raw)).encode("ascii", "ignore").decode("ascii")
    text = GDACS_PREFIX.sub("", text.strip().upper())
    text = YEAR_SUFFIX.sub("", text)
    text = re.sub(r"[^\w\s-]", "", text).replace("_", " ")
    text = re.sub(r"\s+", " ", text).strip()
    unnamed = (text in PLACEHOLDER_NAMES or text.replace(" ", "_") in PLACEHOLDER_NAMES
               or text in SPELLED_NUMBERS or bool(re.fullmatch(r"[\d-]+", text)))
    return (None if unnamed else text), unnamed


def _derive_motion(df: pd.DataFrame) -> pd.DataFrame:
    """Fill missing IBTrACS speed/direction from consecutive fixes of the same storm."""
    df["motion_derived"] = False
    fixes = df[df["event_type"] == "tc_track_fix"].sort_values(["storm_id", "timestamp_utc"])
    for _, g in fixes.groupby("storm_id"):
        if len(g) < 2:
            continue
        lat, lon = g["latitude"].to_numpy(), g["longitude"].to_numpy()
        hours = g["timestamp_utc"].diff().dt.total_seconds().to_numpy() / 3600
        dist_nm = np.r_[np.nan, haversine_km(lat[:-1], lon[:-1], lat[1:], lon[1:])] / 1.852
        speed = dist_nm / hours
        dlon = np.radians(np.r_[np.nan, np.diff(lon)])
        y = np.sin(dlon) * np.cos(np.radians(lat))
        x = np.cos(np.radians(np.r_[np.nan, lat[:-1]])) * np.sin(np.radians(lat)) - \
            np.sin(np.radians(np.r_[np.nan, lat[:-1]])) * np.cos(np.radians(lat)) * np.cos(dlon)
        bearing = (np.degrees(np.arctan2(y, x)) + 360) % 360
        missing = g["storm_speed_kt"].isna().to_numpy()
        fill = missing & ~np.isnan(speed)
        if fill.any():
            idx = g.index[fill]
            df.loc[idx, "storm_speed_kt"] = speed[fill]
            df.loc[idx, "storm_dir_deg"] = bearing[fill]
            df.loc[idx, "motion_derived"] = True
    return df


def _flag(df: pd.DataFrame, mask, note: str) -> None:
    mask = pd.Series(mask, index=df.index).fillna(False).astype(bool)
    df.loc[mask, "qc_flags"] = df.loc[mask, "qc_flags"].map(lambda s: f"{s};{note}" if s else note)


def clean(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply null handling, type normalization, text normalization and outlier rules.
    Returns (clean_rows, rejected_rows)."""
    df = df.reset_index(drop=True).copy()
    df["qc_flags"] = ""

    # --- types
    for col in ("latitude", "longitude", "wind_kt", "pressure_hpa", "peak_wind_kt", "storm_speed_kt",
                "storm_dir_deg", "dist2land_km", "alert_score"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], errors="coerce", utc=True)
    df["date_modified_utc"] = pd.to_datetime(df["date_modified_utc"], errors="coerce", utc=True)

    # --- nulls: required identity, time and position -> reject (quarantine)
    bad = df["event_id"].isna() | df["timestamp_utc"].isna() | df["latitude"].isna() | df["longitude"].isna() \
        | ~df["latitude"].between(-90, 90)
    rejected = df[bad].assign(reject_reason="missing or invalid event_id/time/position")
    df = df[~bad].copy()

    # --- nulls: coalesce / derive / sentinel / flag
    df = _derive_motion(df)
    _flag(df, (df["event_type"] == "tc_track_fix") & df["wind_kt"].isna(), "wind_missing")
    df["basin"] = df["basin"].where(df["basin"].notna() & (df["basin"].astype(str).str.strip() != ""), "UN")
    df["nature"] = df["nature"].where(df["nature"].notna() | (df["event_type"] != "tc_track_fix"), "NR")
    df["wind_avg_period"] = df["wind_avg_period"].fillna("unknown")

    # --- text normalization
    names = df["storm_name_raw"].map(normalize_name)
    df["storm_name"] = names.map(lambda t: t[0])
    df["is_unnamed"] = names.map(lambda t: t[1])

    # --- outliers 1: physical plausibility (impossible value -> NULL + flag)
    for col, (lo, hi) in BOUNDS.items():
        out = df[col].notna() & ~df[col].between(lo, hi)
        _flag(df, out, f"{col}_out_of_range")
        df.loc[out, col] = np.nan

    # --- outliers 2: statistical flags against the climatology (values are kept)
    # Workshop 2 computed the IQR of wind_kt inside each batch: in the 17 Nov 01:00 batch it
    # flagged every CEMPAKA fix above 70 kt (a real typhoon peak) and in the 22:00 batch none,
    # because the fences moved with the storms present. The fences now come from the
    # 1980-2025 IBTrACS reference (reference_stats.json) and the wind rule looks at the
    # 6-hourly intensity CHANGE of a storm, not at its level.
    fences = outlier_fences(df)
    dw = wind_change_6h(df)
    df["wind_iqr_outlier"] = False
    df["speed_zscore_outlier"] = False
    if fences.get("dwind6_lo") is not None:
        df["wind_iqr_outlier"] = dw.notna() & ~dw.between(fences["dwind6_lo"], fences["dwind6_hi"])
    if fences.get("speed_std"):
        z = (df["storm_speed_kt"] - fences["speed_mean"]) / fences["speed_std"]
        df["speed_zscore_outlier"] = z.abs() > 3
    df.attrs["outlier_fences"] = fences

    # --- SSHS category only from 1-minute winds
    df.loc[df["wind_avg_period"] != "1-min", "sshs_category"] = None
    return df, rejected


# ==================================================================== outliers
IQR_K = 3.0          # Tukey "far out" fence on intensity changes
MIN_FENCE_N = 30     # minimum sample to compute a fence from the batch itself


def wind_change_6h(df: pd.DataFrame) -> pd.Series:
    """Intensity change of each IBTrACS fix with respect to the previous fix of the same
    storm, scaled to kt per 6 h (gaps above 12 h are ignored)."""
    out = pd.Series(np.nan, index=df.index, dtype="float64")
    fixes = df[(df["event_type"] == "tc_track_fix") & df["wind_kt"].notna()].sort_values(["storm_id", "timestamp_utc"])
    if fixes.empty:
        return out
    hours = fixes.groupby("storm_id")["timestamp_utc"].diff().dt.total_seconds() / 3600
    dw = fixes.groupby("storm_id")["wind_kt"].diff()
    rate = (dw / hours * 6).where((hours > 0) & (hours <= 12))
    out.loc[rate.index] = rate
    return out


def reference_stats() -> dict | None:
    return paths.read_json(paths.reference_dir() / "reference_stats.json")


def outlier_fences(df: pd.DataFrame) -> dict:
    """Fences from the historical reference; if it is not built yet, from the batch itself
    (recorded in the lineage as basis='batch' so the weaker rule stays visible)."""
    ref = reference_stats()
    if ref and ref.get("dwind6_q1") is not None:
        q1, q3 = ref["dwind6_q1"], ref["dwind6_q3"]
        return {"basis": "reference", "dwind6_lo": q1 - IQR_K * (q3 - q1), "dwind6_hi": q3 + IQR_K * (q3 - q1),
                "speed_mean": ref["speed_mean"], "speed_std": ref["speed_std"],
                "reference_built_at_utc": ref.get("built_at_utc")}
    fences: dict = {"basis": "batch"}
    dw = wind_change_6h(df).dropna()
    if len(dw) >= MIN_FENCE_N:
        q1, q3 = dw.quantile([0.25, 0.75])
        fences.update(dwind6_lo=float(q1 - IQR_K * (q3 - q1)), dwind6_hi=float(q3 + IQR_K * (q3 - q1)))
    sp = df["storm_speed_kt"].dropna()
    if len(sp) >= MIN_FENCE_N and sp.std(ddof=0) > 0:
        fences.update(speed_mean=float(sp.mean()), speed_std=float(sp.std(ddof=0)))
    return fences


# ======================================================================= dedup
def record_hashes(df: pd.DataFrame) -> pd.Series:
    cols = [c for c in BUSINESS_COLUMNS if c != "qc_flags"]
    def h(row):
        payload = "|".join("" if pd.isna(v) else str(v) for v in row)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return df[cols].apply(h, axis=1) if len(df) else pd.Series(dtype="string")


def previous_hashes(exclude: Path | None = None) -> pd.DataFrame:
    """(event_id, record_hash) of every earlier Silver file."""
    files = [p for p in sorted(paths.silver_dir().glob(f"{paths.SILVER_TOPIC}_*.parquet")) if p != exclude]
    if not files:
        return pd.DataFrame(columns=["event_id", "record_hash"])
    return pd.concat([pd.read_parquet(p, columns=["event_id", "record_hash"]) for p in files], ignore_index=True)


def deduplicate(df: pd.DataFrame, previous: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    stats = {"input": len(df)}
    df = df.sort_values(["event_id", "bronze_file"]).drop_duplicates("event_id", keep="last")
    stats["within_batch_duplicates"] = stats["input"] - len(df)
    df = df.copy()
    df["record_hash"] = record_hashes(df).to_numpy() if len(df) else []
    seen = set(zip(previous["event_id"].astype(str), previous["record_hash"].astype(str)))
    seen_ids = set(previous["event_id"].astype(str))
    dup = [(e, h) in seen for e, h in zip(df["event_id"].astype(str), df["record_hash"].astype(str))]
    stats["cross_batch_duplicates"] = int(sum(dup))
    df = df[~np.array(dup, dtype=bool)] if len(df) else df
    df["is_revision"] = df["event_id"].astype(str).isin(seen_ids)
    stats["revisions"] = int(df["is_revision"].sum()) if len(df) else 0
    stats["output"] = len(df)
    return df, stats


# ====================================================================== schema
def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in COLUMNS if c not in df.columns]
    for c in missing:
        df[c] = None
    df = df[COLUMNS].copy()
    for f in SILVER_SCHEMA:
        target = PANDAS_DTYPES[f.kind]
        if f.kind == "timestamp":
            df[f.name] = pd.to_datetime(df[f.name], errors="coerce", utc=True)
        elif f.kind == "bool":
            df[f.name] = df[f.name].map(lambda v: None if v is None or (isinstance(v, float) and np.isnan(v)) else bool(v)).astype("boolean")
        elif f.kind in ("int8", "int64"):
            df[f.name] = pd.to_numeric(df[f.name], errors="coerce").round().astype(target)
        elif f.kind == "float":
            df[f.name] = pd.to_numeric(df[f.name], errors="coerce").astype("float64")
        elif f.kind == "category":
            vals = df[f.name].astype("string")
            df[f.name] = vals.astype(object).where(vals.notna(), None).astype("category")
        else:
            df[f.name] = df[f.name].astype("string")
    nulls = {c: int(df[c].isna().sum()) for c in REQUIRED if df[c].isna().any()}
    if nulls:
        raise ValueError(f"Required Silver fields contain nulls: {nulls}")
    return df


def write_parquet(df: pd.DataFrame, path: Path, metadata: dict) -> Path:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from .schema import arrow_schema

    schema = arrow_schema()
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False, safe=False)
    meta = dict(table.schema.metadata or {})
    meta[b"tcmon_lineage"] = json.dumps(metadata, default=str).encode("utf-8")
    table = table.replace_schema_metadata(meta)
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf, compression="snappy")
    return paths.atomic_write_bytes(path, buf.getvalue().to_pybytes())


# ===================================================================== process
def process(bronze_names: list[str], silver_ts: str) -> dict:
    """Full Silver run for the given Bronze files. Returns a summary for the logs/XCom."""
    paths.ensure_layout()
    frames = []
    for name in bronze_names:
        env = json.loads((paths.bronze_dir() / name).read_text(encoding="utf-8"))
        conform = conform_ibtracs if env["source"].startswith("ibtracs") else conform_gdacs
        part = conform(env, name)
        log.info("Conformed %s: %s rows", name, len(part))
        frames.append(part)
    frames = [f for f in frames if len(f)]
    raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLUMNS)

    cleaned, rejected = clean(raw) if len(raw) else (raw, raw)
    out_path = paths.silver_file(silver_ts)
    deduped, stats = deduplicate(cleaned, previous_hashes(exclude=out_path)) if len(cleaned) \
        else (cleaned, {"input": 0, "within_batch_duplicates": 0, "cross_batch_duplicates": 0, "revisions": 0, "output": 0})
    now = datetime.now(timezone.utc)
    deduped = deduped.assign(silver_run_ts=silver_ts, processed_at_utc=now)
    final = enforce_schema(deduped)

    lineage = {"silver_file": out_path.name if len(final) else None, "silver_run_ts": silver_ts,
               "bronze_files": bronze_names, "rows": len(final), "rejected": len(rejected), "dedup": stats,
               "outlier_fences": cleaned.attrs.get("outlier_fences") if len(raw) else None,
               "processed_at_utc": now.isoformat(timespec="seconds")}
    if len(final):
        write_parquet(final, out_path, lineage)
    else:
        # Workshop 2 wrote a 0-row Parquet file for every batch without changes (3 of 6
        # files on 17 Nov). The run is now only recorded in the manifest.
        out_path.unlink(missing_ok=True)
        log.info("Silver run %s: no new or changed records; no Parquet file written", silver_ts)
    if len(rejected):
        paths.atomic_write_json(paths.rejected_file(silver_ts), json.loads(rejected.to_json(orient="records", date_format="iso")))
    manifest = paths.silver_dir() / "_manifest.jsonl"
    lines = [l for l in (manifest.read_text(encoding="utf-8").splitlines() if manifest.exists() else [])
             if l.strip() and json.loads(l).get("silver_run_ts") != silver_ts]   # re-run replaces its own line
    lines.append(json.dumps(lineage, default=str))
    paths.atomic_write_bytes(manifest, ("\n".join(lines) + "\n").encode("utf-8"))
    log.info("Silver run recorded: %s", lineage)
    return lineage
