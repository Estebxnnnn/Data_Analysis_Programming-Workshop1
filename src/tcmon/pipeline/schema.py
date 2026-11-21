"""Target schema of the Silver layer (tropical_cyclones_*.parquet).

One row = one observation of a tropical cyclone: an IBTrACS track fix or a GDACS
alert episode. This table is the single source of truth: silver.py enforces it
and the Workshop 2 documentation is generated from it.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Field:
    name: str
    kind: str          # string | category | float | int8 | int64 | bool | timestamp
    required: bool
    description: str


SILVER_SCHEMA: list[Field] = [
    # identity and lineage of the observation
    Field("event_id", "string", True, "SHA-1 of source, native storm id and time (IBTrACS) or event and episode (GDACS)"),
    Field("source", "category", True, "ibtracs_active | gdacs_tc"),
    Field("event_type", "category", True, "tc_track_fix | tc_alert_episode"),
    Field("storm_id", "string", True, "IBTrACS SID or GDACS eventid"),
    Field("storm_name", "string", False, "Normalized storm name (upper case, no prefix or year suffix); NULL if unnamed"),
    Field("storm_name_raw", "string", False, "Name exactly as published by the source"),
    Field("is_unnamed", "bool", True, "True for placeholders (UNNAMED, NOT_NAMED, numeric codes, spelled numbers)"),
    Field("basin", "category", True, "IBTrACS basin code (NA, EP, WP, NI, SI, SP, SA); UN = not reported (GDACS)"),
    # time and position
    Field("timestamp_utc", "timestamp", True, "Observation time in UTC"),
    Field("latitude", "float", True, "Degrees north, WGS84, in [-90, 90]"),
    Field("longitude", "float", True, "Degrees east, WGS84, normalized to [-180, 180)"),
    # intensity
    Field("wind_kt", "float", False, "Current maximum sustained wind (kt): USA_WIND, else WMO_WIND; IBTrACS only"),
    Field("wind_avg_period", "category", True, "1-min | 10-min agency | unknown"),
    Field("sshs_category", "int8", False, "Saffir-Simpson category (-1 TD, 0 TS, 1-5) from 1-min wind"),
    Field("pressure_hpa", "float", False, "Minimum central pressure (hPa): USA_PRES, else WMO_PRES"),
    Field("peak_wind_kt", "float", False, "GDACS severity converted from km/h; peak wind of the storm so far"),
    # storm nature and track metadata
    Field("nature", "category", False, "DS, TS, ET, SS, MX; NR = not reported"),
    Field("track_type", "category", False, "main, PROVISIONAL, spur-* ..."),
    Field("is_provisional", "bool", True, "Track still provisional (will be revised)"),
    Field("is_spur", "bool", True, "Spur track (excluded from analogs)"),
    Field("storm_speed_kt", "float", False, "Translation speed (kt)"),
    Field("storm_dir_deg", "float", False, "Translation direction (degrees)"),
    Field("motion_derived", "bool", True, "Speed/direction derived from consecutive fixes because the source left them empty"),
    Field("dist2land_km", "float", False, "Distance to land (km)"),
    # GDACS alert attributes
    Field("alert_level", "category", False, "Green < Orange < Red"),
    Field("alert_score", "float", False, "GDACS alert score"),
    Field("is_current", "bool", False, "GDACS iscurrent flag"),
    Field("gdacs_episode_id", "int64", False, "GDACS episode number"),
    Field("date_modified_utc", "timestamp", False, "GDACS datemodified (UTC)"),
    # quality flags
    Field("wind_iqr_outlier", "bool", True, "6-hourly wind change outside [Q1 - 3 IQR, Q3 + 3 IQR] of the 1980-2025 reference (flag only; batch IQR of wind_kt until Workshop 2)"),
    Field("speed_zscore_outlier", "bool", True, "|z| > 3 for storm_speed_kt against the reference mean and SD (flag only; batch z-score until Workshop 2)"),
    Field("qc_flags", "string", True, "Semicolon-separated quality notes; empty string when none"),
    # deduplication and lineage
    Field("record_hash", "string", True, "SHA-256 of the business columns, used for cross-batch deduplication"),
    Field("is_revision", "bool", True, "Same event_id already in an earlier Silver file with different values"),
    Field("bronze_file", "string", True, "Bronze JSON file the row comes from"),
    Field("bronze_sha256", "string", True, "SHA-256 of the raw HTTP response stored in that Bronze file"),
    Field("silver_run_ts", "string", True, "Silver run timestamp (YYYYMMDD_HHMMSS, UTC), as in the file name"),
    Field("processed_at_utc", "timestamp", True, "Processing time (UTC)"),
]

COLUMNS = [f.name for f in SILVER_SCHEMA]
REQUIRED = [f.name for f in SILVER_SCHEMA if f.required]
BUSINESS_COLUMNS = [f.name for f in SILVER_SCHEMA if f.name not in {
    "record_hash", "is_revision", "bronze_file", "bronze_sha256", "silver_run_ts", "processed_at_utc",
    "wind_iqr_outlier", "speed_zscore_outlier"}]

# pandas dtypes used before conversion to Arrow
PANDAS_DTYPES = {
    "string": "string", "category": "category", "float": "float64", "int8": "Int8",
    "int64": "Int64", "bool": "boolean", "timestamp": "datetime64[ns, UTC]",
}


def arrow_schema():
    """pyarrow schema (imported lazily: pyarrow is installed in the Airflow image)."""
    import pyarrow as pa
    types = {
        "string": pa.string(), "category": pa.dictionary(pa.int32(), pa.string()),
        "float": pa.float64(), "int8": pa.int8(), "int64": pa.int64(), "bool": pa.bool_(),
        "timestamp": pa.timestamp("us", tz="UTC"),
    }
    return pa.schema([pa.field(f.name, types[f.kind], nullable=not f.required) for f in SILVER_SCHEMA])


def markdown_table() -> str:
    lines = ["| Field | Type | Required | Description |", "| --- | --- | --- | --- |"]
    for f in SILVER_SCHEMA:
        lines.append(f"| `{f.name}` | {f.kind} | {'yes' if f.required else 'no'} | {f.description} |")
    return "\n".join(lines)
