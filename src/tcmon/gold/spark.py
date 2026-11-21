"""Spark helpers for the Gold layer.

The session runs in local mode inside the Airflow task process (no cluster):
master local[N], small shuffle, no UI, UTC everywhere. Settings can be changed
with environment variables in docker-compose.yml:

    TCMON_SPARK_MASTER         default local[2]
    TCMON_SPARK_DRIVER_MEMORY  default 2g
"""
from __future__ import annotations

import glob
import logging
import math
import os
import shutil
import sys
import time
from pathlib import Path

from ..pipeline import paths

log = logging.getLogger(__name__)

EARTH_RADIUS_KM = 6371.0088


def get_spark(app: str = "tcmon-gold"):
    """Create (or reuse) a local SparkSession."""
    os.environ["TZ"] = "UTC"                    # collected timestamps are converted with the process time zone
    if hasattr(time, "tzset"):
        time.tzset()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    local_dir = Path(os.getenv("TCMON_SPARK_LOCAL_DIR", "/tmp/tcmon-spark"))
    local_dir.mkdir(parents=True, exist_ok=True)
    builder = (
        SparkSession.builder.appName(app)
        .master(os.getenv("TCMON_SPARK_MASTER", "local[2]"))
        .config("spark.driver.memory", os.getenv("TCMON_SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.local.dir", str(local_dir))
        # Spark builds the text of every physical plan for its event log even with the UI off;
        # long plans (many joins and unions) can exhaust the driver heap, so the text is capped.
        .config("spark.sql.maxPlanStringLength", "200000")
        .config("spark.sql.maxMetadataStringLength", "1000")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark %s session started (master=%s)", spark.version, spark.sparkContext.master)
    return spark


# --------------------------------------------------------------------- Silver
def silver_files() -> list[Path]:
    return sorted(paths.silver_dir().glob(f"{paths.SILVER_TOPIC}_*.parquet"))


def read_silver(spark, latest_only: bool = True):
    """spark.read.parquet("datalake_silver/").

    Spark skips every path that starts with "_" or ".", so _manifest.jsonl,
    _rejected/ and _reference/ are not read as data. With latest_only=True each
    event_id keeps only its most recent version (Silver stores revisions as new rows)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    sdf = spark.read.parquet(str(paths.silver_dir()) + "/").withColumn("silver_file", F.input_file_name())
    sdf = sdf.withColumn("silver_file", F.regexp_extract("silver_file", r"([^/]+\.parquet)$", 1))
    if not latest_only:
        return sdf
    w = Window.partitionBy("event_id").orderBy(F.col("processed_at_utc").desc(), F.col("silver_run_ts").desc())
    return sdf.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def read_reference(spark):
    from ..pipeline import reference
    return spark.read.parquet(str(reference.parquet_path()))


# --------------------------------------------------------------------- output
def write_single_parquet(sdf, target: Path) -> Path:
    """Write a Spark DataFrame as ONE Parquet file with the exact name required
    (governance_YYYYMMDD_HHMMSS.parquet). Spark writes a folder of part files, so the
    data is coalesced to one partition, written to a temporary folder whose name starts
    with "_" (ignored by readers), and the single part file is renamed into place."""
    target = Path(target)
    tmp = target.parent / f"_tmp_{target.stem}"
    shutil.rmtree(tmp, ignore_errors=True)
    sdf.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(str(tmp))
    parts = glob.glob(str(tmp / "part-*.parquet"))
    if len(parts) != 1:
        raise RuntimeError(f"Expected one part file in {tmp}, found {len(parts)}")
    os.replace(parts[0], target)                 # atomic rename on the same file system
    shutil.rmtree(tmp, ignore_errors=True)
    log.info("Gold file written: %s", target.name)
    return target


# ------------------------------------------------------------------- geodesy
def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance (km) between Column coordinates; safe across 180 degrees."""
    from pyspark.sql import functions as F
    p1, p2 = F.radians(lat1), F.radians(lat2)
    dphi = p2 - p1
    dlmb = F.radians(lon2) - F.radians(lon1)
    a = F.pow(F.sin(dphi / 2), 2) + F.cos(p1) * F.cos(p2) * F.pow(F.sin(dlmb / 2), 2)
    return F.lit(2 * EARTH_RADIUS_KM) * F.asin(F.sqrt(F.least(F.lit(1.0), a)))


def bearing_deg(lat1, lon1, lat2, lon2):
    """Initial bearing (degrees from north, 0-360) of the move from point 1 to point 2."""
    from pyspark.sql import functions as F
    p1, p2 = F.radians(lat1), F.radians(lat2)
    dlmb = F.radians(lon2) - F.radians(lon1)
    y = F.sin(dlmb) * F.cos(p2)
    x = F.cos(p1) * F.sin(p2) - F.sin(p1) * F.cos(p2) * F.cos(dlmb)
    return F.pmod(F.degrees(F.atan2(y, x)) + F.lit(360.0), F.lit(360.0))


def circular_diff(a, b, period: float):
    """Smallest absolute difference on a circle (angles: 360, day of year: 365)."""
    from pyspark.sql import functions as F
    d = F.pmod(F.abs(a - b), F.lit(float(period)))
    return F.least(d, F.lit(float(period)) - d)


def compass16(deg: float | None) -> str | None:
    if deg is None or (isinstance(deg, float) and math.isnan(deg)):
        return None
    names = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    return names[int((deg % 360) / 22.5 + 0.5) % 16]
