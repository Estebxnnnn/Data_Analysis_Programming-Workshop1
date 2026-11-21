"""Data-governance KPIs of the pipeline, computed with PySpark.

Output: datalake_gold/governance_YYYYMMDD_HHMMSS.parquet, a tidy table with one row
per KPI value:

    kpi_id, dimension, kpi_name, scope, scope_value, value, unit, target, status,
    basis_n, detail, user_story, gold_run_ts, computed_at_utc

Dimensions follow the usual data-quality frameworks (DAMA-DMBOK; ISO/IEC 25012):
completeness, uniqueness, validity, conformity, timeliness, consistency, plus
efficiency and volume of the ingestion. Every KPI answers a finding of the
Workshop 2 runs (workshop_3/data_quality_findings.md) or a trust user story
(US-04 freshness and record counts, US-07 provisional and rejected records).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from ..pipeline.schema import COLUMNS, REQUIRED
from . import inventory

log = logging.getLogger(__name__)

KPI_COLUMNS = ["kpi_id", "dimension", "kpi_name", "scope", "scope_value", "value", "unit", "target",
               "status", "basis_n", "detail", "user_story"]
EXPECTED_RUNS_PER_DAY = 24          # hourly Bronze schedule (Workshop 1 ingestion contract)
NAME_MATCH_DAYS = 10                # GDACS <-> IBTrACS name matching window


def _schemas():
    from pyspark.sql import types as T
    attempts = T.StructType([
        T.StructField("source", T.StringType()), T.StructField("run_ts", T.StringType()),
        T.StructField("run_time_utc", T.TimestampType()), T.StructField("outcome", T.StringType()),
        T.StructField("bytes", T.LongType()), T.StructField("elapsed_s", T.DoubleType()),
        T.StructField("http_status", T.StringType()), T.StructField("origin", T.StringType())])
    bronze = T.StructType([
        T.StructField("source", T.StringType()), T.StructField("run_ts", T.StringType()),
        T.StructField("file", T.StringType()), T.StructField("run_time_utc", T.TimestampType()),
        T.StructField("ingested_at_utc", T.TimestampType()), T.StructField("record_count", T.LongType()),
        T.StructField("file_bytes", T.LongType()), T.StructField("download_bytes", T.LongType()),
        T.StructField("elapsed_s", T.DoubleType()), T.StructField("requests", T.LongType()),
        T.StructField("sha256", T.StringType()), T.StructField("etag", T.StringType()),
        T.StructField("last_modified", T.StringType()), T.StructField("name_ok", T.BooleanType())])
    manifest = T.StructType([
        T.StructField("silver_run_ts", T.StringType()), T.StructField("silver_file", T.StringType()),
        T.StructField("run_time_utc", T.TimestampType()), T.StructField("processed_at_utc", T.TimestampType()),
        T.StructField("bronze_files", T.LongType()), T.StructField("newest_bronze_time_utc", T.TimestampType()),
        T.StructField("rows", T.LongType()), T.StructField("rejected", T.LongType()),
        T.StructField("input", T.LongType()), T.StructField("within_batch_duplicates", T.LongType()),
        T.StructField("cross_batch_duplicates", T.LongType()), T.StructField("revisions", T.LongType()),
        T.StructField("fence_basis", T.StringType())])
    lineage = T.StructType([
        T.StructField("silver_run_ts", T.StringType()), T.StructField("bronze_file", T.StringType()),
        T.StructField("source", T.StringType()), T.StructField("bronze_ingested_at_utc", T.TimestampType()),
        T.StructField("processed_at_utc", T.TimestampType())])
    files = T.StructType([
        T.StructField("file", T.StringType()), T.StructField("name_ok", T.BooleanType()),
        T.StructField("rows", T.LongType()), T.StructField("columns", T.LongType()),
        T.StructField("schema_ok", T.BooleanType()), T.StructField("has_lineage", T.BooleanType()),
        T.StructField("file_bytes", T.LongType())])
    return attempts, bronze, manifest, lineage, files


def _kpi(sdf, kpi_id, dimension, name, unit, value, *, scope="pipeline", scope_value=None,
         target=None, ok=None, warn=None, basis=None, detail=None, story="US-04"):
    """Project an aggregated Spark DataFrame onto the KPI schema.

    ok / warn are Column predicates on `value`; status = ok | warn | fail, or info
    when no predicate is given."""
    from pyspark.sql import functions as F

    def as_col(x, cast="string"):
        if x is None:
            return F.lit(None).cast(cast)
        return x.cast(cast) if hasattr(x, "cast") and not isinstance(x, str) else F.lit(x).cast(cast)

    v = as_col(value, "double")
    if ok is None:
        status = F.lit("info")
    else:
        status = F.when(v.isNull(), F.lit("n/a")).when(ok(v), F.lit("ok"))
        status = status.when(warn(v), F.lit("warn")) if warn is not None else status
        status = status.otherwise(F.lit("fail"))
    return sdf.select(
        F.lit(kpi_id).alias("kpi_id"), F.lit(dimension).alias("dimension"), F.lit(name).alias("kpi_name"),
        F.lit(scope).alias("scope"), as_col(scope_value).alias("scope_value"), v.alias("value"),
        F.lit(unit).alias("unit"), as_col(target).alias("target"), status.alias("status"),
        as_col(basis, "long").alias("basis_n"), as_col(detail).alias("detail"), F.lit(story).alias("user_story"))


def build(spark, run_ts: str):
    from functools import reduce

    from pyspark.sql import Window
    from pyspark.sql import functions as F

    from .spark import read_silver

    attempts_s, bronze_s, manifest_s, lineage_s, files_s = _schemas()
    att = spark.createDataFrame(inventory.attempts(), attempts_s)
    brz = spark.createDataFrame(inventory.bronze_inventory(), bronze_s)
    man = spark.createDataFrame(inventory.silver_manifest(), manifest_s)
    lin = spark.createDataFrame(inventory.silver_lineage(), lineage_s)
    fil = spark.createDataFrame(inventory.silver_file_checks(), files_s)
    allv = read_silver(spark, latest_only=False).cache()
    cur = read_silver(spark, latest_only=True).cache()
    now = datetime.now(timezone.utc)
    pct = lambda num, den: F.when(den > 0, F.lit(100.0) * num / den)   # noqa: E731
    k = []

    # ------------------------------------------------------------ completeness
    # C1 null rate of every Silver field, per source (latest version of each record)
    by_src = cur.groupBy("source").agg(
        F.count(F.lit(1)).alias("n"), *[F.sum(F.col(c).isNull().cast("int")).alias(f"null__{c}") for c in COLUMNS])
    for c in COLUMNS:
        req = c in REQUIRED
        k.append(_kpi(by_src, "C1", "completeness", f"Null rate of {c}", "%", pct(F.col(f"null__{c}"), F.col("n")),
                      scope="source/field", scope_value=F.concat_ws("/", F.col("source").cast("string"), F.lit(c)),
                      target="0" if req else None, ok=(lambda v: v == 0) if req else None,
                      basis=F.col("n"), detail="required field" if req else "optional field", story="US-04"))
    # C2 hourly runs that delivered data (stored or 'not modified') per UTC day and source
    good = att.filter(F.col("outcome").isin("stored", "not_modified"))
    per_day = good.groupBy("source", F.to_date("run_time_utc").alias("day")).agg(
        F.countDistinct(F.date_trunc("hour", "run_time_utc")).alias("hours"))
    k.append(_kpi(per_day, "C2", "completeness", "Hourly ingestion slots covered per day", "%",
                  pct(F.col("hours"), F.lit(EXPECTED_RUNS_PER_DAY)), scope="source/day",
                  scope_value=F.concat_ws("/", "source", F.col("day").cast("string")), target=">= 95",
                  ok=lambda v: v >= 95, warn=lambda v: v >= 50, basis=F.col("hours"),
                  detail="distinct hours with a successful extraction / 24 (days with the stack off count as gaps)"))
    # C3 success rate of extraction attempts per source
    succ = att.groupBy("source").agg(F.count(F.lit(1)).alias("n"),
                                     F.sum((F.col("outcome") != "failed").cast("int")).alias("ok"),
                                     F.sum((F.col("outcome") == "failed").cast("int")).alias("failed"))
    k.append(_kpi(succ, "C3", "completeness", "Extraction success rate", "%", pct(F.col("ok"), F.col("n")),
                  scope="source", scope_value=F.col("source"), target=">= 99", ok=lambda v: v >= 99,
                  warn=lambda v: v >= 90, basis=F.col("n"),
                  detail=F.concat(F.lit("failed attempts: "), F.col("failed").cast("string"))))

    # -------------------------------------------------------------- uniqueness
    tot = man.agg(*[F.sum(c).alias(c) for c in ("input", "within_batch_duplicates", "cross_batch_duplicates",
                                                  "revisions", "rows", "rejected")], F.count(F.lit(1)).alias("runs"))
    k.append(_kpi(tot, "U1", "uniqueness", "Cross-batch duplicate rate (records already in Silver)", "%",
                  pct(F.col("cross_batch_duplicates"), F.col("input")), basis=F.col("input"),
                  detail="high values mean the same source content is ingested again (see E2)"))
    k.append(_kpi(tot, "U2", "uniqueness", "Within-batch duplicate rate", "%",
                  pct(F.col("within_batch_duplicates"), F.col("input")), basis=F.col("input"),
                  detail="expected when one Silver batch claims several Bronze runs with the same content"))
    k.append(_kpi(tot, "U3", "uniqueness", "Revision rate (same event_id, changed values)", "%",
                  pct(F.col("revisions"), F.col("rows")), basis=F.col("rows"), story="US-07"))
    dup = allv.agg(F.count(F.lit(1)).alias("n"), F.countDistinct("event_id").alias("d"))
    k.append(_kpi(dup, "U4", "uniqueness", "Silver rows that are older versions of an event", "%",
                  pct(F.col("n") - F.col("d"), F.col("n")), basis=F.col("n"),
                  detail="Gold keeps only the latest version of each event_id"))

    # ---------------------------------------------------------------- validity
    k.append(_kpi(tot, "V1", "validity", "Rejected rows (missing id/time/position)", "%",
                  pct(F.col("rejected"), F.col("input") + F.col("rejected")), target="< 1", ok=lambda v: v < 1,
                  basis=F.col("input"), story="US-07"))
    fixes = cur.filter(F.col("event_type") == "tc_track_fix")
    val = fixes.groupBy("source").agg(
        F.count(F.lit(1)).alias("n"),
        F.sum(F.col("qc_flags").contains("out_of_range").cast("int")).alias("oor"),
        F.sum(F.col("qc_flags").contains("wind_missing").cast("int")).alias("wmiss"),
        F.sum(F.col("wind_iqr_outlier").cast("int")).alias("wout"),
        F.sum(F.col("speed_zscore_outlier").cast("int")).alias("sout"),
        F.sum(F.col("motion_derived").cast("int")).alias("mder"),
        F.sum(F.col("is_provisional").cast("int")).alias("prov"),
        F.sum(F.col("is_spur").cast("int")).alias("spur"))
    for kid, col, name, story, tgt in [
            ("V2", "oor", "Values outside physical bounds (set to NULL)", "US-07", "0"),
            ("V3", "wmiss", "Track fixes without wind", "US-07", None),
            ("V4", "wout", "6-h intensity change outside the climatological 3-IQR fence", "US-07", None),
            ("V5", "sout", "Translation speed |z| > 3 against climatology", "US-07", None),
            ("V6", "mder", "Motion derived from consecutive fixes", "US-06", None),
            ("V7", "prov", "Provisional track fixes (will be revised by the agency)", "US-07", None),
            ("V8", "spur", "Spur track fixes (excluded from analogs)", "US-03", None)]:
        k.append(_kpi(val, kid, "validity", name, "%", pct(F.col(col), F.col("n")), scope="source",
                      scope_value=F.col("source").cast("string"), target=tgt,
                      ok=(lambda v: v == 0) if tgt == "0" else None, basis=F.col("n"), story=story))
    k.append(_kpi(man.groupBy("fence_basis").agg(F.count(F.lit(1)).alias("n")), "V9", "validity",
                  "Silver runs per outlier-fence basis", "runs", F.col("n"), scope="fence_basis",
                  scope_value=F.col("fence_basis"), basis=F.col("n"),
                  detail="'reference' = 1980-2025 climatology; 'batch' = fence computed inside the batch"))

    # -------------------------------------------------------------- conformity
    conf = fil.agg(F.count(F.lit(1)).alias("n"), F.sum(F.col("schema_ok").cast("int")).alias("ok"),
                   F.sum(F.col("name_ok").cast("int")).alias("named"),
                   F.sum(F.col("has_lineage").cast("int")).alias("lin"),
                   F.sum((F.col("rows") == 0).cast("int")).alias("empty"))
    k.append(_kpi(conf, "F1", "conformity", "Silver files matching the 38-field schema", "%",
                  pct(F.col("ok"), F.col("n")), target="100", ok=lambda v: v == 100, basis=F.col("n")))
    k.append(_kpi(conf, "F2", "conformity", "Silver files following the naming convention", "%",
                  pct(F.col("named"), F.col("n")), target="100", ok=lambda v: v == 100, basis=F.col("n")))
    k.append(_kpi(conf, "F3", "conformity", "Silver files carrying lineage metadata", "%",
                  pct(F.col("lin"), F.col("n")), target="100", ok=lambda v: v == 100, basis=F.col("n")))
    k.append(_kpi(conf, "F4", "conformity", "Empty Silver files (0 rows)", "files", F.col("empty"), target="0",
                  ok=lambda v: v == 0, basis=F.col("n"),
                  detail="written by Workshop 2 for batches without changes; no longer written"))
    order = man.filter(F.col("newest_bronze_time_utc").isNotNull()).agg(
        F.count(F.lit(1)).alias("n"),
        F.sum((F.col("run_time_utc") < F.col("newest_bronze_time_utc")).cast("int")).alias("bad"))
    k.append(_kpi(order, "F5", "conformity", "Silver runs named earlier than their newest Bronze input", "%",
                  pct(F.col("bad"), F.col("n")), target="0", ok=lambda v: v == 0, basis=F.col("n"),
                  detail="caused by naming scheduled runs with the start of the data interval (fixed in Workshop 3)"))

    # -------------------------------------------------------------- timeliness
    delay = brz.withColumn("delay_min", (F.unix_timestamp("ingested_at_utc") - F.unix_timestamp("run_time_utc")) / 60)
    dl = delay.groupBy("source").agg(F.expr("percentile_approx(delay_min, 0.5)").alias("p50"),
                                     F.max("delay_min").alias("mx"), F.count(F.lit(1)).alias("n"))
    k.append(_kpi(dl, "T1", "timeliness", "Delay between file timestamp and ingestion (median)", "min",
                  F.col("p50"), scope="source", scope_value=F.col("source"), target="<= 5", ok=lambda v: v <= 5,
                  warn=lambda v: v <= 60, basis=F.col("n"),
                  detail=F.concat(F.lit("max "), F.round("mx", 1).cast("string"), F.lit(" min"))))
    lat = lin.withColumn("lat_min", (F.unix_timestamp("processed_at_utc") - F.unix_timestamp("bronze_ingested_at_utc")) / 60) \
             .filter(F.col("lat_min").isNotNull())
    ll = lat.groupBy("source").agg(F.expr("percentile_approx(lat_min, 0.5)").alias("p50"),
                                   F.max("lat_min").alias("mx"), F.count(F.lit(1)).alias("n"))
    k.append(_kpi(ll, "T2", "timeliness", "Bronze-to-Silver latency (median)", "min", F.col("p50"), scope="source",
                  scope_value=F.col("source"), target="<= 15", ok=lambda v: v <= 15, warn=lambda v: v <= 60,
                  basis=F.col("n"), detail=F.concat(F.lit("max "), F.round("mx", 1).cast("string"), F.lit(" min"))))
    age = cur.groupBy("source").agg(F.max("timestamp_utc").alias("last_obs"), F.count(F.lit(1)).alias("n"))
    age = age.withColumn("age_h", (F.lit(int(now.timestamp())) - F.unix_timestamp("last_obs")) / 3600)
    k.append(_kpi(age, "T3", "timeliness", "Age of the newest observation at Gold time", "h", F.col("age_h"),
                  scope="source", scope_value=F.col("source").cast("string"), target="<= 24",
                  ok=lambda v: v <= 24, warn=lambda v: v <= 72, basis=F.col("n"),
                  detail=F.concat(F.lit("newest observation "), F.date_format("last_obs", "yyyy-MM-dd HH:mm 'UTC'"))))
    w_last = Window.partitionBy("source").orderBy(F.col("ingested_at_utc").desc())
    last_ok = brz.withColumn("rn", F.row_number().over(w_last)).filter("rn = 1")
    last_ok = last_ok.withColumn("age_h", (F.lit(int(now.timestamp())) - F.unix_timestamp("ingested_at_utc")) / 3600)
    k.append(_kpi(last_ok, "T4", "timeliness", "Hours since the last stored Bronze file", "h", F.col("age_h"),
                  scope="source", scope_value=F.col("source"), basis=F.col("record_count"),
                  detail=F.concat(F.col("file"), F.lit(", "), F.col("record_count").cast("string"), F.lit(" records")),
                  story="US-04"))

    # ------------------------------------------------------------- consistency
    ib = cur.filter((F.col("source") == "ibtracs_active") & F.col("storm_name").isNotNull()) \
            .groupBy("storm_name").agg(F.max("wind_kt").alias("ib_max"), F.max("timestamp_utc").alias("ib_last"))
    gd = cur.filter((F.col("source") == "gdacs_tc") & F.col("storm_name").isNotNull()) \
            .groupBy("storm_name").agg(F.max("peak_wind_kt").alias("gd_peak"), F.max("timestamp_utc").alias("gd_last"))
    pair = ib.join(gd, "storm_name", "full_outer")
    pair = pair.withColumn("matched", F.col("ib_last").isNotNull() & F.col("gd_last").isNotNull() & (
        F.abs(F.unix_timestamp("gd_last") - F.unix_timestamp("ib_last")) <= NAME_MATCH_DAYS * 86400))
    m = pair.filter("matched")
    k.append(_kpi(m, "K1", "consistency", "GDACS peak wind minus IBTrACS max wind", "kt",
                  F.col("gd_peak") - F.col("ib_max"), scope="storm", scope_value=F.col("storm_name"),
                  target="|diff| <= 10", ok=lambda v: F.abs(v) <= 10, warn=lambda v: F.abs(v) <= 25,
                  detail=F.concat(F.lit("GDACS "), F.round("gd_peak", 0).cast("string"), F.lit(" kt vs IBTrACS "),
                                  F.col("ib_max").cast("string"), F.lit(" kt")), story="US-04"))
    k.append(_kpi(m, "K2", "consistency", "GDACS newest time minus IBTrACS newest fix", "h",
                  (F.unix_timestamp("gd_last") - F.unix_timestamp("ib_last")) / 3600, scope="storm",
                  scope_value=F.col("storm_name"),
                  detail="positive = the alert feed is ahead of the best-track file", story="US-04"))
    cov = pair.agg(F.count(F.lit(1)).alias("n"), F.sum(F.col("matched").cast("int")).alias("m"))
    k.append(_kpi(cov, "K3", "consistency", "Storms present in both sources (name match)", "%",
                  pct(F.col("m"), F.col("n")), basis=F.col("n"), target=">= 90", ok=lambda v: v >= 90,
                  warn=lambda v: v >= 50))
    names = cur.groupBy("source").agg(
        F.count(F.lit(1)).alias("n"), F.sum(F.col("is_unnamed").cast("int")).alias("unn"),
        F.sum((F.col("storm_name_raw").isNotNull() & (F.coalesce(F.col("storm_name"), F.lit("")) !=
                                                       F.col("storm_name_raw"))).cast("int")).alias("chg"),
        F.avg(F.length("storm_name_raw")).alias("len_raw"), F.avg(F.length("storm_name")).alias("len_norm"))
    k.append(_kpi(names, "K4", "consistency", "Names changed by text normalization", "%", pct(F.col("chg"), F.col("n")),
                  scope="source", scope_value=F.col("source").cast("string"), basis=F.col("n"),
                  detail=F.concat(F.lit("mean length raw "), F.round("len_raw", 1).cast("string"),
                                  F.lit(" -> normalized "), F.round("len_norm", 1).cast("string"))))
    k.append(_kpi(names, "K5", "consistency", "Unnamed storms (placeholders, numbers)", "%", pct(F.col("unn"), F.col("n")),
                  scope="source", scope_value=F.col("source").cast("string"), basis=F.col("n")))

    # --------------------------------------------------------- efficiency/volume
    w_prev = Window.partitionBy("source").orderBy("run_time_utc")
    red = brz.withColumn("prev_sha", F.lag("sha256").over(w_prev)) \
             .withColumn("redundant", (F.col("sha256") == F.col("prev_sha")) & (F.col("sha256") != ""))
    rr = red.groupBy("source").agg(F.count(F.lit(1)).alias("n"), F.sum(F.col("redundant").cast("int")).alias("r"),
                                   F.sum(F.when(F.col("redundant"), F.col("file_bytes")).otherwise(0)).alias("rb"))
    k.append(_kpi(rr, "E1", "efficiency", "Bronze files identical to the previous file of the source", "%",
                  pct(F.col("r"), F.col("n")), scope="source", scope_value=F.col("source"), target="0",
                  ok=lambda v: v == 0, warn=lambda v: v <= 10, basis=F.col("n"),
                  detail=F.concat(F.round(F.col("rb") / 1e6, 2).cast("string"), F.lit(" MB stored twice"))))
    nm = att.groupBy("source").agg(F.count(F.lit(1)).alias("n"),
                                   F.sum((F.col("outcome") == "not_modified").cast("int")).alias("nm"),
                                   F.sum("bytes").alias("b"))
    k.append(_kpi(nm, "E2", "efficiency", "Extractions answered 'not modified' (no file stored)", "%",
                  pct(F.col("nm"), F.col("n")), scope="source", scope_value=F.col("source"), basis=F.col("n"),
                  detail=F.concat(F.round(F.col("b") / 1e6, 2).cast("string"), F.lit(" MB downloaded in total"))))
    vol = brz.groupBy("source", F.to_date("run_time_utc").alias("day")).agg(
        F.count(F.lit(1)).alias("files"), F.sum("record_count").alias("recs"), F.sum("file_bytes").alias("b"))
    k.append(_kpi(vol, "E3", "volume", "Bronze records stored per day", "records", F.col("recs"),
                  scope="source/day", scope_value=F.concat_ws("/", "source", F.col("day").cast("string")),
                  basis=F.col("files"), detail=F.concat(F.round(F.col("b") / 1e6, 2).cast("string"), F.lit(" MB"))))
    sv = man.groupBy(F.to_date("run_time_utc").alias("day")).agg(
        F.sum("rows").alias("rows"), F.sum("input").alias("inp"), F.count(F.lit(1)).alias("runs"),
        F.sum((F.col("rows") > 0).cast("int")).alias("with_data"))
    k.append(_kpi(sv, "E4", "volume", "New or changed Silver rows per day", "rows", F.col("rows"), scope="day",
                  scope_value=F.col("day").cast("string"), basis=F.col("runs"),
                  detail=F.concat(F.col("with_data").cast("string"), F.lit(" of "), F.col("runs").cast("string"),
                                  F.lit(" Silver runs found new data; input rows "), F.col("inp").cast("string"))))
    curv = cur.groupBy("source").agg(F.count(F.lit(1)).alias("n"), F.countDistinct("storm_id").alias("storms"))
    k.append(_kpi(curv, "E5", "volume", "Records in the current Silver view", "records", F.col("n"), scope="source",
                  scope_value=F.col("source").cast("string"), basis=F.col("storms"),
                  detail=F.concat(F.col("storms").cast("string"), F.lit(" storms")), story="US-04"))

    out = reduce(lambda a, b: a.unionByName(b), k)
    out = out.withColumn("gold_run_ts", F.lit(run_ts)) \
             .withColumn("computed_at_utc", F.to_timestamp(F.lit(now.strftime("%Y-%m-%d %H:%M:%S"))))
    return out.orderBy("kpi_id", "scope_value")
