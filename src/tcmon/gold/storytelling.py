"""Storytelling aggregations of the Gold layer, computed with PySpark.

Output: datalake_gold/storytelling_YYYYMMDD_HHMMSS.parquet, a tidy table:

    section, user_story, storm_id, storm_name, basin, item_id, item_name, rank,
    metric, value, value_text, timestamp_utc, latitude, longitude, offset_h,
    gold_run_ts, computed_at_utc

Sections and the Workshop 1 user stories they serve (the story is the comparison of
each current storm with similar historical storms):

| section            | user story | content                                                          |
| ------------------ | ---------- | ---------------------------------------------------------------- |
| current_status     | US-01/06   | last fix, intensity, category, motion, GDACS alert of every storm |
| whats_new          | US-02      | records added or revised in the last 24 h of processing           |
| intensity_track    | US-05      | wind and pressure of every fix of the current storms              |
| analogs            | US-03      | top-5 historical analogs (SAS) with every term of the distance    |
| analog_outcomes    | US-03/05   | what each analog did in the next 72 h (descriptive, not forecast) |
| analog_tracks      | US-05      | current window and analog tracks aligned on the matching point    |
| gdacs_alerts       | US-01/02   | alert level and score of every GDACS episode                      |
| source_comparison  | US-04      | IBTrACS vs GDACS for the same storm: time lag and intensity       |
| basin_climatology  | US-08      | climatology of the basin and percentile of the current storm      |
| narrative          | all        | one plain-language paragraph per storm built from the rows above  |
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from functools import reduce

from . import analogs as an
from .spark import compass16

log = logging.getLogger(__name__)

STORY_COLUMNS = ["section", "user_story", "storm_id", "storm_name", "basin", "item_id", "item_name", "rank",
                 "metric", "value", "value_text", "timestamp_utc", "latitude", "longitude", "offset_h"]
CURRENT_WINDOW_DAYS = 7          # a storm is "current" if its last fix is within 7 days of the newest fix
NAME_MATCH_DAYS = 10
BASIN_NAMES = {"NA": "North Atlantic", "EP": "Eastern North Pacific", "WP": "Western North Pacific",
               "NI": "North Indian", "SI": "South Indian", "SP": "South Pacific", "SA": "South Atlantic"}
SSHS_TEXT = {-1: "tropical depression", 0: "tropical storm", 1: "Category 1", 2: "Category 2",
             3: "Category 3", 4: "Category 4", 5: "Category 5"}


def _c(x, cast):
    from pyspark.sql import functions as F
    if x is None:
        return F.lit(None).cast(cast)
    if isinstance(x, str):
        return F.col(x).cast(cast)
    return x.cast(cast)


def melt(sdf, section, story, metrics: dict, *, storm_id="storm_id", storm_name="storm_name", basin="basin",
         item_id=None, item_name=None, rank=None, ts=None, lat=None, lon=None, offset=None):
    """Wide -> tidy. metrics = {metric_name: (value Column|str|None, text Column|str|None)}.

    All metrics of a row are packed into one array of structs and exploded, so the source
    plan appears once per section (a union of one select per metric repeated the whole
    lineage for every metric and made the query plan too large for the driver). The section
    is materialized with a local checkpoint, which cuts its lineage before the final union."""
    from pyspark.sql import functions as F
    packed = F.array(*[
        F.struct(F.lit(metric).alias("metric"), _c(val, "double").alias("value"),
                 _c(txt, "string").alias("value_text"))
        for metric, (val, txt) in metrics.items()])
    wide = sdf.select(
        F.lit(section).alias("section"), F.lit(story).alias("user_story"),
        _c(storm_id, "string").alias("storm_id"), _c(storm_name, "string").alias("storm_name"),
        _c(basin, "string").alias("basin"), _c(item_id, "string").alias("item_id"),
        _c(item_name, "string").alias("item_name"), _c(rank, "int").alias("rank"),
        _c(ts, "timestamp").alias("timestamp_utc"), _c(lat, "double").alias("latitude"),
        _c(lon, "double").alias("longitude"), _c(offset, "double").alias("offset_h"),
        F.explode(packed).alias("_m"))
    tidy = wide.select(*[F.col(f"_m.{c}").alias(c) if c in ("metric", "value", "value_text") else F.col(c)
                         for c in STORY_COLUMNS])
    return tidy.localCheckpoint(eager=True)


def _sshs_text_col(col):
    from pyspark.sql import functions as F
    expr = F.lit(None).cast("string")
    for k, v in sorted(SSHS_TEXT.items()):
        expr = F.when(col == k, F.lit(v)).otherwise(expr)
    return expr


def build(spark, run_ts: str, reference_available: bool):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    from .spark import read_reference, read_silver

    now = datetime.now(timezone.utc)
    cur = read_silver(spark, latest_only=True).cache()
    allv = read_silver(spark, latest_only=False)
    sections = []

    # ---------------------------------------------------------- current storms
    ib = cur.filter((F.col("source") == "ibtracs_active") & (F.col("event_type") == "tc_track_fix") & ~F.col("is_spur"))
    newest_df = ib.agg(F.max("timestamp_utc").alias("newest_fix"), F.month(F.max("timestamp_utc")).alias("month"))
    month = newest_df.collect()[0]["month"] or now.month
    w_last = Window.partitionBy("storm_id").orderBy(F.col("timestamp_utc").desc())
    w_all = Window.partitionBy("storm_id")
    last = ib.withColumn("_rn", F.row_number().over(w_last)) \
             .withColumn("max_wind_so_far", F.max("wind_kt").over(w_all)) \
             .withColumn("first_fix", F.min("timestamp_utc").over(w_all)) \
             .withColumn("n_fixes", F.count(F.lit(1)).over(w_all)).filter("_rn = 1").drop("_rn")
    last = last.crossJoin(newest_df.select("newest_fix")) \
               .filter(F.unix_timestamp("timestamp_utc") >= F.unix_timestamp("newest_fix") - CURRENT_WINDOW_DAYS * 86400) \
               .drop("newest_fix")
    current_ids = [r["storm_id"] for r in last.select("storm_id").collect()]
    last = last.cache()

    gd = cur.filter(F.col("source") == "gdacs_tc")
    w_ep = Window.partitionBy("storm_id").orderBy(F.col("gdacs_episode_id").desc(), F.col("timestamp_utc").desc())
    gd_last = gd.withColumn("_rn", F.row_number().over(w_ep)).filter("_rn = 1").drop("_rn").select(
        F.col("storm_id").alias("gdacs_event_id"), F.col("storm_name").alias("g_name"),
        F.col("timestamp_utc").alias("g_time"), F.col("alert_level").cast("string").alias("g_alert"),
        F.col("alert_score").alias("g_score"), F.col("peak_wind_kt").alias("g_peak"),
        F.col("is_current").alias("g_current"), F.col("gdacs_episode_id").alias("g_episode"))
    joined = last.join(gd_last, (F.col("storm_name") == F.col("g_name")) &
                       (F.abs(F.unix_timestamp("g_time") - F.unix_timestamp("timestamp_utc")) <= NAME_MATCH_DAYS * 86400),
                       "left")
    joined = joined.withColumn("status", F.when(F.col("g_current") == True, F.lit("active (GDACS current)"))  # noqa: E712
                               .when(F.col("g_current") == False, F.lit("not current in GDACS"))  # noqa: E712
                               .otherwise(F.lit("no GDACS match"))).cache()
    sections.append(melt(joined, "current_status", "US-01/US-06", {
        "wind_kt": ("wind_kt", "wind_avg_period"),
        "sshs_category": ("sshs_category", _sshs_text_col(F.col("sshs_category"))),
        "pressure_hpa": ("pressure_hpa", None),
        "max_wind_so_far_kt": ("max_wind_so_far", None),
        "storm_speed_kt": ("storm_speed_kt", F.when(F.col("motion_derived"), F.lit("derived from fixes"))),
        "storm_dir_deg": ("storm_dir_deg", None),
        "dist2land_km": ("dist2land_km", None),
        "nature": (None, "nature"),
        "is_provisional": (F.col("is_provisional").cast("int"), "track_type"),
        "fixes": ("n_fixes", F.date_format("first_fix", "yyyy-MM-dd HH:mm 'UTC'")),
        "gdacs_alert": ("g_score", "g_alert"),
        "gdacs_peak_wind_kt": ("g_peak", F.concat(F.lit("episode "), F.col("g_episode").cast("string"))),
        "status": (F.col("g_current").cast("int"), "status"),
    }, item_id="gdacs_event_id", ts="timestamp_utc", lat="latitude", lon="longitude"))

    # ------------------------------------------------------------- what's new
    recent = allv.crossJoin(allv.agg(F.max("processed_at_utc").alias("newest_proc"))) \
                 .filter(F.unix_timestamp("processed_at_utc") >= F.unix_timestamp("newest_proc") - 24 * 3600)
    wn = recent.groupBy("source", "storm_id", "storm_name").agg(
        F.sum((~F.col("is_revision")).cast("int")).alias("new"), F.sum(F.col("is_revision").cast("int")).alias("rev"),
        F.max("timestamp_utc").alias("newest_obs"), F.max("processed_at_utc").alias("proc"))
    sections.append(melt(wn, "whats_new", "US-02", {
        "new_records_24h": ("new", F.col("source").cast("string")),
        "revised_records_24h": ("rev", F.col("source").cast("string"))},
        basin=None, ts="newest_obs"))

    # --------------------------------------------------------- intensity track
    track = ib.filter(F.col("storm_id").isin(current_ids)) if current_ids else ib.limit(0)
    sections.append(melt(track, "intensity_track", "US-05", {
        "wind_kt": ("wind_kt", "nature"), "pressure_hpa": ("pressure_hpa", None)},
        ts="timestamp_utc", lat="latitude", lon="longitude"))

    # ------------------------------------------------------------- GDACS alerts
    sections.append(melt(gd, "gdacs_alerts", "US-01/US-02", {
        "alert_score": ("alert_score", F.col("alert_level").cast("string")),
        "peak_wind_kt": ("peak_wind_kt", F.when(F.col("is_current"), F.lit("current")).otherwise(F.lit("not current")))},
        basin=None, item_id=F.col("gdacs_episode_id").cast("string"), item_name="storm_name_raw",
        ts="timestamp_utc", lat="latitude", lon="longitude"))

    # -------------------------------------------------------- source comparison
    ib_all = cur.filter((F.col("source") == "ibtracs_active") & F.col("storm_name").isNotNull()) \
                .groupBy("storm_name").agg(F.max("timestamp_utc").alias("ib_last"), F.max("wind_kt").alias("ib_max"),
                                           F.first("storm_id").alias("storm_id"), F.first("basin").alias("basin"))
    gd_all = gd.filter(F.col("storm_name").isNotNull()).groupBy("storm_name").agg(
        F.max("timestamp_utc").alias("gd_last"), F.max("peak_wind_kt").alias("gd_peak"))
    cmp_ = ib_all.join(gd_all, "storm_name").filter(
        F.abs(F.unix_timestamp("gd_last") - F.unix_timestamp("ib_last")) <= NAME_MATCH_DAYS * 86400)
    sections.append(melt(cmp_, "source_comparison", "US-04", {
        "ibtracs_max_wind_kt": ("ib_max", F.date_format("ib_last", "yyyy-MM-dd HH:mm 'UTC'")),
        "gdacs_peak_wind_kt": ("gd_peak", F.date_format("gd_last", "yyyy-MM-dd HH:mm 'UTC'")),
        "peak_difference_kt": (F.col("gd_peak") - F.col("ib_max"), F.lit("GDACS minus IBTrACS")),
        "gdacs_lead_h": ((F.unix_timestamp("gd_last") - F.unix_timestamp("ib_last")) / 3600,
                         F.lit("hours by which GDACS is ahead of the best-track file"))}))

    # ------------------------------------------------- analogs and climatology
    analog_rows, outcome_rows, clim_rows = [], [], []
    if reference_available and current_ids:
        ref = read_reference(spark)
        ref_syn = an.reference_synoptic(ref).cache()
        cur_win = an.current_windows(cur.filter(F.col("storm_id").isin(current_ids))).cache()
        top, _ = an.find_analogs(cur_win, ref_syn)
        top = top.join(last.select("storm_id", "storm_name", "basin", F.col("wind_kt").alias("cur_wind")), "storm_id").cache()
        top = top.withColumn("band", F.when(F.col("sas") >= 70, F.lit("high"))
                             .when(F.col("sas") >= 40, F.lit("moderate")).otherwise(F.lit("low")))
        top = top.withColumn("analog_label", F.concat_ws(" ", F.coalesce(F.col("analog_name"), F.lit("UNNAMED")),
                                                         F.concat(F.lit("("), F.col("analog_season").cast("string"), F.lit(")"))))
        sections.append(melt(top, "analogs", "US-03", {
            "sas": ("sas", "band"), "distance_D": ("D", None), "mean_track_km": ("track_km", None),
            "mean_abs_dwind_kt": ("dv_kt", None), "mean_abs_dheading_deg": ("dtheta_deg", None),
            "doy_difference_days": ("ddoy_days", None), "analog_wind_at_match_kt": ("analog_wind_at_anchor", None),
            "analog_lifetime_max_wind_kt": ("life_max_wind", None), "pairs_used": ("n_pairs", None)},
            item_id="sid", item_name="analog_label", rank="rank", ts=F.timestamp_seconds("ta"),
            lat="anchor_lat", lon="anchor_lon"))
        pctl = top.groupBy("storm_id", "storm_name", "basin").agg(
            (F.lit(100.0) * F.avg((F.col("analog_wind_at_anchor") <= F.col("cur_wind")).cast("double"))).alias("pct"),
            F.avg("sas").alias("mean_sas"), F.max("sas").alias("best_sas"), F.count(F.lit(1)).alias("n"))
        sections.append(melt(pctl, "analogs", "US-03", {
            "current_wind_percentile_among_analogs": ("pct", F.lit("% of analogs at or below the current wind at their match")),
            "mean_sas_top5": ("mean_sas", None), "best_sas": ("best_sas", None), "analogs_found": ("n", None)},
            item_id=F.lit("TOP5"), item_name=F.lit("summary of the top analogs")))

        out = an.analog_outcomes(top, ref_syn).join(top.select("storm_id", "sid", "storm_name", "basin", "analog_label"),
                                                    ["storm_id", "sid"])
        metrics = {f"dwind_{h}h_kt": (f"dwind_{h}h", None) for h in an.OUTCOME_H}
        metrics.update({f"wind_{h}h_kt": (f"wind_{h}h", None) for h in an.OUTCOME_H})
        metrics.update({"max_wind_72h_kt": ("max_wind_72h", None), "min_dist2land_72h_km": ("min_dist2land_72h", None),
                        "landfall_72h": (F.col("landfall_72h").cast("int"), None)})
        sections.append(melt(out, "analog_outcomes", "US-03/US-05", metrics, item_id="sid", item_name="analog_label",
                             rank="rank"))
        summ = out.groupBy("storm_id", "storm_name", "basin").agg(
            F.expr("percentile_approx(dwind_48h, 0.5)").alias("med48"),
            F.coalesce(F.sum(F.col("landfall_72h").cast("int")), F.lit(0)).alias("lf"),
            F.coalesce(F.sum((F.col("dwind_48h") > 0).cast("int")), F.lit(0)).alias("up"),
            F.coalesce(F.sum((F.col("dwind_48h") < 0).cast("int")), F.lit(0)).alias("down"),
            F.count("dwind_48h").alias("with48"), F.count(F.lit(1)).alias("n"))
        sections.append(melt(summ, "analog_outcomes", "US-03/US-05", {
            "median_dwind_48h_kt": ("med48", F.lit("median change of the analogs 48 h after their match")),
            "analogs_with_landfall_72h": ("lf", None), "analogs_intensifying_48h": ("up", None),
            "analogs_tracked_48h_after_match": ("with48", None),
            "analogs_weakening_48h": ("down", None)}, item_id=F.lit("TOP5"), item_name=F.lit("summary")))

        tr = an.analog_tracks(top, ref_syn, cur_win).join(
            top.select("storm_id", "sid", "analog_label").unionByName(
                cur_win.select("storm_id", F.col("storm_id").alias("sid"),
                               F.concat(F.coalesce(F.col("storm_name"), F.col("storm_id")), F.lit(" (current)"))
                               .alias("analog_label")).distinct()),
            ["storm_id", "sid"], "left") \
            .join(last.select("storm_id", "storm_name", "basin"), "storm_id")
        sections.append(melt(tr, "analog_tracks", "US-05", {"track_point": ("wind", None)}, item_id="sid",
                             item_name="analog_label", rank="rank", ts="ts", lat="lat", lon="lon", offset="offset_h"))

        # basin climatology (US-08)
        basins = [r["basin"] for r in last.select(F.col("basin").cast("string").alias("basin")).distinct().collect()]
        storms = ref_syn.filter(F.col("basin").isin(basins)).groupBy("basin", "sid", "season").agg(
            F.max("wind").alias("lmi"), F.collect_set(F.month("ts")).alias("months"))
        per_basin = storms.groupBy("basin").agg(
            F.countDistinct("season").alias("seasons"), F.count(F.lit(1)).alias("storms"),
            F.expr("percentile_approx(lmi, 0.5)").alias("med_lmi"),
            F.avg((F.col("lmi") >= 64).cast("double")).alias("share64"),
            F.avg(F.array_contains("months", month).cast("double")).alias("share_month"))
        sections.append(melt(per_basin, "basin_climatology", "US-08", {
            "storms_per_season": (F.col("storms") / F.col("seasons"), F.lit("main tracks, 1980 to the last final season")),
            "median_lifetime_max_wind_kt": ("med_lmi", None),
            "share_reaching_64kt_pct": (F.col("share64") * 100, None),
            "share_active_in_current_month_pct": (F.col("share_month") * 100, F.lit(f"month {month}"))},
            storm_id=None, storm_name=None, basin="basin"))
        rank_cur = last.select("storm_id", "storm_name", "basin", "max_wind_so_far").join(
            storms.select("basin", "lmi"), "basin") \
            .groupBy("storm_id", "storm_name", "basin").agg(
                (F.lit(100.0) * F.avg((F.col("lmi") <= F.col("max_wind_so_far")).cast("double"))).alias("pct"),
                F.count(F.lit(1)).alias("n"))
        sections.append(melt(rank_cur, "basin_climatology", "US-08", {
            "current_max_wind_percentile_in_basin": ("pct", F.lit("% of historical storms whose lifetime max wind is at or below"))}))
        analog_rows = top.orderBy("storm_id", "rank").collect()
        outcome_rows = summ.collect()
        clim_rows = rank_cur.collect()

    # ---------------------------------------------------------------- narrative
    texts = _narratives(joined.collect(), analog_rows, outcome_rows, clim_rows, reference_available)
    if texts:
        sections.append(melt(spark.createDataFrame(texts, "storm_id string, storm_name string, basin string, text string"),
                             "narrative", "all", {"summary": (None, "text")}))

    out = reduce(lambda a, b: a.unionByName(b), sections)
    out = out.withColumn("gold_run_ts", F.lit(run_ts)) \
             .withColumn("computed_at_utc", F.to_timestamp(F.lit(now.strftime("%Y-%m-%d %H:%M:%S"))))
    return out.orderBy("section", "storm_id", "rank", "item_id", "metric", "timestamp_utc")


def _fmt(v, nd=0, unit=""):
    if v is None:
        return "n/a"
    return f"{v:.{nd}f}{unit}"


def _latlon(lat, lon):
    return f"{abs(lat):.1f}°{'N' if lat >= 0 else 'S'} {abs(lon):.1f}°{'E' if lon >= 0 else 'W'}"


def _narratives(status_rows, analog_rows, outcome_rows, clim_rows, reference_available) -> list[tuple]:
    """Plain-language summary per storm, assembled only from the computed rows."""
    by_storm = {}
    for r in analog_rows:
        by_storm.setdefault(r["storm_id"], []).append(r)
    outc = {r["storm_id"]: r for r in outcome_rows}
    clim = {r["storm_id"]: r for r in clim_rows}
    texts = []
    for s in status_rows:
        name = s["storm_name"] or "Unnamed storm"
        basin = BASIN_NAMES.get(s["basin"], s["basin"])
        cat = SSHS_TEXT.get(s["sshs_category"]) if s["sshs_category"] is not None else None
        parts = [f"{name} ({basin}): last best-track fix {s['timestamp_utc']:%d %b %H:%M} UTC at "
                 f"{_latlon(s['latitude'], s['longitude'])}, {_fmt(s['wind_kt'], 0, ' kt')}"
                 + (f" ({cat}, 1-min wind)" if cat else f" ({s['wind_avg_period']} wind)") + "."]
        if s["storm_speed_kt"] is not None and s["storm_dir_deg"] is not None:
            parts.append(f"It was moving {compass16(s['storm_dir_deg'])} at {s['storm_speed_kt']:.0f} kt.")
        if s["g_alert"]:
            parts.append(f"GDACS alert {s['g_alert']} (episode {s['g_episode']}, peak "
                         f"{_fmt(s['g_peak'], 0, ' kt')}), {s['status']}.")
        top = by_storm.get(s["storm_id"], [])
        if top:
            best = top[0]
            parts.append(f"Closest historical analog: {best['analog_label']} with SAS {best['sas']:.0f} ({best['band']}); "
                         f"mean track separation {best['track_km']:.0f} km over the last 48 h, wind difference "
                         f"{_fmt(best['dv_kt'], 0, ' kt')}, {best['ddoy_days']:.0f} days apart in the season.")
            others = ", ".join(f"{r['analog_label']} {r['sas']:.0f}" for r in top[1:])
            if others:
                parts.append(f"Other analogs: {others}.")
            o = outc.get(s["storm_id"])
            if o and o["n"]:
                if o["with48"]:
                    parts.append(f"Of the {o['with48']} analogs still tracked 48 h after their matching point, {o['up']} "
                                 f"had intensified and {o['down']} had weakened (median change "
                                 f"{_fmt(o['med48'], 0, ' kt')}); {o['lf']} of {o['n']} reached land within 72 h.")
                else:
                    parts.append(f"None of the {o['n']} analog tracks continues 48 h beyond the matching point "
                                 f"(they dissipated or became extratropical); {o['lf']} reached land within 72 h.")
        elif reference_available:
            parts.append("No historical storm of the basin passed within 300 km with a complete 48-h window.")
        else:
            parts.append("Historical analogs are not available in this run (reference archive missing).")
        c = clim.get(s["storm_id"])
        if c and c["pct"] is not None:
            parts.append(f"Its peak so far is at or above the lifetime maximum of {c['pct']:.0f}% of the basin's "
                         f"storms since 1980.")
        parts.append("Analogs describe resemblance with past storms; they are not a forecast.")
        texts.append((s["storm_id"], s["storm_name"], s["basin"], " ".join(parts)))
    return texts
