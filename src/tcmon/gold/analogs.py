"""Storm Analog Similarity (SAS), the core indicator of Team 4 (Workshop 1, Sec. "Indicator Proposal").

For every current storm, each historical storm of the same basin is compared with the
last 48 h of the current track:

    D   = w1 * mean_track_km / d0 + w2 * mean|dV| / V0 + w3 * mean|dTheta| / theta0 + w4 * |dDoy| / t0
    SAS = 100 * exp(-D)

* current window: the fixes at 00/06/12/18 UTC of the last 48 h (up to 9 fixes, offsets k = 0..8);
* candidates: IBTrACS since1980, same basin, TRACK_TYPE = main (provisional tracks of the running
  seasons are therefore excluded), not the current storm, ended at least 15 days before the current
  fix, and with at least one 6-hourly fix within 300 km of the current position;
* sliding window: every 6-hourly fix of a candidate is tried as the anchor matching the current
  fix (k = 0); the candidate fixes at anchor - 6k h are paired with the current fixes; windows
  that do not cover every current offset are dropped; the anchor with the lowest D is kept;
* heading = bearing of the 6-h displacement ending at the fix; dDoy is the circular day-of-year
  difference between the current fix and the anchor;
* weights are equal and sum to one; a wind or heading term without data (old storms of some basins
  have no wind in IBTrACS) counts as one reference unit (15 kt, 45 degrees): a neutral penalty, so
  that an analog with unknown intensity can never look closer than one with a known intensity
  (first Gold run, 19 Nov 2026: without it, 1980s North Indian storms with no wind ranked first);
* bands (Workshop 1, provisional): SAS >= 70 high, 40-69 moderate, < 40 low.

SAS describes resemblance with past storms. It is NOT a forecast; the "outcome" columns only
report what the analogs did after the matching point.

`find_analogs` is the PySpark implementation used by the Gold DAG; `find_analogs_pandas` is a
straightforward pandas twin used by the tests to check the Spark result on the same input.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

STEP_H = 6
STEP_S = STEP_H * 3600
WINDOW_H = 48
N_OFFSETS = WINDOW_H // STEP_H + 1           # k = 0..8
CANDIDATE_RADIUS_KM = 300.0
MIN_GAP_DAYS = 15
D0_KM, V0_KT, THETA0_DEG, T0_DAYS = 250.0, 15.0, 45.0, 30.0
WEIGHTS = {"track": 0.25, "wind": 0.25, "heading": 0.25, "doy": 0.25}
TOP_N = 5
MISSING_TERM = 1.0            # normalized penalty when a wind or heading difference cannot be computed
OUTCOME_H = (24, 48, 72)
BANDS = ((70.0, "high"), (40.0, "moderate"), (0.0, "low"))


def band(sas: float | None) -> str | None:
    if sas is None or (isinstance(sas, float) and math.isnan(sas)):
        return None
    return next(name for lo, name in BANDS if sas >= lo)


# =========================================================================== Spark
def _synoptic(sdf, id_col, time_col, lat_col, lon_col, wind_col, extra=()):
    """6-hourly fixes with the heading of the 6-h move that ends at each fix."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    from .spark import bearing_deg

    s = sdf.filter((F.hour(time_col) % STEP_H == 0) & (F.minute(time_col) == 0) & (F.second(time_col) == 0))
    s = s.select(F.col(id_col).cast("string").alias("sid"), F.col(time_col).alias("ts"),
                 F.unix_timestamp(time_col).alias("t"), F.col(lat_col).cast("double").alias("lat"),
                 F.col(lon_col).cast("double").alias("lon"), F.col(wind_col).cast("double").alias("wind"),
                 *[F.col(c) for c in extra])
    s = s.dropDuplicates(["sid", "t"])
    w = Window.partitionBy("sid").orderBy("t")
    s = s.withColumn("_plat", F.lag("lat").over(w)).withColumn("_plon", F.lag("lon").over(w)) \
         .withColumn("_pt", F.lag("t").over(w))
    s = s.withColumn("heading", F.when(F.col("t") - F.col("_pt") == STEP_S,
                                       bearing_deg(F.col("_plat"), F.col("_plon"), F.col("lat"), F.col("lon"))))
    return s.drop("_plat", "_plon", "_pt")


def current_windows(silver_latest):
    """Last-48-h window (k = 0..8) of every IBTrACS storm in the current Silver view."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    fixes = silver_latest.filter((F.col("source") == "ibtracs_active") & (F.col("event_type") == "tc_track_fix")
                                 & ~F.col("is_spur"))
    syn = _synoptic(fixes, "storm_id", "timestamp_utc", "latitude", "longitude", "wind_kt",
                    extra=("storm_name", "basin"))
    w = Window.partitionBy("sid")
    syn = syn.withColumn("t0", F.max("t").over(w)).filter(F.col("t") >= F.col("t0") - WINDOW_H * 3600)
    syn = syn.withColumn("k", ((F.col("t0") - F.col("t")) / STEP_S).cast("int"))
    syn = syn.withColumn("n_cur", F.count(F.lit(1)).over(w))
    return syn.select(F.col("sid").alias("storm_id"), F.col("storm_name").cast("string").alias("storm_name"),
                      F.col("basin").cast("string").alias("basin"), "k", "t", "t0", "ts",
                      F.col("lat").alias("c_lat"), F.col("lon").alias("c_lon"), F.col("wind").alias("c_wind"),
                      F.col("heading").alias("c_head"), "n_cur")


def reference_synoptic(reference):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    main = reference.filter((F.col("track_type") == "main") & ~F.col("is_spur"))
    syn = _synoptic(main, "sid", "iso_time", "lat", "lon", "wind_kt",
                    extra=("basin", "name", "season", "dist2land_km"))
    w = Window.partitionBy("sid")
    return syn.withColumn("t_end", F.max("t").over(w)).withColumn("life_max_wind", F.max("wind").over(w))


def find_analogs(cur_win, ref_syn, top_n: int = TOP_N):
    """Returns (analogs, all_scores): top_n analogs per storm and the best window of every candidate."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    from .spark import circular_diff, haversine_km

    now_fix = cur_win.filter("k = 0").select("storm_id", "basin", "t0", "c_lat", "c_lon")
    r = ref_syn.alias("r")
    near = now_fix.alias("c").join(r, F.col("r.basin") == F.col("c.basin")) \
        .filter((F.col("r.sid") != F.col("c.storm_id"))
                & (F.col("r.t_end") < F.col("c.t0") - MIN_GAP_DAYS * 86400)
                & (haversine_km(F.col("c.c_lat"), F.col("c.c_lon"), F.col("r.lat"), F.col("r.lon"))
                   <= CANDIDATE_RADIUS_KM)) \
        .select(F.col("c.storm_id").alias("storm_id"), F.col("r.sid").alias("sid")).distinct()
    anchors = near.join(ref_syn.select("sid", F.col("t").alias("ta")), "sid")
    pairs = anchors.join(cur_win, "storm_id") \
        .withColumn("t_ref", F.col("ta") - F.col("k") * STEP_S)
    pairs = pairs.join(ref_syn.select("sid", F.col("t").alias("t_ref"), F.col("lat").alias("r_lat"),
                                      F.col("lon").alias("r_lon"), F.col("wind").alias("r_wind"),
                                      F.col("heading").alias("r_head")), ["sid", "t_ref"])
    pairs = pairs.withColumn("dist", haversine_km(F.col("c_lat"), F.col("c_lon"), F.col("r_lat"), F.col("r_lon"))) \
                 .withColumn("dv", F.abs(F.col("c_wind") - F.col("r_wind"))) \
                 .withColumn("dth", circular_diff(F.col("c_head"), F.col("r_head"), 360.0))
    agg = pairs.groupBy("storm_id", "sid", "ta").agg(
        F.count(F.lit(1)).alias("n_pairs"), F.first("n_cur").alias("n_cur"), F.first("t0").alias("t0"),
        F.avg("dist").alias("track_km"), F.avg("dv").alias("dv_kt"), F.avg("dth").alias("dtheta_deg"))
    agg = agg.filter(F.col("n_pairs") == F.col("n_cur"))
    doy = lambda c: F.dayofyear(F.timestamp_seconds(c))   # noqa: E731  (session time zone is UTC)
    agg = agg.withColumn("ddoy_days", circular_diff(doy(F.col("t0")).cast("double"), doy(F.col("ta")).cast("double"), 365.0))
    terms = {"track": F.col("track_km") / D0_KM, "wind": F.col("dv_kt") / V0_KT,
             "heading": F.col("dtheta_deg") / THETA0_DEG, "doy": F.col("ddoy_days") / T0_DAYS}
    D = sum(F.lit(WEIGHTS[n]) * F.coalesce(terms[n], F.lit(MISSING_TERM)) for n in terms)
    agg = agg.withColumn("D", D).withColumn("sas", F.lit(100.0) * F.exp(-F.col("D")))
    best = agg.withColumn("_rn", F.row_number().over(
        Window.partitionBy("storm_id", "sid").orderBy("D", "ta"))).filter("_rn = 1").drop("_rn")
    best = best.withColumn("rank", F.row_number().over(Window.partitionBy("storm_id").orderBy("D", "sid")))
    info = ref_syn.select("sid", F.col("t").alias("ta"), F.col("name").alias("analog_name"),
                          F.col("season").alias("analog_season"), F.col("wind").alias("analog_wind_at_anchor"),
                          F.col("lat").alias("anchor_lat"), F.col("lon").alias("anchor_lon"), "life_max_wind")
    best = best.join(info, ["sid", "ta"], "left")
    return best.filter(F.col("rank") <= top_n), best


def analog_outcomes(analogs, ref_syn):
    """What each analog did in the 72 h after its matching point (descriptive, not a forecast)."""
    from pyspark.sql import functions as F

    a = analogs.select("storm_id", "sid", "rank", "ta", "analog_wind_at_anchor")
    after = a.join(ref_syn.select("sid", "t", "wind", "dist2land_km", "lat", "lon"), "sid") \
             .filter((F.col("t") > F.col("ta")) & (F.col("t") <= F.col("ta") + max(OUTCOME_H) * 3600))
    aggs = [F.max("wind").alias("max_wind_72h"), F.min("dist2land_km").alias("min_dist2land_72h"),
            F.count(F.lit(1)).alias("fixes_72h")]
    for h in OUTCOME_H:
        aggs.append(F.max(F.when(F.col("t") == F.col("ta") + h * 3600, F.col("wind"))).alias(f"wind_{h}h"))
    out = a.join(after.groupBy("storm_id", "sid").agg(*aggs), ["storm_id", "sid"], "left")
    for h in OUTCOME_H:
        out = out.withColumn(f"dwind_{h}h", F.col(f"wind_{h}h") - F.col("analog_wind_at_anchor"))
    return out.withColumn("landfall_72h", F.col("min_dist2land_72h") <= 0)


def analog_tracks(analogs, ref_syn, cur_win):
    """Track points for the overlay chart (US-05): analog fixes from anchor - 48 h to anchor + 72 h
    (offset_h relative to the anchor) and the current window (offset_h <= 0)."""
    from pyspark.sql import functions as F

    a = analogs.select("storm_id", "sid", "rank", "ta")
    pts = a.join(ref_syn.select("sid", "t", "ts", "lat", "lon", "wind"), "sid") \
           .filter((F.col("t") >= F.col("ta") - WINDOW_H * 3600) & (F.col("t") <= F.col("ta") + max(OUTCOME_H) * 3600)) \
           .select("storm_id", "sid", "rank", "ts", "lat", "lon", "wind",
                   ((F.col("t") - F.col("ta")) / 3600).alias("offset_h"))
    cur = cur_win.select("storm_id", F.col("storm_id").alias("sid"), F.lit(0).alias("rank"), "ts",
                         F.col("c_lat").alias("lat"), F.col("c_lon").alias("lon"), F.col("c_wind").alias("wind"),
                         ((F.col("t") - F.col("t0")) / 3600).alias("offset_h"))
    return pts.unionByName(cur)


# ========================================================================== pandas
def _haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * 6371.0088 * np.arcsin(np.sqrt(np.minimum(1.0, a)))


def _bearing(lat1, lon1, lat2, lon2):
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dl = np.radians(lon2) - np.radians(lon1)
    y = np.sin(dl) * np.cos(p2)
    x = np.cos(p1) * np.sin(p2) - np.sin(p1) * np.cos(p2) * np.cos(dl)
    return (np.degrees(np.arctan2(y, x)) + 360.0) % 360.0


def _circ(a, b, period):
    d = np.abs(a - b) % period
    return np.minimum(d, period - d)


def _syn_pd(df, id_col, time_col, lat_col, lon_col, wind_col):
    t = pd.to_datetime(df[time_col], utc=True)
    m = (t.dt.hour % STEP_H == 0) & (t.dt.minute == 0) & (t.dt.second == 0)
    s = pd.DataFrame({"sid": df.loc[m, id_col].astype(str), "ts": t[m],
                      "lat": df.loc[m, lat_col].astype(float), "lon": df.loc[m, lon_col].astype(float),
                      "wind": pd.to_numeric(df.loc[m, wind_col], errors="coerce").astype(float)})
    s["t"] = s["ts"].map(lambda x: int(x.timestamp())).astype("int64")
    s = s.drop_duplicates(["sid", "t"]).sort_values(["sid", "t"]).reset_index(drop=True)
    prev = s.groupby("sid")[["lat", "lon", "t"]].shift()
    head = _bearing(prev["lat"], prev["lon"], s["lat"], s["lon"])
    s["heading"] = np.where((s["t"] - prev["t"]) == STEP_S, head, np.nan)
    return s


def find_analogs_pandas(silver: pd.DataFrame, reference: pd.DataFrame, top_n: int = TOP_N) -> pd.DataFrame:
    fx = silver[(silver["source"] == "ibtracs_active") & (silver["event_type"] == "tc_track_fix") & ~silver["is_spur"].astype(bool)]
    cur = _syn_pd(fx, "storm_id", "timestamp_utc", "latitude", "longitude", "wind_kt")
    basin = fx.drop_duplicates("storm_id").set_index("storm_id")["basin"].astype(str)
    ref = reference[(reference["track_type"] == "main") & ~reference["is_spur"].astype(bool)]
    rs = _syn_pd(ref, "sid", "iso_time", "lat", "lon", "wind_kt")
    meta = ref.drop_duplicates("sid").set_index("sid")
    rs["basin"] = rs["sid"].map(meta["basin"].astype(str))
    rs["t_end"] = rs.groupby("sid")["t"].transform("max")
    by_sid_t = rs.set_index(["sid", "t"])
    out = []
    for sid_c, g in cur.groupby("sid"):
        t0 = g["t"].max()
        win = g[g["t"] >= t0 - WINDOW_H * 3600].copy()
        win["k"] = ((t0 - win["t"]) // STEP_S).astype(int)
        last = win[win["k"] == 0].iloc[0]
        cand_pool = rs[(rs["basin"] == basin[sid_c]) & (rs["sid"] != sid_c) & (rs["t_end"] < t0 - MIN_GAP_DAYS * 86400)]
        near = cand_pool[_haversine(last["lat"], last["lon"], cand_pool["lat"], cand_pool["lon"]) <= CANDIDATE_RADIUS_KM]
        rows = []
        for sid in near["sid"].unique():
            for ta in rs.loc[rs["sid"] == sid, "t"]:
                keys = [(sid, ta - k * STEP_S) for k in win["k"]]
                if not all(key in by_sid_t.index for key in keys):
                    continue
                r = by_sid_t.loc[keys]
                dist = _haversine(win["lat"].to_numpy(), win["lon"].to_numpy(), r["lat"].to_numpy(), r["lon"].to_numpy()).mean()
                dv = np.nanmean(np.abs(win["wind"].to_numpy() - r["wind"].to_numpy())) \
                    if np.isfinite(win["wind"].to_numpy() - r["wind"].to_numpy()).any() else np.nan
                dthv = _circ(win["heading"].to_numpy(), r["heading"].to_numpy(), 360.0)
                dth = np.nanmean(dthv) if np.isfinite(dthv).any() else np.nan
                d0 = pd.Timestamp(t0, unit="s", tz="UTC").dayofyear
                da = pd.Timestamp(ta, unit="s", tz="UTC").dayofyear
                ddoy = float(_circ(np.array([d0]), np.array([da]), 365.0)[0])
                terms = {"track": dist / D0_KM, "wind": dv / V0_KT, "heading": dth / THETA0_DEG, "doy": ddoy / T0_DAYS}
                D = sum(WEIGHTS[n] * (MISSING_TERM if v is None or np.isnan(v) else v) for n, v in terms.items())
                rows.append({"storm_id": sid_c, "sid": sid, "ta": int(ta), "track_km": dist, "dv_kt": dv,
                             "dtheta_deg": dth, "ddoy_days": ddoy, "D": D, "sas": 100 * math.exp(-D)})
        if not rows:
            continue
        df = pd.DataFrame(rows).sort_values(["D", "ta"]).drop_duplicates("sid").sort_values(["D", "sid"])
        df["rank"] = range(1, len(df) + 1)
        out.append(df.head(top_n))
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame(
        columns=["storm_id", "sid", "ta", "track_km", "dv_kt", "dtheta_deg", "ddoy_days", "D", "sas", "rank"])
