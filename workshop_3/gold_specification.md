# Workshop 3 — Gold Layer Specification

## 1. Processing

| Item | Value |
| --- | --- |
| DAG | [`airflow/dags/gold_processing_dag.py`](../airflow/dags/gold_processing_dag.py), id `gold_processing` |
| Schedule | `40 * * * *` (hourly, 40 min after the Bronze run, once Silver has picked the batch up) |
| Engine | PySpark, **local mode** inside the Airflow task (`local[2]`, driver 2 GB, 4 shuffle partitions, UI off, UTC session) — [`src/tcmon/gold/spark.py`](../src/tcmon/gold/spark.py) |
| Input | `spark.read.parquet("datalake_silver/")` — every Silver file; folders and files starting with `_` (`_manifest.jsonl`, `_rejected/`, `_reference/`) are skipped by Spark. Each `event_id` keeps its latest version (window on `processed_at_utc`). |
| Historical baseline | `datalake_silver/_reference/ibtracs_since1980.parquet` built by `ensure_reference` from IBTrACS v04r01 since1980 (conditional GET; checked at most weekly) |
| Outputs | `datalake_gold/governance_YYYYMMDD_HHMMSS.parquet`, `datalake_gold/storytelling_YYYYMMDD_HHMMSS.parquet` (one file each: Spark writes one partition to `_tmp_*/`, the part file is renamed atomically) |
| Retention | Gold files older than 14 days are deleted by `verify_gold` (`TCMON_GOLD_RETENTION_DAYS`) |

Task graph: `run_timestamp`, `check_silver_inputs` → `ensure_reference` → `build_storytelling`;
`check_silver_inputs` → `build_governance`; both → `verify_gold`. A failure to reach NCEI does not stop the
governance table: the storytelling table is then written without the analog sections and says so in its narrative.

Both tables are **tidy** (long): one row per KPI value or per story fact, so a new KPI or section never changes the
schema, and the history of any metric can be queried across runs through `gold_run_ts`.

## 2. Governance table

`kpi_id, dimension, kpi_name, scope, scope_value, value, unit, target, status, basis_n, detail, user_story, gold_run_ts, computed_at_utc`

`status` = `ok` (target met), `warn`, `fail`, `info` (descriptive, no target) or `n/a` (no data). Dimensions follow
DAMA-DMBOK and ISO/IEC 25012.

| KPI | Dimension | Definition | Target | Why (finding / story) |
| --- | --- | --- | --- | --- |
| C1 | completeness | null rate of each of the 38 Silver fields, per source (latest versions) | 0 % for required fields | F7, US-04 |
| C2 | completeness | distinct hourly slots with a successful extraction per UTC day / 24 | ≥ 95 % | US-04 |
| C3 | completeness | successful extraction attempts / all attempts, per source | ≥ 99 % | F1 |
| U1 | uniqueness | rows already in Silver / input rows (from `_manifest.jsonl`) | info | F2, F5 |
| U2 | uniqueness | within-batch duplicates / input rows (several Bronze runs claimed by one Silver batch) | info | F2 |
| U3 | uniqueness | revisions / rows written | info | F5, US-07 |
| U4 | uniqueness | Silver rows that are older versions of an event | info | F5 |
| V1 | validity | rejected rows (no id, time or position) / rows | < 1 % | US-07 |
| V2 | validity | values outside physical bounds (set to NULL) | 0 % | — |
| V3 | validity | track fixes without wind | info | F7 |
| V4 | validity | 6-h wind change outside the climatological 3·IQR fence | info | F6 |
| V5 | validity | translation speed with \|z\| > 3 against climatology | info | F6 |
| V6 | validity | motion derived from consecutive fixes | info | US-06 |
| V7 | validity | provisional fixes | info | US-07 |
| V8 | validity | spur fixes (excluded from analogs) | info | US-03 |
| V9 | validity | Silver runs per outlier-fence basis (`reference` / `batch`) | info | F6 |
| F1 | conformity | Silver files whose Parquet schema equals the 38-field schema | 100 % | — |
| F2 | conformity | Silver files following the naming convention | 100 % | — |
| F3 | conformity | Silver files carrying `tcmon_lineage` metadata | 100 % | — |
| F4 | conformity | Silver files with 0 rows | 0 | F4 |
| F5 | conformity | Silver runs named earlier than their newest Bronze input | 0 % | F3 |
| T1 | timeliness | median delay between file timestamp and ingestion, per source | ≤ 5 min | F3 |
| T2 | timeliness | median Bronze-to-Silver latency, per source | ≤ 15 min | — |
| T3 | timeliness | age of the newest observation at Gold time, per source | ≤ 24 h | F8, US-04 |
| T4 | timeliness | hours since the last stored Bronze file (with record count) | info | US-04 |
| K1 | consistency | GDACS peak wind − IBTrACS max wind, per storm | \|diff\| ≤ 10 kt | F8 |
| K2 | consistency | GDACS newest time − IBTrACS newest fix, per storm | info | F8 |
| K3 | consistency | storms present in both sources (name match within 10 days) | ≥ 90 % | F8 |
| K4 | consistency | names changed by normalization; mean length raw → normalized | info | F9 |
| K5 | consistency | unnamed storms | info | F9 |
| E1 | efficiency | Bronze files identical to the previous file of the source (and MB stored twice) | 0 % | F2 |
| E2 | efficiency | extractions answered "not modified" (and MB downloaded) | info | F2 |
| E3 | volume | Bronze records and MB per source and day | info | — |
| E4 | volume | new or changed Silver rows per day; Silver runs with new data | info | F4 |
| E5 | volume | records and storms in the current Silver view, per source | info | US-04 |

Inputs besides Silver: Bronze envelope headers, `datalake_bronze/_state/ingestion_log_<source>.jsonl`, error bodies
in `_errors/` (and, for Workshop 2 runs, `_staging/`), `datalake_silver/_manifest.jsonl` and Parquet footers
([`src/tcmon/gold/inventory.py`](../src/tcmon/gold/inventory.py)). Only metadata is read in Python; every aggregation
runs in Spark.

## 3. Storytelling table

`section, user_story, storm_id, storm_name, basin, item_id, item_name, rank, metric, value, value_text, timestamp_utc, latitude, longitude, offset_h, gold_run_ts, computed_at_utc`

The team's story is the **comparison of each current storm with similar historical storms** (team assignment).
Every section answers one or more Workshop 1 user stories:

| Section | User story | Content |
| --- | --- | --- |
| `current_status` | US-01, US-06 | last fix of every storm whose last fix is within 7 days of the newest fix: position, wind and averaging period, Saffir–Simpson category (1-min winds only), pressure, maximum so far, motion and compass direction, distance to land, nature, provisional flag, GDACS alert, peak and current/not-current status |
| `whats_new` | US-02 | new and revised records per storm in the last 24 h of processing |
| `intensity_track` | US-05 | wind and pressure of every fix of the current storms |
| `analogs` | US-03 | top-5 analogs per storm: SAS and band, D and every term (track km, \|ΔV\|, \|Δθ\|, Δdoy), wind of the analog at the matching point, its lifetime maximum, pairs used; summary row `TOP5`: percentile of the current wind among the analogs, mean and best SAS |
| `analog_outcomes` | US-03, US-05 | wind of each analog 24/48/72 h after the matching point, change, maximum, minimum distance to land and landfall in 72 h; summary: median 48-h change, analogs intensifying / weakening / reaching land |
| `analog_tracks` | US-05 | current 48-h window (rank 0, offset ≤ 0 h) and analog fixes from −48 h to +72 h around the matching point, for the overlay of tracks and intensity curves |
| `gdacs_alerts` | US-01, US-02 | alert level, score and peak wind of every GDACS episode |
| `source_comparison` | US-04 | IBTrACS max wind vs GDACS peak, difference and GDACS lead time per storm |
| `basin_climatology` | US-08 | storms per season, median lifetime maximum, share reaching 64 kt and share active in the current month (1980 to the last final season); percentile of the current storm's maximum among the basin's lifetime maxima |
| `narrative` | all | one paragraph per storm assembled only from the rows above, ending with "Analogs describe resemblance with past storms; they are not a forecast." |

US-09 (NHC cone) is optional and the NHC feed is not ingested yet; US-10 is out of scope.

### Storm Analog Similarity (SAS) as implemented
Definition from Workshop 1 (Sec. "Indicator Proposal"); implementation in
[`src/tcmon/gold/analogs.py`](../src/tcmon/gold/analogs.py).

```
D   = w1·d̄_track/d0 + w2·mean|ΔV|/V0 + w3·mean|Δθ|/θ0 + w4·|Δdoy|/t0      SAS = 100·e^(−D)
d0 = 250 km, V0 = 15 kt, θ0 = 45°, t0 = 30 days, w1 = w2 = w3 = w4 = 1/4
```

* Current window: fixes at 00/06/12/18 UTC in the last 48 h (≤ 9 fixes, offsets k = 0…8).
* Candidates: same basin, `TRACK_TYPE = main` (so provisional tracks of running seasons are out), not spur, not the
  current SID, ended ≥ 15 days before the current fix, with a 6-hourly fix within 300 km of the current position.
* Sliding window: every 6-hourly fix of a candidate is tried as the matching point; the fixes 6k h earlier are
  paired with the current fixes; windows not covering every current offset are dropped; the lowest D per candidate
  is kept; candidates are ranked by D.
* Heading = bearing of the 6-h move ending at the fix; Δdoy is circular (365 days).
* A wind or heading term that cannot be computed (old storms of some basins have no wind in IBTrACS)
  counts as one reference unit (15 kt or 45°). In the first Gold run the terms were dropped and the
  weights rescaled instead; North Indian storms of the 1980s without wind then ranked as AKVAN's best
  analogs with "high" SAS, so the neutral penalty was adopted.
* Bands: ≥ 70 high, 40–69 moderate, < 40 low (provisional, Workshop 1).
* The Spark result is checked against an independent pandas implementation in `tests/test_gold_spark.py`.
