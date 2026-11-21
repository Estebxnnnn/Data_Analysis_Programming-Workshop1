# Workshop 3 — Data-Quality Findings and Pipeline Improvements

Audit of the data lake on the deployment machine between **17 and 21 November 2026**: 45 Bronze files and
27 Silver runs. Runs made with the Workshop 2 code are labelled *Workshop 2*; runs that write the new
ingestion log (`datalake_bronze/_state/ingestion_log_*.jsonl`) are labelled *Workshop 3*. Every number is
reproduced by [`notebooks/03_data_quality_findings.ipynb`](../notebooks/03_data_quality_findings.ipynb);
the KPI catalogue is in [`gold_specification.md`](gold_specification.md).

| # | Finding (Workshop 2) | Evidence | Improvement (Workshop 3) | KPI |
| --- | --- | --- | --- | --- |
| F1 | GDACS returns **HTTP 400** "Execution Timeout Expired" | 5 failed hourly runs between 17 Nov 23:00 and 18 Nov 03:00 UTC, 31.8–38.8 s each; error bodies left in `_staging/` (one copy in `evidence/http/`) | short 400 body with "timeout" retried (4/8/16 s); bodies moved to `_errors/`; every attempt logged | C3 |
| F2 | IBTrACS ACTIVE **stored every hour although unchanged** | 13 files, 2 distinct contents (ETags of 15 and 17 Nov); 11 redundant files = 6.36 of 7.53 MB; ≈ 13.7 MB/day, ≈ 5 GB/year | conditional GET (`If-None-Match`, `If-Modified-Since`) + SHA-256; no file when unchanged | E1, E2 |
| F3 | **File timestamps earlier than ingestion** | scheduled runs named with the start of the data interval: median delay 60–64 min (run `…_220000` ingested at 23:53) | `data_interval_end` for scheduled runs, trigger time for manual runs | T1 |
| F4 | **Empty Silver files** | 7 of 12 Silver runs wrote 0-row files (18.9 KB each) | no file when nothing is new; run kept in `_manifest.jsonl` | F4 |
| F5 | **Silver file named before its input** | 3 of 12 runs (Silver `…_020000` processed Bronze `…_021437`); later also caused by the FileSensor wait | Silver file takes the later of its run time and its newest Bronze run | F5 |
| F6 | **Batch IQR flagged a real typhoon peak** | batch fences [5.3, 71.3] kt: 16 flags = every CEMPAKA fix of 74–93 kt; same storm 0 flags in later batches | 1980–2025 climatology: 3·IQR of the 6-h wind change = [−15, +20] kt; speed \|z\| > 3 above 30.0 kt | V4, V5, V9 |
| F7 | **Structural vs real nulls** | GDACS has no current wind, pressure, motion (100 % null by design); real gaps: pressure 3.0 %, names 9.5 %; all fixes provisional | null rate per source and field | C1, V7 |
| F8 | **Sources disagree and are not equally fresh** | XAVIER: GDACS 130 kt vs IBTrACS 99 kt, GDACS 78 h ahead (20 Nov); after the IBTrACS release of 20 Nov: 130 vs 128 kt, 36 h | both kept with the lead time | K1–K3, T3 |
| F9 | **Name formats** | GDACS names 25.5 characters on average (`Tropical Cyclone XAVIER-26`), 5.5 after normalization | normalization measured | K4, K5 |

GDACS also edits records in place: episode 13 of AKVAN changed its `datemodified` nine times
(16 revisions among 231 Silver rows), so the Gold layer reads only the latest version of each `event_id`.

## Before / after (same data lake)

| Measure | Workshop 2 | Workshop 3 |
| --- | --- | --- |
| GDACS extraction attempts failed | 5 of 18 | 0 of 17 |
| IBTrACS checks that stored a file | 13 of 13 | 2 of 17 (bootstrap + new NCEI release of 20 Nov 03:12 GMT) |
| Redundant IBTrACS volume | 6.36 MB | 0.57 MB (bootstrap only) |
| Median timestamp-to-ingestion delay | 60–64 min | 1.2 min (two runs of 17–18 h were created before the laptop was shut down and executed at restart) |
| Empty Silver files written | 7 of 12 | 0 of 15 |
| Silver named before its input | 3 of 12 | 0 of 10 after the fix of 20 Nov 01:30 UTC |
| Wind flags, Bronze batch of 17 Nov 01:00 | 16 | 0 |
| Monitoring | none | 149 governance KPI values per Gold run |
| Historical baseline | not in the lake | 312,416 fixes, 5,019 storms (`datalake_silver/_reference/`) |
| Tests | Workshop 2 suite | 45 passed + 1 skipped in the container (incl. 9 Workshop 3 and 4 PySpark tests) |

## Problems found when Gold met real data

| Problem | Fix |
| --- | --- |
| `build_storytelling` failed 3 times with `java.lang.OutOfMemoryError` (query plan text too large: one `select` per metric joined by `union`) | metrics packed in an array of structs and exploded once; `localCheckpoint` per section; `spark.sql.maxPlanStringLength` capped. Task now ≈ 2 min |
| AKVAN's best analog had SAS 77 ("high") without wind data | a missing wind or heading term counts as one reference unit (15 kt or 45°) |
| Narrative printed "None of 5 analogs intensified" when no analog lasted 48 h | counts the analogs still tracked at 48 h |
| U2 (within-batch duplicates) had a 0 % target | descriptive: expected when one Silver batch claims several Bronze runs |

## Rules that did not change
Bronze stays raw (records exactly as published); Silver keeps its 38-field schema (only the meaning of the two
statistical flags changed, and they are excluded from the record hash, so the new rule does not create false
revisions); deduplication by (`event_id`, `record_hash`); atomic writes; lineage per file and per row.
