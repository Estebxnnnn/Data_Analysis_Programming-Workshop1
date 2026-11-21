"""Gold layer end to end with PySpark (local mode) on a small lake built from the recorded fixtures.

Runs inside the Airflow image, which has Java and PySpark:
    docker compose exec airflow-scheduler python -m pytest -q /opt/airflow/tests
Skipped where pyspark or pyarrow is not installed.
"""
import json
import shutil

import pandas as pd
import pytest

pytest.importorskip("pyarrow")
pytest.importorskip("pyspark")

from tcmon.gold import analogs, governance, storytelling  # noqa: E402
from tcmon.gold.spark import get_spark, write_single_parquet  # noqa: E402
from tcmon.pipeline import bronze, paths, reference, silver  # noqa: E402

from test_workshop3 import FIX, _synthetic_reference  # noqa: E402

RUN1, RUN2, GOLD = "20261114_190000", "20261114_200000", "20261114_204000"


@pytest.fixture(scope="module")
def lake(tmp_path_factory):
    root = tmp_path_factory.mktemp("lake")
    mp = pytest.MonkeyPatch()
    mp.setenv("TCMON_DATALAKE_ROOT", str(root))
    paths.ensure_layout()
    for run in (RUN1, RUN2):
        names = []
        for source, fixture, ext in [(bronze.IBTRACS_SOURCE, "ibtracs_active_20261114.csv", "csv"),
                                     (bronze.GDACS_SOURCE, "gdacs_tc_current_20261114.json", "json")]:
            staged = paths.staging_dir() / f"{source}_{run}.{ext}"
            shutil.copyfile(FIX / fixture, staged)
            ex = {"source": source, "staged": [str(staged)], "not_modified": False,
                  "http": [{"status": 200, "sha256": "fixture", "size_bytes": staged.stat().st_size, "elapsed_s": 1.0,
                            "headers": {"ETag": '"fixture"'}}]}
            names.append(bronze.persist(ex, bronze.validate(ex), run))
        bronze.signal_silver(run, names)
        silver.process(silver.bronze_files_of(silver.claim_batch(run)), run)
    cur = pd.read_parquet(paths.silver_file(RUN1))
    ref = _synthetic_reference(cur)
    import pyarrow as pa
    import pyarrow.parquet as pq
    pq.write_table(pa.Table.from_pandas(ref[reference.REF_COLUMNS], preserve_index=False), reference.parquet_path(),
                   coerce_timestamps="us", allow_truncated_timestamps=True)
    spark = get_spark("tcmon-test")
    yield {"root": root, "spark": spark, "silver": cur, "reference": ref}
    spark.stop()
    mp.undo()


def test_spark_reads_silver_folder_and_skips_underscore_paths(lake):
    from tcmon.gold.spark import read_silver
    allv = read_silver(lake["spark"], latest_only=False)
    assert allv.count() == len(lake["silver"])          # RUN2 had no changes: no file, nothing new
    assert set(allv.columns) >= {"event_id", "record_hash", "silver_file"}


def test_governance_file(lake):
    out = write_single_parquet(governance.build(lake["spark"], GOLD), paths.gold_file("governance", GOLD))
    assert out.name == f"governance_{GOLD}.parquet" and out.exists()
    assert not list(paths.gold_dir().glob("_tmp_*"))
    g = pd.read_parquet(out)
    assert list(g.columns) == governance.KPI_COLUMNS + ["gold_run_ts", "computed_at_utc"]
    assert g.loc[g.kpi_id == "F1", "value"].iloc[0] == 100.0          # every Silver file matches the schema
    assert g.loc[g.kpi_id == "U1", "value"].iloc[0] == pytest.approx(50.0)  # 2nd batch = exact copy of the 1st
    req = g[(g.kpi_id == "C1") & (g.detail == "required field")]
    assert (req["status"] == "ok").all()
    assert {"completeness", "uniqueness", "validity", "conformity", "timeliness", "consistency",
            "efficiency", "volume"} <= set(g.dimension)


def test_storytelling_file_and_analogs_match_pandas_twin(lake):
    sdf = storytelling.build(lake["spark"], GOLD, reference_available=True)
    out = write_single_parquet(sdf, paths.gold_file("storytelling", GOLD))
    s = pd.read_parquet(out)
    assert list(s.columns) == storytelling.STORY_COLUMNS + ["gold_run_ts", "computed_at_utc"]
    assert {"current_status", "intensity_track", "analogs", "analog_outcomes", "analog_tracks", "gdacs_alerts",
            "source_comparison", "basin_climatology", "narrative", "whats_new"} <= set(s.section)
    sas = s[(s.section == "analogs") & (s.metric == "sas")]
    cempaka = sas[sas.storm_id == "2026310N18149"].sort_values("rank")
    assert list(cempaka["item_id"])[:2] == ["2016310N19149", "2010310N20149"]
    twin = analogs.find_analogs_pandas(lake["silver"], lake["reference"]).set_index(["storm_id", "sid"])
    for _, r in sas.iterrows():
        assert r["value"] == pytest.approx(twin.loc[(r["storm_id"], r["item_id"]), "sas"], rel=1e-9)
    narrative = s.loc[(s.section == "narrative") & (s.storm_id == "2026310N18149"), "value_text"].iloc[0]
    assert "CEMPAKA" in narrative and "not a forecast" in narrative


def test_reference_stats_file_is_valid_json(lake):
    stats = reference.compute_stats(lake["reference"])
    json.dumps(stats)
    assert stats["speed_mean"] > 0
