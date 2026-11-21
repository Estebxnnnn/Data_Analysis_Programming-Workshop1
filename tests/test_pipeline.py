"""Bronze/Silver pipeline tests on the REAL responses recorded in Workshop 1.

The Parquet round-trip test needs pyarrow (installed in the Airflow image):
    docker compose exec airflow-scheduler python -m pytest -q /opt/airflow/tests
"""
import json
import shutil
from pathlib import Path

import pandas as pd
import pytest

from tcmon.pipeline import bronze, paths, silver
from tcmon.pipeline.schema import COLUMNS, REQUIRED

FIX = Path(__file__).parent / "fixtures"
RUN1, RUN2 = "20261114_183000", "20261114_193000"


@pytest.fixture()
def lake(tmp_path, monkeypatch):
    monkeypatch.setenv("TCMON_DATALAKE_ROOT", str(tmp_path))
    paths.ensure_layout()
    return tmp_path


def _bronze_run(run_ts):
    """Simulate one Bronze DAG run with the recorded responses (no network)."""
    names = []
    for source, fixture, ext in [(bronze.IBTRACS_SOURCE, "ibtracs_active_20261114.csv", "csv"),
                                 (bronze.GDACS_SOURCE, "gdacs_tc_current_20261114.json", "json")]:
        staged = paths.staging_dir() / f"{source}_{run_ts}.{ext}"
        shutil.copyfile(FIX / fixture, staged)
        extracted = {"source": source, "staged": [str(staged)],
                     "http": [{"status": 200, "sha256": "test", "url": "fixture"}]}
        report = bronze.validate(extracted)
        names.append(bronze.persist(extracted, report, run_ts))
    bronze.signal_silver(run_ts, names)
    return names


def test_bronze_files_and_nomenclature(lake):
    names = _bronze_run(RUN1)
    assert names == [f"ibtracs_active_{RUN1}.json", f"gdacs_tc_{RUN1}.json"]
    env = json.loads((paths.bronze_dir() / names[0]).read_text())
    assert env["record_count"] == 139 and env["validation"]["units_row"] is True
    assert env["records"][0]["WMO_WIND"] == " "                      # raw blanks preserved
    assert (paths.pending_dir() / f"bronze_run_{RUN1}.json").exists()
    assert not any(paths.staging_dir().iterdir())                    # staging cleaned


def test_validation_rejects_missing_fields(lake):
    bad = paths.staging_dir() / "bad.csv"
    bad.write_text("SID,NAME\nX,Y\n")
    with pytest.raises(ValueError):
        bronze.validate({"source": bronze.IBTRACS_SOURCE, "staged": [str(bad)], "http": []})


def test_silver_transform_rules(lake):
    names = _bronze_run(RUN1)
    frames = [silver.conform_ibtracs(json.loads((paths.bronze_dir() / names[0]).read_text()), names[0]),
              silver.conform_gdacs(json.loads((paths.bronze_dir() / names[1]).read_text()), names[1])]
    raw = pd.concat(frames, ignore_index=True)
    cleaned, rejected = silver.clean(raw)
    assert len(rejected) == 0 and len(cleaned) == 139 + 3
    assert cleaned["longitude"].between(-180, 180).all()
    assert str(cleaned["timestamp_utc"].dt.tz) == "UTC"
    g = cleaned[cleaned.source == "gdacs_tc"].set_index("storm_id")
    assert g.loc["1001347", "storm_name"] == "CEMPAKA"                # prefix and -26 removed
    assert g.loc["1001347", "peak_wind_kt"] == pytest.approx(95.0, abs=0.1)
    assert pd.isna(g.loc["1001347", "wind_kt"])                      # GDACS is a peak, not current wind
    assert bool(g.loc["1001350", "is_unnamed"]) if "1001350" in g.index else True
    i = cleaned[cleaned.source == "ibtracs_active"]
    assert set(i["basin"]) <= {"EP", "WP", "SI", "NA", "SP", "NI", "SA"}
    assert i["wind_kt"].notna().all() and (i["wind_avg_period"] == "1-min").all()
    assert i.loc[i.storm_name_raw == "UNNAMED", "is_unnamed"].all()
    final = silver.enforce_schema(silver.deduplicate(cleaned, silver.previous_hashes())[0]
                                  .assign(silver_run_ts=RUN1, processed_at_utc=pd.Timestamp.now(tz="UTC")))
    assert list(final.columns) == COLUMNS
    assert not final[REQUIRED].isna().any().any()


def test_dedup_across_batches():
    df = pd.DataFrame({"event_id": ["a", "b", "b"], "bronze_file": ["x", "x", "y"]})
    for c in silver.BUSINESS_COLUMNS:
        if c not in df:
            df[c] = "v"
    first, st1 = silver.deduplicate(df, pd.DataFrame(columns=["event_id", "record_hash"]))
    assert st1["within_batch_duplicates"] == 1 and len(first) == 2
    again, st2 = silver.deduplicate(df, first[["event_id", "record_hash"]])
    assert st2["cross_batch_duplicates"] == 2 and len(again) == 0
    changed = df.copy(); changed.loc[0, "storm_name"] = "CHANGED"
    rev, st3 = silver.deduplicate(changed, first[["event_id", "record_hash"]])
    assert st3["revisions"] == 1 and rev["is_revision"].all()


def test_name_normalization():
    assert silver.normalize_name("Tropical Cyclone CEMPAKA-26") == ("CEMPAKA", False)
    assert silver.normalize_name("Tropical Cyclone FIVE-24") == (None, True)
    assert silver.normalize_name("02-20262027-27") == (None, True)
    assert silver.normalize_name("UNNAMED") == (None, True)
    assert silver.normalize_name(" Peñà ") == ("PENA", False)


def test_parquet_roundtrip_and_idempotency(lake):
    pytest.importorskip("pyarrow")
    _bronze_run(RUN1)
    batch = silver.bronze_files_of(silver.claim_batch(RUN1))
    out1 = silver.process(batch, RUN1)
    df = pd.read_parquet(paths.silver_file(RUN1))
    assert len(df) == out1["rows"] == 142 and list(df.columns) == COLUMNS
    assert df["bronze_file"].str.startswith(("ibtracs_active_", "gdacs_tc_")).all()
    # same Silver run again -> same file, same rows (idempotent)
    out_again = silver.process(silver.bronze_files_of(silver.claim_batch(RUN1)), RUN1)
    assert out_again["rows"] == 142
    # a second Bronze run with identical content -> no new rows
    _bronze_run(RUN2)
    out2 = silver.process(silver.bronze_files_of(silver.claim_batch(RUN2)), RUN2)
    assert out2["rows"] == 0 and out2["dedup"]["cross_batch_duplicates"] == 142
    # Workshop 3: a batch without changes writes no Parquet file, only a manifest line
    assert out2["silver_file"] is None and not paths.silver_file(RUN2).exists()
    lines = (paths.silver_dir() / "_manifest.jsonl").read_text().splitlines()
    assert [json.loads(l)["silver_run_ts"] for l in lines] == [RUN1, RUN2]
