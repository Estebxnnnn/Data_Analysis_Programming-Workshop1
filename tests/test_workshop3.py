"""Workshop 3 improvements of the Bronze/Silver layers and the pandas twin of the analog search.

These tests need no network and no Spark. Inputs are the recorded responses of
tests/fixtures/ and the IBTrACS since1980 head sample (tests/fixtures/); the
synthetic analog below is built from a real fixture track shifted by ten years.
"""
import shutil
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from tcmon.gold import analogs
from tcmon.http_client import PoliteClient
from tcmon.pipeline import bronze, paths, reference, silver

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / "tests" / "fixtures"
HEAD = FIX / "ibtracs_since1980_head.csv"   # copy of workshop_1/samples/ (not mounted in the containers)
RUN = "20261117_230000"


@pytest.fixture()
def lake(tmp_path, monkeypatch):
    monkeypatch.setenv("TCMON_DATALAKE_ROOT", str(tmp_path))
    paths.ensure_layout()
    return tmp_path


class _Resp:
    def __init__(self, status, body, length=None):
        self.status_code, self.content = status, body
        self.headers = {"Content-Length": str(length if length is not None else len(body))}


def test_gdacs_timeout_400_is_transient():
    body = b'{"message":"Execution Timeout Expired.  The timeout period elapsed prior to completion of the operation or the server is not responding."}'
    assert PoliteClient.is_transient_400(_Resp(400, body))           # real body of 18 Nov 2026 00:00 UTC
    assert not PoliteClient.is_transient_400(_Resp(400, b'{"message":"invalid date"}'))
    assert not PoliteClient.is_transient_400(_Resp(400, body, length=10_000_000))


def test_run_moment_uses_interval_end_for_scheduled_runs():
    start, end = pd.Timestamp("2026-11-17 22:00", tz="UTC"), pd.Timestamp("2026-11-17 23:00", tz="UTC")
    sched = {"dag_run": SimpleNamespace(run_type=SimpleNamespace(value="scheduled")),
             "logical_date": start, "data_interval_end": end}
    manual = {"dag_run": SimpleNamespace(run_type=SimpleNamespace(value="manual")),
              "logical_date": pd.Timestamp("2026-11-17 23:54:29", tz="UTC"), "data_interval_end": end}
    assert paths.ts_string(paths.run_moment(sched)) == "20261117_230000"
    assert paths.ts_string(paths.run_moment(manual)) == "20261117_235429"


def test_ibtracs_not_modified_writes_no_file(lake):
    http = [{"status": 304, "size_bytes": 0, "elapsed_s": 0.2, "headers": {}, "sha256": None}]
    extracted = {"source": bronze.IBTRACS_SOURCE, "staged": [], "http": http, "not_modified": True}
    report = bronze.validate(extracted)
    assert report["passed"] and report["skipped"]
    assert bronze.persist(extracted, report, RUN) == ""
    assert not any(paths.bronze_dir().glob("ibtracs_active_*.json"))
    assert bronze.expected_files(RUN) == {f"gdacs_tc_{RUN}.json"}
    log = paths.read_jsonl(bronze.ingestion_log(bronze.IBTRACS_SOURCE))
    assert log[-1]["outcome"] == "not_modified" and log[-1]["run_ts"] == RUN


def test_ibtracs_stored_file_saves_validators(lake):
    staged = paths.staging_dir() / f"ibtracs_active_{RUN}.csv"
    shutil.copyfile(FIX / "ibtracs_active_20261114.csv", staged)
    http = [{"status": 200, "sha256": "abc", "size_bytes": 67175, "elapsed_s": 0.7,
             "headers": {"ETag": '"10c15-6607619c2acbb"', "Last-Modified": "Tue, 17 Nov 2026 10:10:14 GMT"}}]
    ex = {"source": bronze.IBTRACS_SOURCE, "staged": [str(staged)], "http": http, "not_modified": False}
    name = bronze.persist(ex, bronze.validate(ex), RUN)
    state = paths.read_json(bronze.state_file(bronze.IBTRACS_SOURCE))
    assert name == f"ibtracs_active_{RUN}.json" and state["etag"] == '"10c15-6607619c2acbb"'
    assert bronze.expected_files(RUN) == {f"gdacs_tc_{RUN}.json", f"ibtracs_active_{RUN}.json"}


def test_error_bodies_leave_staging(lake):
    err = paths.staging_dir() / f"gdacs_tc_{RUN}_p01.json.http400"
    err.write_text('{"message":"Execution Timeout Expired."}')
    Path(str(err) + ".meta.json").write_text("{}")
    moved = bronze._quarantine([str(err)])
    assert sorted(moved) == [err.name, err.name + ".meta.json"]
    assert not any(paths.staging_dir().iterdir()) and (paths.errors_dir() / err.name).exists()


def _fixture_silver():
    env = {"source": "ibtracs_active", "http": [{"sha256": "x"}]}
    rows, units, header = bronze._ibtracs_rows(str(FIX / "ibtracs_active_20261114.csv"))
    env.update(columns=header, records=rows)
    return silver.conform_ibtracs(env, "ibtracs_active_x.json")


def test_outlier_fences_from_reference(lake):
    df = reference.read_reference_csv(HEAD)
    stats = reference.compute_stats(df)
    assert stats["dwind6_n"] > 100 and stats["speed_std"] > 0
    raw = _fixture_silver()
    before = silver.clean(raw)[0]
    assert before.attrs["outlier_fences"]["basis"] == "batch"
    paths.atomic_write_json(reference.stats_path(), stats)
    after = silver.clean(raw)[0]
    assert after.attrs["outlier_fences"]["basis"] == "reference"
    # the rule now looks at the intensity CHANGE: a typhoon peak is no longer an outlier by itself
    cempaka_peak = after[(after.storm_name == "CEMPAKA") & (after.wind_kt >= 85)]
    assert len(cempaka_peak) and not cempaka_peak["wind_iqr_outlier"].any()


def test_reference_conform_on_real_head_sample():
    df = reference.read_reference_csv(HEAD)
    assert list(df.columns) == reference.REF_COLUMNS
    assert df["iso_time"].dt.tz is not None and df["lon"].between(-180, 180).all()
    assert not df.duplicated(["sid", "iso_time"]).any()
    assert set(df["basin"]) <= {"NA", "EP", "WP", "NI", "SI", "SP", "SA", "UN"}


def _synthetic_reference(current: pd.DataFrame) -> pd.DataFrame:
    """Real CEMPAKA track (fixture) moved ten years back and 1 degree north = a perfect analog,
    plus the same track 2 degrees away with +20 kt (weaker analog); a copy 8 degrees away would be
    outside the 300 km candidate radius and the real 1980 head sample."""
    c = current[current.storm_name == "CEMPAKA"]
    parts = [reference.read_reference_csv(HEAD)]
    for sid, dlat, dw in (("2016310N19149", 1.0, 5.0), ("2010310N20149", 2.0, 20.0)):
        parts.append(pd.DataFrame({
            "sid": sid, "season": int(sid[:4]), "basin": "WP", "name": f"ANALOG{sid[:4]}",
            "iso_time": c["timestamp_utc"] - pd.DateOffset(years=2026 - int(sid[:4])),
            "lat": c["latitude"] + dlat, "lon": c["longitude"], "wind_kt": c["wind_kt"] + dw,
            "wind_avg_period": "1-min", "pressure_hpa": np.nan, "sshs_category": pd.NA, "nature": "TS",
            "track_type": "main", "is_spur": False, "dist2land_km": 500.0, "storm_speed_kt": np.nan,
            "storm_dir_deg": np.nan}))
    ref = pd.concat(parts, ignore_index=True)
    ref["iso_time"] = pd.to_datetime(ref["iso_time"], utc=True)
    return ref


def test_analog_twin_ranks_the_shifted_copy_first():
    cur = silver.clean(_fixture_silver())[0]
    res = analogs.find_analogs_pandas(cur, _synthetic_reference(cur))
    c = res[res.storm_id == "2026310N18149"].sort_values("rank")
    assert list(c["sid"]) == ["2016310N19149", "2010310N20149"]
    best = c.iloc[0]
    assert best["track_km"] == pytest.approx(111.2, abs=0.5)       # 1 degree of latitude
    assert best["dv_kt"] == pytest.approx(5.0) and best["dtheta_deg"] < 1.0
    assert 0 <= best["ddoy_days"] <= 3                               # leap days only
    assert best["sas"] > c.iloc[1]["sas"] and 0 < best["sas"] <= 100
    assert analogs.band(best["sas"]) in {"high", "moderate"}
