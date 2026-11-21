"""Parser tests. Inputs here are minimal hand-built strings that exercise the
parsing logic; they are NOT recorded source responses (those live in fixtures/)."""
import json
from pathlib import Path

import pandas as pd
import pytest

from tcmon.normalization import canonical
from tcmon.sources import gdacs, ibtracs, nhc

FIXTURES = Path(__file__).parent / "fixtures"


def _write_ibtracs_like(tmp_path, with_units_row: bool) -> Path:
    header = ",".join(ibtracs.USED_COLUMNS)
    units = ",".join(["", "Year"] + [""] * (len(ibtracs.USED_COLUMNS) - 2))
    values = {c: "" for c in ibtracs.USED_COLUMNS}
    values.update(SID="TESTSID0001", SEASON="2000", BASIN="WP", NAME="TEST", ISO_TIME="2000-01-01 00:00:00",
                  LAT="10.0", LON="190.0", WMO_WIND=" ", USA_WIND="65", TRACK_TYPE="main")
    row = ",".join(values[c] for c in ibtracs.USED_COLUMNS)
    text = "\n".join([header, units, row] if with_units_row else [header, row]) + "\n"
    p = tmp_path / "ib.csv"
    p.write_text(text)
    return p


def test_ibtracs_units_row_and_blanks(tmp_path):
    df = ibtracs.read_csv(_write_ibtracs_like(tmp_path, with_units_row=True))
    assert len(df) == 1
    assert df.attrs["units_row"] is not None
    assert pd.isna(df.loc[0, "WMO_WIND"])          # whitespace -> NaN
    assert df.loc[0, "USA_WIND"] == 65
    assert str(df.loc[0, "ISO_TIME"].tz) == "UTC"


def test_ibtracs_without_units_row(tmp_path):
    df = ibtracs.read_csv(_write_ibtracs_like(tmp_path, with_units_row=False))
    assert len(df) == 1 and df.attrs["units_row"] is None


def test_ibtracs_canonical(tmp_path):
    df = ibtracs.read_csv(_write_ibtracs_like(tmp_path, with_units_row=True))
    ev = canonical.ibtracs_to_events(df, "ibtracs_active", "file.csv")
    row = ev.iloc[0]
    assert row["longitude"] == -170.0              # normalized
    assert row["severity"] == 65 and row["sshs_category"] == 1
    assert row["wind_avg_period"] == "1-min"
    assert set(canonical.CANONICAL) <= set(ev.columns)


def test_nhc_empty_feed_is_valid():
    doc = nhc.load(FIXTURES / "nhc_current_storms_empty.json")
    frame = nhc.storms_to_frame(doc)
    assert frame.empty
    assert canonical.nhc_to_events(frame, "x").empty


def test_gdacs_flatten_and_convert(tmp_path):
    feature = {"type": "Feature", "geometry": {"type": "Point", "coordinates": [120.0, 15.0]},
               "properties": {"eventid": 1, "episodeid": 2, "eventtype": "TC", "name": "TEST",
                              "todate": "2000-01-01T00:00:00", "alertlevel": "Green",
                              "severitydata": {"severity": 185.2, "severityunit": "km/h"}}}
    p = tmp_path / "g.json"
    p.write_text(json.dumps({"type": "FeatureCollection", "features": [feature]}))
    frame = gdacs.features_to_frame(gdacs.load_features(p))
    ev = canonical.gdacs_to_events(frame, "g.json")
    assert ev.loc[0, "severity"] == pytest.approx(100.0)
    assert ev.loc[0, "latitude"] == 15.0


def test_ingest_cli_dry_run(capsys):
    # ingest.py (Workshop 1 CLI) lives in the repository root, which is not mounted in
    # the Airflow containers; there the test is skipped instead of failing.
    ingest = pytest.importorskip("ingest")
    assert ingest.main(["--mode", "incremental", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "ibtracs_active" in out and "gdacs_tc_current" in out
    assert "nhc_current_storms" not in out           # optional source disabled
