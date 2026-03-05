"""Tests for across_qa.visualization module."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pandas as pd # type: ignore
import plotly.graph_objects as go # type: ignore

from across_qa.visualization import plot_ingesetion_status_timeline, _STATUS_COLORS, _STATUS_ORDER


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


NOW = _utc(datetime(2024, 6, 1, 12, 0, 0))
LAST_30MIN_AGO = NOW - timedelta(minutes=30)
LAST_2H_AGO = NOW - timedelta(hours=2)
NEXT_30MIN = NOW + timedelta(minutes=30)
LAST_1H_AGO = NOW - timedelta(hours=1)


def _make_df(*rows: dict) -> pd.DataFrame:
    """Build a minimal DataFrame mirroring check_all_telescopes() output."""
    columns = [
        "telescope_name",
        "telescope_short_name",
        "telescope_id",
        "schedule_status",
        "cron",
        "last_ingested",
        "ingested_attempts",
        "next_ingestion_attempt",
        "status",
        "message",
    ]
    # Convert old column names to new ones if present
    converted_rows = []
    for row in rows:
        converted = {k: v for k, v in row.items()}
        if "next_expected" in converted:
            converted["next_ingestion_attempt"] = converted.pop("next_expected")
        if "ingested_attempts" not in converted:
            converted["ingested_attempts"] = []
        converted_rows.append(converted)
    return pd.DataFrame(converted_rows, columns=columns)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestPlotTimeline:
    """Unit tests for :func:`plot_ingesetion_status_timeline`."""

    def test_returns_figure(self):
        """plot_ingesetion_status_timeline should always return a Plotly Figure."""

        df = _make_df(
            {
                "telescope_name": "Neil Gehrels Swift Observatory",
                "telescope_short_name": "Swift",
                "telescope_id": "t1",
                "schedule_status": "planned",
                "cron": "0 * * * *",
                "last_ingested": LAST_30MIN_AGO,
                "next_expected": NEXT_30MIN,
                "status": "OK",
                "message": "All good.",
            }
        )
        fig = plot_ingesetion_status_timeline(df)
        assert isinstance(fig, go.Figure)

    def test_empty_dataframe(self):
        """An empty DataFrame should not raise and should return a Figure."""
        import plotly.graph_objects as go

        df = pd.DataFrame(
            columns=[
                "telescope_name", "telescope_short_name", "telescope_id",
                "schedule_status", "cron", "last_ingested", "next_expected",
                "status", "message",
            ]
        )
        fig = plot_ingesetion_status_timeline(df)
        assert isinstance(fig, go.Figure)

    def test_one_trace_per_status(self):
        """Each status value present in the data produces at least one named trace."""
        df = _make_df(
            {
                "telescope_name": "Neil Gehrels Swift Observatory",
                "telescope_short_name": "Swift",
                "telescope_id": "t1",
                "schedule_status": "planned",
                "cron": "0 * * * *",
                "last_ingested": LAST_30MIN_AGO,
                "next_expected": NEXT_30MIN,
                "status": "OK",
                "message": "OK",
            },
            {
                "telescope_name": "Chandra X-ray Observatory",
                "telescope_short_name": "Chandra",
                "telescope_id": "t2",
                "schedule_status": "planned",
                "cron": "0 * * * *",
                "last_ingested": LAST_2H_AGO,
                "next_expected": LAST_1H_AGO,
                "status": "LATE",
                "message": "Late.",
            },
            {
                "telescope_name": "Fermi Gamma-ray Space Telescope",
                "telescope_short_name": "Fermi",
                "telescope_id": "t3",
                "schedule_status": "planned",
                "cron": "0 * * * *",
                "last_ingested": None,
                "next_expected": None,
                "status": "MISSING",
                "message": "Missing.",
            },
            {
                "telescope_name": "Hubble Space Telescope",
                "telescope_short_name": "Hubble",
                "telescope_id": "t4",
                "schedule_status": "",
                "cron": None,
                "last_ingested": None,
                "next_expected": None,
                "status": "NO_CADENCE",
                "message": "No cadence.",
            },
        )
        fig = plot_ingesetion_status_timeline(df)
        trace_names = {t.name for t in fig.data}
        # All four statuses are present in the data; each should have a trace.
        for status in ["OK", "LATE", "MISSING", "NO_CADENCE"]:
            assert status in trace_names, f"Expected trace for status {status!r}"

    def test_status_colors_match_spec(self):
        """Marker colours in traces must match the spec colours."""
        df = _make_df(
            {
                "telescope_name": "Neil Gehrels Swift Observatory",
                "telescope_short_name": "Swift",
                "telescope_id": "t1",
                "schedule_status": "planned",
                "cron": "0 * * * *",
                "last_ingested": LAST_30MIN_AGO,
                "next_expected": NEXT_30MIN,
                "status": "OK",
                "message": "OK",
            }
        )
        fig = plot_ingesetion_status_timeline(df)
        ok_trace = next(t for t in fig.data if t.name == "OK")
        assert ok_trace.marker.color == _STATUS_COLORS["OK"]

    def test_saves_html_file(self, tmp_path):
        """When output_path is provided, an HTML file is created."""
        df = _make_df(
            {
                "telescope_name": "Neil Gehrels Swift Observatory",
                "telescope_short_name": "Swift",
                "telescope_id": "t1",
                "schedule_status": "planned",
                "cron": "0 * * * *",
                "last_ingested": LAST_30MIN_AGO,
                "next_expected": NEXT_30MIN,
                "status": "OK",
                "message": "OK",
            }
        )
        out = str(tmp_path / "report.html")
        plot_ingesetion_status_timeline(df, output_path=out)
        assert os.path.exists(out)
        with open(out) as fh:
            content = fh.read()
        assert "<html" in content.lower() or "plotly" in content.lower()

    def test_missing_has_no_last_ingested(self):
        """MISSING rows (last_ingested=None) should not raise."""
        df = _make_df(
            {
                "telescope_name": "Fermi Gamma-ray Space Telescope",
                "telescope_short_name": "Fermi",
                "telescope_id": "t3",
                "schedule_status": "planned",
                "cron": "0 * * * *",
                "last_ingested": None,
                "next_expected": None,
                "status": "MISSING",
                "message": "No schedule ingested.",
            }
        )
        import plotly.graph_objects as go

        fig = plot_ingesetion_status_timeline(df)
        assert isinstance(fig, go.Figure)

    def test_figure_has_xaxis_type_date(self):
        """The X axis should be of type 'date' (time on the X axis)."""
        df = _make_df(
            {
                "telescope_name": "Neil Gehrels Swift Observatory",
                "telescope_short_name": "Swift",
                "telescope_id": "t1",
                "schedule_status": "planned",
                "cron": "0 * * * *",
                "last_ingested": LAST_30MIN_AGO,
                "next_expected": NEXT_30MIN,
                "status": "OK",
                "message": "OK",
            }
        )
        fig = plot_ingesetion_status_timeline(df)
        assert fig.layout.xaxis.type == "date"

    def test_short_name_used_as_y_axis_label(self):
        """Y-axis labels should use telescope_short_name, not the full name."""
        df = _make_df(
            {
                "telescope_name": "Neil Gehrels Swift Observatory",
                "telescope_short_name": "Swift",
                "telescope_id": "t1",
                "schedule_status": "planned",
                "cron": "0 * * * *",
                "last_ingested": LAST_30MIN_AGO,
                "next_expected": NEXT_30MIN,
                "status": "OK",
                "message": "OK",
            }
        )
        fig = plot_ingesetion_status_timeline(df)
        ok_trace = next(t for t in fig.data if t.name == "OK")
        y_labels = list(ok_trace.y)
        assert any("Swift" in str(lbl) for lbl in y_labels)
        assert not any("Neil Gehrels" in str(lbl) for lbl in y_labels)

    def test_all_statuses_covered_in_color_map(self):
        """_STATUS_COLORS must define a colour for every member of _STATUS_ORDER."""
        for status in _STATUS_ORDER:
            assert status in _STATUS_COLORS, f"No colour defined for {status!r}"
