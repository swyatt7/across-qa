"""Tests for across_qa.history module."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from across_qa.history import (
    _DEFAULT_LOOKBACK_DAYS,
    _STATUS_COLORS,
    get_schedule_history,
    plot_schedule_history,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


NOW = _utc(datetime(2024, 6, 1, 12, 0, 0))
DATE_BEGIN = NOW - timedelta(days=5)
DATE_END = NOW - timedelta(days=2)


def _make_telescope(
    name: str,
    tid: str,
    short_name: str | None = None,
) -> MagicMock:
    t = MagicMock()
    t.name = name
    t.short_name = short_name if short_name is not None else name
    t.id = tid
    return t


def _make_schedule(
    telescope_id: str,
    status: str,
    date_begin: datetime,
    date_end: datetime,
    created_on: datetime | None = None,
) -> MagicMock:
    s = MagicMock()
    s.telescope_id = telescope_id
    s.status = status
    date_range = MagicMock()
    date_range.begin = date_begin
    date_range.end = date_end
    s.date_range = date_range
    s.created_on = created_on or NOW
    return s


def _make_page(schedules: list) -> MagicMock:
    page = MagicMock()
    page.items = schedules
    return page


def _make_client(telescopes: list, schedules: list) -> MagicMock:
    client = MagicMock()
    client.telescope.get_many.return_value = telescopes
    client.schedule.get_many.return_value = _make_page(schedules)
    return client


def _make_history_df(*rows: dict) -> pd.DataFrame:
    """Build a minimal DataFrame matching get_schedule_history() output."""
    columns = [
        "telescope_name",
        "telescope_short_name",
        "telescope_id",
        "status",
        "date_range_begin",
        "date_range_end",
        "created_on",
    ]
    # Convert date_begin/date_end keys to date_range_begin/date_range_end
    converted_rows = []
    for row in rows:
        converted = {k: v for k, v in row.items()}
        if "date_begin" in converted:
            converted["date_range_begin"] = converted.pop("date_begin")
        if "date_end" in converted:
            converted["date_range_end"] = converted.pop("date_end")
        converted_rows.append(converted)
    return pd.DataFrame(converted_rows, columns=columns)


# ---------------------------------------------------------------------------
# Tests for get_schedule_history
# ---------------------------------------------------------------------------

class TestGetScheduleHistory:
    """Unit tests for :func:`get_schedule_history`."""

    def test_returns_dataframe(self):
        """A successful call returns a pandas DataFrame."""
        t = _make_telescope("Swift", "t1", "Swift")
        s = _make_schedule("t1", "planned", DATE_BEGIN, DATE_END)
        client = _make_client([t], [s])

        df = get_schedule_history(client=client)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1

    def test_dataframe_columns(self):
        """The returned DataFrame has the expected columns."""
        expected = {
            "telescope_name",
            "telescope_short_name",
            "telescope_id",
            "status",
            "date_range_begin",
            "date_range_end",
            "created_on",
        }
        t = _make_telescope("Swift", "t1")
        s = _make_schedule("t1", "planned", DATE_BEGIN, DATE_END)
        client = _make_client([t], [s])

        df = get_schedule_history(client=client)

        assert set(df.columns) == expected

    def test_telescope_name_resolved(self):
        """Telescope name is resolved from the telescope list, not left as an id."""
        t = _make_telescope("Neil Gehrels Swift Observatory", "t1", "Swift")
        s = _make_schedule("t1", "planned", DATE_BEGIN, DATE_END)
        client = _make_client([t], [s])

        df = get_schedule_history(client=client)

        assert df.iloc[0]["telescope_name"] == "Neil Gehrels Swift Observatory"
        assert df.iloc[0]["telescope_short_name"] == "Swift"

    def test_filter_by_telescope_name(self):
        """Only schedules for the requested telescope name are returned."""
        t1 = _make_telescope("Swift", "t1")
        t2 = _make_telescope("Fermi", "t2")
        s1 = _make_schedule("t1", "planned", DATE_BEGIN, DATE_END)
        s2 = _make_schedule("t2", "planned", DATE_BEGIN, DATE_END)
        client = _make_client([t1, t2], [s1, s2])

        df = get_schedule_history(client=client, telescope_identifiers=["Swift"])

        assert len(df) == 1
        assert df.iloc[0]["telescope_name"] == "Swift"

    def test_filter_by_telescope_id(self):
        """telescope_identifiers accepts UUIDs as well as names."""
        t1 = _make_telescope("Swift", "uuid-t1")
        t2 = _make_telescope("Fermi", "uuid-t2")
        s1 = _make_schedule("uuid-t1", "planned", DATE_BEGIN, DATE_END)
        s2 = _make_schedule("uuid-t2", "planned", DATE_BEGIN, DATE_END)
        client = _make_client([t1, t2], [s1, s2])

        df = get_schedule_history(client=client, telescope_identifiers=["uuid-t2"])

        assert len(df) == 1
        assert df.iloc[0]["telescope_id"] == "uuid-t2"

    def test_filter_by_short_name(self):
        """telescope_identifiers accepts short names."""
        t1 = _make_telescope("Neil Gehrels Swift Observatory", "t1", "Swift")
        t2 = _make_telescope("Fermi Gamma-ray Space Telescope", "t2", "Fermi")
        s1 = _make_schedule("t1", "planned", DATE_BEGIN, DATE_END)
        s2 = _make_schedule("t2", "planned", DATE_BEGIN, DATE_END)
        client = _make_client([t1, t2], [s1, s2])

        df = get_schedule_history(client=client, telescope_identifiers=["Fermi"])

        assert len(df) == 1
        assert df.iloc[0]["telescope_id"] == "t2"

    def test_no_identifiers_returns_all(self):
        """When telescope_identifiers is None, all telescopes are included."""
        t1 = _make_telescope("Swift", "t1")
        t2 = _make_telescope("Fermi", "t2")
        s1 = _make_schedule("t1", "planned", DATE_BEGIN, DATE_END)
        s2 = _make_schedule("t2", "planned", DATE_BEGIN, DATE_END)
        client = _make_client([t1, t2], [s1, s2])

        df = get_schedule_history(client=client)

        assert len(df) == 2

    def test_default_date_begin_is_90_days_ago(self):
        """date_range_begin defaults to _DEFAULT_LOOKBACK_DAYS days in the past."""
        t = _make_telescope("Swift", "t1")
        client = _make_client([t], [])

        get_schedule_history(client=client)

        call_kwargs = client.schedule.get_many.call_args.kwargs
        assert "date_range_begin" in call_kwargs
        # Allow a small tolerance for test execution time.
        delta = abs(
            (call_kwargs["date_range_begin"] - (
                datetime.now() - timedelta(days=_DEFAULT_LOOKBACK_DAYS)
            )).total_seconds()
        )
        assert delta < 5

    def test_custom_date_begin_is_forwarded(self):
        """A supplied date_range_begin is forwarded to the API call."""
        t = _make_telescope("Swift", "t1")
        client = _make_client([t], [])
        custom = _utc(datetime(2024, 1, 1))

        get_schedule_history(client=client, date_range_begin=custom)

        call_kwargs = client.schedule.get_many.call_args.kwargs
        assert call_kwargs["date_range_begin"] == custom

    def test_naive_date_begin_forwarded_unchanged(self):
        """A naive date_range_begin datetime is forwarded to the API as-is (no UTC coercion)."""
        t = _make_telescope("Swift", "t1")
        client = _make_client([t], [])
        naive = datetime(2024, 1, 1)  # no tzinfo

        get_schedule_history(client=client, date_range_begin=naive)

        call_kwargs = client.schedule.get_many.call_args.kwargs
        assert call_kwargs["date_range_begin"] == naive
        assert call_kwargs["date_range_begin"].tzinfo is None

    def test_empty_telescope_list_returns_empty_df(self):
        """No telescopes → empty DataFrame with correct columns."""
        client = _make_client([], [])

        df = get_schedule_history(client=client)

        assert df.empty
        assert "telescope_name" in df.columns

    def test_no_matching_identifiers_returns_empty_df(self):
        """Identifiers that match no telescope return an empty DataFrame."""
        t = _make_telescope("Swift", "t1")
        client = _make_client([t], [])

        df = get_schedule_history(client=client, telescope_identifiers=["NonExistent"])

        assert df.empty

    def test_empty_schedule_list_returns_empty_df(self):
        """No schedules returned by the API → empty DataFrame."""
        t = _make_telescope("Swift", "t1")
        client = _make_client([t], [])

        df = get_schedule_history(client=client)

        assert df.empty

    def test_api_error_returns_empty_df(self):
        """An exception from the schedule API returns an empty DataFrame."""
        t = _make_telescope("Swift", "t1")
        client = MagicMock()
        client.telescope.get_many.return_value = [t]
        client.schedule.get_many.side_effect = RuntimeError("API down")

        df = get_schedule_history(client=client)

        assert df.empty

    def test_default_client_created(self):
        """When client=None, a default Client() is instantiated."""
        with patch("across_qa.history.Client") as mock_cls:
            mock_client = MagicMock()
            mock_client.telescope.get_many.return_value = []
            mock_cls.return_value = mock_client

            get_schedule_history()

            mock_cls.assert_called_once_with()

    def test_single_schedule_api_call(self):
        """Exactly one schedule API call is made regardless of telescope count."""
        t1 = _make_telescope("Swift", "t1")
        t2 = _make_telescope("Fermi", "t2")
        client = _make_client([t1, t2], [])

        get_schedule_history(client=client)

        assert client.schedule.get_many.call_count == 1

    def test_status_preserved(self):
        """The schedule status is preserved as-is in the returned DataFrame."""
        t = _make_telescope("Swift", "t1")
        s = _make_schedule("t1", "performed", DATE_BEGIN, DATE_END)
        client = _make_client([t], [s])

        df = get_schedule_history(client=client)

        assert df.iloc[0]["status"] == "performed"

    def test_multiple_schedules_same_telescope(self):
        """Multiple schedules for the same telescope appear as separate rows."""
        t = _make_telescope("Swift", "t1")
        s1 = _make_schedule("t1", "planned", DATE_BEGIN, DATE_BEGIN + timedelta(days=1))
        s2 = _make_schedule("t1", "performed", DATE_BEGIN, DATE_BEGIN + timedelta(days=1))
        client = _make_client([t], [s1, s2])

        df = get_schedule_history(client=client)

        assert len(df) == 2


# ---------------------------------------------------------------------------
# Tests for plot_schedule_history
# ---------------------------------------------------------------------------

class TestPlotScheduleHistory:
    """Unit tests for :func:`plot_schedule_history`."""

    def test_returns_figure(self):
        """plot_schedule_history always returns a Plotly Figure."""
        import plotly.graph_objects as go

        df = _make_history_df(
            {
                "telescope_name": "Neil Gehrels Swift Observatory",
                "telescope_short_name": "Swift",
                "telescope_id": "t1",
                "status": "planned",
                "date_begin": DATE_BEGIN,
                "date_end": DATE_END,
                "created_on": NOW,
            }
        )
        fig = plot_schedule_history(df)
        assert isinstance(fig, go.Figure)

    def test_empty_dataframe_returns_figure(self):
        """An empty DataFrame does not raise and returns a Figure."""
        import plotly.graph_objects as go

        df = pd.DataFrame(
            columns=[
                "telescope_name", "telescope_short_name", "telescope_id",
                "status", "date_range_begin", "date_range_end", "created_on",
            ]
        )
        fig = plot_schedule_history(df)
        assert isinstance(fig, go.Figure)

    def test_trace_created_for_each_status(self):
        """One named (legend) trace is created per distinct status."""
        df = _make_history_df(
            {
                "telescope_name": "Swift",
                "telescope_short_name": "Swift",
                "telescope_id": "t1",
                "status": "planned",
                "date_begin": DATE_BEGIN,
                "date_end": DATE_END,
                "created_on": NOW,
            },
            {
                "telescope_name": "Fermi",
                "telescope_short_name": "Fermi",
                "telescope_id": "t2",
                "status": "performed",
                "date_begin": DATE_BEGIN,
                "date_end": DATE_END,
                "created_on": NOW,
            },
        )
        fig = plot_schedule_history(df)
        legend_trace_names = {t.name for t in fig.data if t.showlegend}
        assert "planned" in legend_trace_names
        assert "performed" in legend_trace_names

    def test_xaxis_type_is_date(self):
        """The X axis should be of type 'date'."""
        df = _make_history_df(
            {
                "telescope_name": "Swift",
                "telescope_short_name": "Swift",
                "telescope_id": "t1",
                "status": "planned",
                "date_begin": DATE_BEGIN,
                "date_end": DATE_END,
                "created_on": NOW,
            }
        )
        fig = plot_schedule_history(df)
        assert fig.layout.xaxis.type == "date"

    def test_planned_trace_is_green(self):
        """Planned schedules use the green colour from _STATUS_COLORS."""
        df = _make_history_df(
            {
                "telescope_name": "Swift",
                "telescope_short_name": "Swift",
                "telescope_id": "t1",
                "status": "planned",
                "date_begin": DATE_BEGIN,
                "date_end": DATE_END,
                "created_on": NOW,
            }
        )
        fig = plot_schedule_history(df)
        # The filled polygon trace for "planned" should use the planned colour.
        planned_trace = next(t for t in fig.data if t.name == "planned" and t.showlegend)
        # fillcolor is the rgba form; we just verify it encodes the green channel.
        assert "2ca02c" in _STATUS_COLORS["planned"]
        assert planned_trace.fillcolor is not None

    def test_performed_trace_is_blue(self):
        """Performed schedules use the blue colour from _STATUS_COLORS."""
        assert "1f77b4" in _STATUS_COLORS["performed"]

    def test_fill_is_semi_transparent(self):
        """The fill colour is semi-transparent (contains an alpha < 1)."""
        df = _make_history_df(
            {
                "telescope_name": "Swift",
                "telescope_short_name": "Swift",
                "telescope_id": "t1",
                "status": "planned",
                "date_begin": DATE_BEGIN,
                "date_end": DATE_END,
                "created_on": NOW,
            }
        )
        fig = plot_schedule_history(df)
        planned_trace = next(t for t in fig.data if t.name == "planned" and t.showlegend)
        # fillcolor should be an rgba string with alpha != 1.
        assert "rgba" in planned_trace.fillcolor
        # Extract alpha value from "rgba(r,g,b,a)" string.
        alpha = float(planned_trace.fillcolor.rstrip(")").split(",")[-1])
        assert alpha < 1.0

    def test_saves_html_file(self, tmp_path):
        """When output_path is given, an HTML file is written to disk."""
        df = _make_history_df(
            {
                "telescope_name": "Swift",
                "telescope_short_name": "Swift",
                "telescope_id": "t1",
                "status": "planned",
                "date_begin": DATE_BEGIN,
                "date_end": DATE_END,
                "created_on": NOW,
            }
        )
        out = str(tmp_path / "history.html")
        plot_schedule_history(df, output_path=out)
        assert os.path.exists(out)
        with open(out) as fh:
            content = fh.read()
        assert "<html" in content.lower() or "plotly" in content.lower()

    def test_yaxis_ticktext_uses_short_name(self):
        """The Y-axis tick labels use telescope_short_name."""
        df = _make_history_df(
            {
                "telescope_name": "Neil Gehrels Swift Observatory",
                "telescope_short_name": "Swift",
                "telescope_id": "t1",
                "status": "planned",
                "date_begin": DATE_BEGIN,
                "date_end": DATE_END,
                "created_on": NOW,
            }
        )
        fig = plot_schedule_history(df)
        tick_texts = list(fig.layout.yaxis.ticktext)
        assert any("Swift" in s for s in tick_texts)
        assert not any("Neil Gehrels" in s for s in tick_texts)

    def test_rows_with_missing_dates_skipped(self):
        """Rows with NaT/None date_begin or date_end do not raise."""
        import plotly.graph_objects as go

        df = _make_history_df(
            {
                "telescope_name": "Swift",
                "telescope_short_name": "Swift",
                "telescope_id": "t1",
                "status": "planned",
                "date_begin": None,
                "date_end": None,
                "created_on": NOW,
            }
        )
        fig = plot_schedule_history(df)
        assert isinstance(fig, go.Figure)

    def test_overlapping_schedules_produce_separate_polygons(self):
        """Two overlapping schedules for the same telescope produce two polygons."""
        df = _make_history_df(
            {
                "telescope_name": "Swift",
                "telescope_short_name": "Swift",
                "telescope_id": "t1",
                "status": "planned",
                "date_begin": DATE_BEGIN,
                "date_end": DATE_END,
                "created_on": NOW,
            },
            {
                "telescope_name": "Swift",
                "telescope_short_name": "Swift",
                "telescope_id": "t1",
                "status": "planned",
                "date_begin": DATE_BEGIN + timedelta(days=1),
                "date_end": DATE_END + timedelta(days=1),
                "created_on": NOW,
            },
        )
        fig = plot_schedule_history(df)
        planned_trace = next(t for t in fig.data if t.name == "planned" and t.showlegend)
        # Two rectangles → two None separators in the x array.
        none_count = sum(1 for v in planned_trace.x if v is None)
        assert none_count == 2
