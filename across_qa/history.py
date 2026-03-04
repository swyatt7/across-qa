"""Schedule history retrieval and visualization for across-qa.

This module provides two public functions:

1. :func:`get_schedule_history` — fetches the complete schedule history for
   one or more telescopes from the ACROSS API and returns it as a
   ``pandas.DataFrame``.

2. :func:`plot_schedule_history` — builds an interactive Plotly timeline of
   the returned schedule history, useful for assessing completeness and
   identifying gaps in ingestion.

Usage
-----
::

    from across_qa.history import get_schedule_history, plot_schedule_history

    df = get_schedule_history()                            # all telescopes, last 90 days
    df = get_schedule_history(date_begin=datetime(...))    # custom start date
    df = get_schedule_history(telescope_identifiers=["Swift", "Fermi"])  # subset

    fig = plot_schedule_history(df)            # display in notebook / browser
    fig = plot_schedule_history(df, "history.html")  # also save to file
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import pandas as pd
import plotly.graph_objects as go

from across.client import Client
from across_qa.checker import _now_utc, _status_value

if TYPE_CHECKING:  # pragma: no cover
    import across.sdk.v1 as sdk

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Default look-back window when no ``date_begin`` is supplied.
_DEFAULT_LOOKBACK_DAYS: int = 90

#: Columns returned by :func:`get_schedule_history`.
_HISTORY_COLUMNS: list[str] = [
    "telescope_name",
    "telescope_short_name",
    "telescope_id",
    "status",
    "date_begin",
    "date_end",
    "created_on",
]

#: Colours used for each schedule status in the history timeline.
#: planned / scheduled share the same green; performed uses blue.
_STATUS_COLORS: dict[str, str] = {
    "planned": "#2ca02c",    # green
    "scheduled": "#2ca02c",  # green (alias for planned)
    "performed": "#1f77b4",  # blue
}
_DEFAULT_COLOR: str = "#7f7f7f"   # grey for any unrecognised status

#: Opacity applied to the interior fill of each schedule rectangle.
_FILL_ALPHA: float = 0.25

#: Half-height of each rectangle in y-axis data units.
_BOX_HALF_HEIGHT: float = 0.35


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _hex_to_rgba(hex_color: str, alpha: float) -> str:
    """Convert a ``#rrggbb`` hex string and alpha float to an RGBA CSS string."""
    h = hex_color.lstrip("#")
    r, g, b = (int(h[i : i + 2], 16) for i in (0, 2, 4))
    return f"rgba({r},{g},{b},{alpha})"


def _fmt_dt(value: object) -> str:
    """Format a datetime-like value as an ISO-8601 UTC string, or fall back to str()."""
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")  # type: ignore[union-attr]
    return str(value)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_schedule_history(
    client: Client | None = None,
    date_begin: datetime | None = None,
    telescope_identifiers: list[str] | None = None,
) -> pd.DataFrame:
    """Fetch the complete schedule history for a set of telescopes.

    Performs at most **two** API calls:

    1. ``client.telescope.get_many()`` — retrieves all telescopes so that
       name/short-name identifiers can be resolved to IDs.
    2. ``client.schedule.get_many(telescope_ids=[...], date_begin=...)`` —
       retrieves all schedules for those telescopes starting from
       ``date_begin``.

    Parameters
    ----------
    client:
        An initialised :class:`~across.client.Client` instance.  When
        ``None`` a default (unauthenticated) client is created.
    date_begin:
        Earliest schedule start date to include.  Defaults to
        :data:`_DEFAULT_LOOKBACK_DAYS` days before the current UTC time.
        Naive datetimes are treated as UTC.
    telescope_identifiers:
        Telescope names (full or short) or UUID strings to include.  When
        ``None`` the history for **all** telescopes is returned.

    Returns
    -------
    pd.DataFrame
        One row per schedule with columns: ``telescope_name``,
        ``telescope_short_name``, ``telescope_id``, ``status``,
        ``date_begin``, ``date_end``, ``created_on``.
        Returns an empty DataFrame (with the correct columns) when no
        matching schedules are found.
    """
    if client is None:
        client = Client()

    if date_begin is None:
        date_begin = _now_utc() - timedelta(days=_DEFAULT_LOOKBACK_DAYS)

    # ------------------------------------------------------------------
    # 1. Fetch all telescopes for name/short-name → id resolution.
    # ------------------------------------------------------------------
    telescopes: list["sdk.Telescope"] = client.telescope.get_many()

    if telescope_identifiers is not None:
        id_set = set(telescope_identifiers)
        telescopes = [
            t
            for t in telescopes
            if t.id in id_set
            or t.name in id_set
            or (getattr(t, "short_name", None) in id_set)
        ]

    if not telescopes:
        return pd.DataFrame(columns=_HISTORY_COLUMNS)

    telescope_map: dict[str, "sdk.Telescope"] = {t.id: t for t in telescopes}
    telescope_ids = list(telescope_map.keys())

    # ------------------------------------------------------------------
    # 2. Single bulk schedule fetch.
    # ------------------------------------------------------------------
    try:
        result = client.schedule.get_many(
            telescope_ids=telescope_ids,
            date_begin=date_begin,
        )
    except Exception:
        logger.exception(
            "Failed to fetch schedule history for telescope_ids=%s", telescope_ids
        )
        return pd.DataFrame(columns=_HISTORY_COLUMNS)

    schedules: list["sdk.Schedule"] = result.items

    # ------------------------------------------------------------------
    # 3. Build the DataFrame.
    # ------------------------------------------------------------------
    rows = []
    for sched in schedules:
        t = telescope_map.get(sched.telescope_id)
        if t is None:
            # The API returned a schedule for a telescope outside the requested
            # set (e.g. due to an API inconsistency).  Skip it so the caller
            # only sees schedules belonging to the telescopes they asked for.
            logger.debug(
                "Skipping schedule for unknown telescope_id=%s", sched.telescope_id
            )
            continue
        rows.append(
            {
                "telescope_name": t.name,
                "telescope_short_name": getattr(t, "short_name", None) or t.name,
                "telescope_id": sched.telescope_id,
                "status": _status_value(sched.status),
                "date_begin": getattr(sched, "date_begin", None),
                "date_end": getattr(sched, "date_end", None),
                "created_on": getattr(sched, "created_on", None),
            }
        )

    if not rows:
        return pd.DataFrame(columns=_HISTORY_COLUMNS)

    return pd.DataFrame(rows, columns=_HISTORY_COLUMNS)


def plot_schedule_history(
    df: pd.DataFrame,
    output_path: str | None = None,
) -> go.Figure:
    """Build an interactive Plotly timeline of telescope schedule history.

    Each schedule is rendered as a coloured rectangle spanning its
    ``date_begin``–``date_end`` range on the date (X) axis.  Overlapping
    rectangles are drawn with a semi-transparent fill so all records
    remain visible simultaneously.

    Colour scheme
    -------------
    * **planned / scheduled** — green
    * **performed** — blue
    * **other** — grey

    Parameters
    ----------
    df:
        DataFrame as returned by :func:`get_schedule_history`.
        Required columns: ``telescope_short_name``, ``status``,
        ``date_begin``, ``date_end``.
    output_path:
        Optional path to save an HTML export of the figure.

    Returns
    -------
    go.Figure
        Interactive Plotly figure.  Call ``.show()`` to open in a browser.
    """
    fig = go.Figure()

    if df.empty:
        _apply_history_layout(fig)
        if output_path:
            fig.write_html(output_path)
        return fig

    df = df.copy()

    # Ensure tz-aware datetimes so Plotly renders correctly.
    for col in ("date_begin", "date_end"):
        if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
            if df[col].dt.tz is None:
                df[col] = df[col].dt.tz_localize("UTC")

    # Decide which column to use for the Y-axis labels.
    name_col = (
        "telescope_short_name"
        if "telescope_short_name" in df.columns
        else "telescope_name"
    )

    # Preserve order of first appearance so the chart is stable.
    telescope_names: list[str] = list(dict.fromkeys(df[name_col]))
    y_map: dict[str, int] = {name: i for i, name in enumerate(telescope_names)}

    # Process statuses in a predictable order (known ones first, rest sorted).
    _known = ["planned", "scheduled", "performed"]
    status_order = _known + sorted(
        s for s in df["status"].unique() if s not in _known
    )

    for status in status_order:
        subset = df[df["status"] == status]
        if subset.empty:
            continue

        hex_color = _STATUS_COLORS.get(status, _DEFAULT_COLOR)
        fill_color = _hex_to_rgba(hex_color, _FILL_ALPHA)
        border_color = _hex_to_rgba(hex_color, 1.0)

        # Polygon coordinates for each rectangle (closed path + None separator).
        poly_x: list = []
        poly_y: list = []

        # Invisible centre-point markers – used solely for hover tooltips.
        center_x: list = []
        center_y: list = []
        hover_texts: list[str] = []

        for _, row in subset.iterrows():
            x0 = row.get("date_begin")
            x1 = row.get("date_end")
            if pd.isna(x0) or pd.isna(x1):
                continue

            y_idx = y_map[row[name_col]]
            y0 = y_idx - _BOX_HALF_HEIGHT
            y1 = y_idx + _BOX_HALF_HEIGHT

            # Closed rectangle polygon followed by a None break.
            poly_x.extend([x0, x1, x1, x0, x0, None])
            poly_y.extend([y0, y0, y1, y1, y0, None])

            # Centre point for hover.
            center_x.append(x0 + (x1 - x0) / 2)
            center_y.append(y_idx)

            label = row.get("telescope_name", row[name_col])
            hover_texts.append(
                f"<b>{label}</b><br>"
                f"Status: {row['status']}<br>"
                f"Begin: {_fmt_dt(x0)}<br>"
                f"End: {_fmt_dt(x1)}"
            )

        if not poly_x:
            continue

        # Filled polygon trace (hover disabled; tooltips come from the markers).
        fig.add_trace(
            go.Scatter(
                x=poly_x,
                y=poly_y,
                mode="lines",
                fill="toself",
                fillcolor=fill_color,
                line=dict(color=border_color, width=2),
                name=status,
                legendgroup=status,
                hoverinfo="skip",
                showlegend=True,
            )
        )

        # Invisible centre markers carry the hover tooltips.
        fig.add_trace(
            go.Scatter(
                x=center_x,
                y=center_y,
                mode="markers",
                marker=dict(opacity=0, size=10, color=hex_color),
                name=status,
                legendgroup=status,
                showlegend=False,
                hovertemplate="%{customdata}<extra></extra>",
                customdata=hover_texts,
            )
        )

    _apply_history_layout(fig, y_map=y_map, telescope_names=telescope_names)

    if output_path:
        fig.write_html(output_path)

    return fig


def _apply_history_layout(
    fig: go.Figure,
    y_map: dict[str, int] | None = None,
    telescope_names: list[str] | None = None,
) -> None:
    """Apply standard layout to a schedule-history figure (mutates *fig*)."""
    yaxis_kwargs: dict = dict(title="Telescope")
    if y_map and telescope_names:
        yaxis_kwargs.update(
            tickmode="array",
            tickvals=list(y_map.values()),
            ticktext=telescope_names,
        )

    n_telescopes = len(telescope_names) if telescope_names else 1
    fig.update_layout(
        title="Telescope Schedule History",
        xaxis=dict(title="Date (UTC)", type="date"),
        yaxis=yaxis_kwargs,
        legend=dict(title="Schedule Status", traceorder="normal"),
        hovermode="closest",
        template="plotly_white",
        height=max(400, 100 + 60 * n_telescopes),
    )
