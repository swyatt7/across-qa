"""Plotly timeline visualization for across-qa schedule status.

Creates an interactive HTML timeline where the X axis shows time and each
telescope / schedule-status combination is plotted as a marker coloured by
its ingestion health:

* **OK** — green
* **LATE** — yellow
* **MISSING** — red
* **NO_CADENCE** — black

Usage
-----
::

    from across_qa.visualization import plot_timeline

    fig = plot_timeline(df)          # display in notebook / browser
    fig = plot_timeline(df, "out.html")  # also save to file
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import plotly.graph_objects as go

# Ordered so the legend reads: OK → LATE → MISSING → NO_CADENCE
_STATUS_ORDER = ["OK", "LATE", "MISSING", "NO_CADENCE"]

_STATUS_COLORS: dict[str, str] = {
    "OK": "#2ca02c",        # green
    "LATE": "#f5c518",      # yellow-amber
    "MISSING": "#d62728",   # red
    "NO_CADENCE": "#1a1a1a",  # near-black
}

_STATUS_SYMBOLS: dict[str, str] = {
    "OK": "circle",
    "LATE": "diamond",
    "MISSING": "x",
    "NO_CADENCE": "square",
}


def plot_timeline(
    df: pd.DataFrame,
    output_path: str | None = None,
) -> go.Figure:
    """Build a Plotly timeline of telescope schedule ingestion health.

    Parameters
    ----------
    df:
        DataFrame as returned by :func:`across_qa.checker.check_all_telescopes`.
        Expected columns: ``telescope_name``, ``schedule_status``,
        ``last_ingested``, ``next_expected``, ``status``, ``message``.
    output_path:
        Optional file path (e.g. ``"report.html"``) to which the interactive
        figure is saved.  The figure is returned regardless.

    Returns
    -------
    go.Figure
        Plotly figure object.  Call ``.show()`` to open it in a browser.
    """
    now = datetime.now(tz=timezone.utc)

    df = df.copy()

    # Build a human-readable Y-axis label per row.
    df["label"] = df["telescope_name"] + " (" + df["schedule_status"] + ")"

    # Ensure datetime columns are tz-aware so Plotly renders them correctly.
    for col in ("last_ingested", "next_expected"):
        if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
            if df[col].dt.tz is None:
                df[col] = df[col].dt.tz_localize("UTC")

    fig = go.Figure()

    # ------------------------------------------------------------------ #
    # One trace per status so the legend is clean and togglable.
    # ------------------------------------------------------------------ #
    for status in _STATUS_ORDER:
        subset = df[df["status"] == status]
        if subset.empty:
            continue

        color = _STATUS_COLORS[status]
        symbol = _STATUS_SYMBOLS[status]

        # --- last_ingested markers ------------------------------------ #
        # Rows without a last_ingested time still appear — placed at `now`
        # with a special hover note so they are visible on the chart.
        x_last = subset["last_ingested"].where(
            subset["last_ingested"].notna(), other=now
        )
        hover_last = subset.apply(
            lambda r: (
                f"<b>{r['telescope_name']}</b> — {r['schedule_status']}<br>"
                f"Status: <b>{r['status']}</b><br>"
                f"Last ingested: {r['last_ingested'].strftime('%Y-%m-%dT%H:%M:%SZ') if pd.notna(r['last_ingested']) else 'never'}<br>"
                f"Next expected: {r['next_expected'].strftime('%Y-%m-%dT%H:%M:%SZ') if pd.notna(r['next_expected']) else 'N/A'}<br>"
                f"{r['message']}"
            ),
            axis=1,
        )

        fig.add_trace(
            go.Scatter(
                x=x_last,
                y=subset["label"],
                mode="markers",
                name=status,
                legendgroup=status,
                marker=dict(color=color, size=14, symbol=symbol, line=dict(width=1, color="white")),
                hovertemplate="%{customdata}<extra></extra>",
                customdata=hover_last,
                showlegend=True,
            )
        )

        # --- connector lines to next_expected ------------------------- #
        # Draw a thin horizontal line from last_ingested → next_expected so
        # the viewer can see how far ahead (or behind) each schedule is.
        has_next = subset[subset["next_expected"].notna()]
        if not has_next.empty:
            x_next = has_next["next_expected"].where(
                has_next["next_expected"].notna(), other=now
            )
            # Add the next_expected markers in a faded version of the same colour.
            fig.add_trace(
                go.Scatter(
                    x=x_next,
                    y=has_next["label"],
                    mode="markers",
                    name=f"{status} (next expected)",
                    legendgroup=status,
                    marker=dict(
                        color=color,
                        size=10,
                        symbol="triangle-right",
                        opacity=0.5,
                        line=dict(width=1, color="white"),
                    ),
                    hovertemplate=(
                        "<b>%{y}</b><br>Next expected: %{x}<extra></extra>"
                    ),
                    showlegend=False,
                )
            )

            # Connector lines — one shape per row to avoid connecting across
            # different telescopes.
            for _, row in has_next.iterrows():
                li = row["last_ingested"] if pd.notna(row["last_ingested"]) else now
                ne = row["next_expected"]
                fig.add_shape(
                    type="line",
                    xref="x",
                    yref="y",
                    x0=li,
                    x1=ne,
                    y0=row["label"],
                    y1=row["label"],
                    line=dict(color=color, width=2, dash="dot"),
                )

    # ------------------------------------------------------------------ #
    # "Now" reference line — vertical line on the time (X) axis.
    # Use add_shape + add_annotation to avoid a Plotly 6 bug where
    # add_vline fails on datetime axes with annotations.
    # ------------------------------------------------------------------ #
    now_iso = now.isoformat()
    fig.add_shape(
        type="line",
        xref="x",
        yref="paper",
        x0=now_iso,
        x1=now_iso,
        y0=0,
        y1=1,
        line=dict(color="royalblue", width=2, dash="dash"),
    )
    fig.add_annotation(
        xref="x",
        yref="paper",
        x=now_iso,
        y=1,
        text=f"now ({now.strftime('%Y-%m-%dT%H:%M:%SZ')})",
        showarrow=False,
        xanchor="left",
        yanchor="bottom",
        font=dict(color="royalblue"),
    )

    # ------------------------------------------------------------------ #
    # Layout
    # ------------------------------------------------------------------ #
    fig.update_layout(
        title="Telescope Schedule Ingestion Status",
        xaxis=dict(title="Time (UTC)", type="date"),
        yaxis=dict(title="Telescope (schedule status)"),
        legend=dict(title="Ingestion Status", traceorder="normal"),
        hovermode="closest",
        template="plotly_white",
        height=600,
    )

    if output_path:
        fig.write_html(output_path)

    return fig
