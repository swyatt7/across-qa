"""CLI entry point for across-qa.

Usage
-----
::

    python -m across_qa.main [--telescope TELESCOPE] [--status STATUS] [--exit-code]
                             [--plot] [--plot-output PATH]

    # or via the installed script:
    across-qa [--telescope TELESCOPE] [--status STATUS] [--exit-code]
              [--plot] [--plot-output PATH]

Options
-------
--telescope TELESCOPE
    Filter by telescope name (case-insensitive substring match).
--status STATUS
    Filter cadence results by schedule status (e.g. ``planned``, ``performed``).
--exit-code
    Exit with a non-zero status code when any check is LATE or MISSING.
--plot
    Open an interactive Plotly timeline in the default web browser.
--plot-output PATH
    Save the Plotly timeline as an HTML file at PATH (implies ``--plot``).
"""

from __future__ import annotations

import argparse
import logging
import sys

from across.client import Client

from across_qa.checker import check_telescope_ingestion_status
from across_qa.visualization import plot_ingesetion_status_timeline


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="across-qa",
        description=(
            "Compare telescope schedule ingestion against expected cadence "
            "using the NASA-ACROSS API."
        ),
    )
    parser.add_argument(
        "--telescope",
        metavar="NAME",
        default=None,
        help="Filter results to telescopes whose name contains NAME (case-insensitive).",
    )
    parser.add_argument(
        "--status",
        metavar="STATUS",
        default=None,
        help="Filter results to a specific schedule status (e.g. planned, performed).",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Return a non-zero exit code when any check is LATE or MISSING.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        default=False,
        help="Enable verbose logging.",
    )
    parser.add_argument(
        "--plot",
        action="store_true",
        default=False,
        help="Open an interactive Plotly timeline in the default web browser.",
    )
    parser.add_argument(
        "--plot-output",
        metavar="PATH",
        default=None,
        help="Save the Plotly timeline as an HTML file at PATH (implies --plot).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point for the across-qa CLI.

    Parameters
    ----------
    argv:
        Argument list (defaults to ``sys.argv[1:]``).

    Returns
    -------
    int
        Exit code: ``0`` on success, ``1`` when ``--exit-code`` is set and at
        least one check is LATE or MISSING.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    client = Client()
    df = check_telescope_ingestion_status(client=client)

    # Apply optional filters
    if args.telescope:
        df = df[df["telescope_name"].str.contains(args.telescope, case=False, na=False, regex=False)]
    if args.status:
        df = df[df["schedule_status"].str.lower() == args.status.lower()]

    if df.empty:
        print("No results found (check your filters).")
        return 0

    print(df.to_string(index=False))

    # ------------------------------------------------------------------ #
    # Optional visualization
    # ------------------------------------------------------------------ #
    if args.plot or args.plot_output:
        fig = plot_ingesetion_status_timeline(df, output_path=args.plot_output)
        if args.plot_output:
            print(f"Timeline saved to {args.plot_output}")
        if args.plot:
            fig.show()

    if args.exit_code and df["status"].isin(["LATE", "MISSING"]).any():
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
