"""Post telescope schedule ingestion status to a Slack channel.

This script is the entry point for the ``slack-ingestion-check`` GitHub
Actions workflow.  It:

1. Calls :func:`across_qa.checker.check_telescope_ingestion_status` to
   retrieve the current ingestion health for every telescope.
2. Calls :func:`across_qa.visualization.plot_ingesetion_status_timeline` to
   generate a static PNG of the timeline.
3. Builds a Slack message that lists any LATE or MISSING telescopes, or
   reports that everything looks good when all checks pass.
4. Uploads the PNG and posts the message to the configured Slack channel.

Use ``--dry-run`` to skip all Slack calls: the message is printed to stdout
and the Plotly timeline is shown interactively (``fig.show()``).  No
environment variables are required in dry-run mode.

Required environment variables (normal mode)
---------------------------------------------
SLACK_BOT_TOKEN
    A Slack Bot User OAuth Token (``xoxb-...``) with the following scopes:
    - ``chat:write`` — post messages
    - ``files:write`` — upload the PNG attachment

SLACK_CHANNEL_ID
    The ID of the Slack channel to post into (e.g. ``C01234ABCDE``).
    The bot must be invited to the channel before this script will work.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile

from across.client import Client

from across_qa.checker import check_telescope_ingestion_status
from across_qa.visualization import plot_ingesetion_status_timeline

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require_env(name: str) -> str:
    """Return the value of *name* or exit with an error message."""
    value = os.environ.get(name, "").strip()
    if not value:
        print(f"ERROR: environment variable {name!r} is not set.", file=sys.stderr)
        sys.exit(1)
    return value


def _build_slack_message(df) -> str:  # type: ignore[type-arg]
    """Return the Slack message text based on the ingestion-status DataFrame."""
    issues = df[df["status"].isin(["LATE", "MISSING"])]

    if issues.empty:
        return (
            ":white_check_mark: *Telescope ingestion check — all good!*\n"
            "No late or missing ingestion tasks detected."
        )

    lines = [":warning: *Telescope ingestion check — issues detected!*\n"]

    missing = issues[issues["status"] == "MISSING"]
    if not missing.empty:
        lines.append("*Missing ingestion tasks* (never ingested):")
        for _, row in missing.iterrows():
            lines.extend([
                f"   • `{row['telescope_name']}` (status: `{row['schedule_status']}`) ",
                f"      • {row['message']}"
            ])

    late = issues[issues["status"] == "LATE"]
    if not late.empty:
        lines.append("\n*Late ingestion tasks* (missed scheduled runs):")
        for _, row in late.iterrows():
            lines.extend([
                f"   • `{row['telescope_name']}` (status: `{row['schedule_status']}`) ",
                f"      • {row['message']}"
            ])

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Post telescope ingestion status to Slack.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Print the Slack message to stdout and show the Plotly timeline "
            "interactively instead of sending anything to Slack."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)

    # 1. Fetch ingestion status
    client = Client()
    df = check_telescope_ingestion_status(client=client)

    # 2. Build message text
    message_text = _build_slack_message(df)

    # 3. Generate the timeline figure
    fig = plot_ingesetion_status_timeline(df)

    if args.dry_run:
        # Dry-run: print to console and show figure interactively
        print(message_text)
        fig.show()
        return

    # Normal mode: require Slack credentials
    from slack_sdk import WebClient  # type: ignore
    from slack_sdk.errors import SlackApiError  # type: ignore

    token = _require_env("SLACK_BOT_TOKEN")
    channel_id = _require_env("SLACK_CHANNEL_ID")

    slack = WebClient(token=token)

    # 4. Export the timeline to a temporary PNG and post to Slack
    with tempfile.TemporaryDirectory() as tmpdir:
        png_path = os.path.join(tmpdir, "ingestion_status.png")
        fig.write_image(png_path)

        try:
            # Post the text message first
            post_resp = slack.chat_postMessage(
                channel=channel_id,
                text=message_text,
            )
            thread_ts = post_resp["ts"]

            # Upload the PNG as a reply in the same thread
            slack.files_upload_v2(
                channel=channel_id,
                file=png_path,
                filename="ingestion_status.png",
                title="Telescope Ingestion Status Timeline",
                thread_ts=thread_ts,
            )
        except SlackApiError as exc:
            print(f"Slack API error: {exc.response['error']}", file=sys.stderr)
            sys.exit(1)

    print("Slack notification sent successfully.")


if __name__ == "__main__":
    main()
