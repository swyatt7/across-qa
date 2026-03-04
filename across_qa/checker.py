"""
Core QA logic for comparing telescope schedule ingestion against expected cadence.

For each telescope registered in the ACROSS server this module:
  1. Fetches the telescope's ``schedule_cadences`` (cron expressions + expected status).
  2. Queries the most recently created schedule for that telescope / status combination.
  3. Uses ``croniter`` to compute when the *next* schedule was due after the last
     ingested one and decides whether ingestion is on-time, late, or missing.

The public entry point is :func:`check_all_telescopes`, which returns a list of
:class:`CadenceResult` objects that callers can inspect or print.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING

from croniter import croniter

from across.client import Client

if TYPE_CHECKING:  # pragma: no cover
    import across.sdk.v1 as sdk

logger = logging.getLogger(__name__)


class Status(str, Enum):
    """Ingestion health status for a single cadence check."""

    OK = "OK"
    LATE = "LATE"
    MISSING = "MISSING"
    NO_CADENCE = "NO_CADENCE"


@dataclass
class CadenceResult:
    """Result of a single telescope / cadence check."""

    telescope_name: str
    telescope_id: str
    schedule_status: str
    cron: str | None
    last_ingested: datetime | None
    next_expected: datetime | None
    status: Status
    message: str

    def __str__(self) -> str:
        last = (
            self.last_ingested.strftime("%Y-%m-%dT%H:%M:%SZ")
            if self.last_ingested
            else "never"
        )
        nxt = (
            self.next_expected.strftime("%Y-%m-%dT%H:%M:%SZ")
            if self.next_expected
            else "N/A"
        )
        return (
            f"[{self.status.value:10s}] {self.telescope_name} "
            f"(status={self.schedule_status}, cron={self.cron!r}) "
            f"last_ingested={last} next_expected={nxt} — {self.message}"
        )


def _now_utc() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(tz=timezone.utc)


def _latest_schedule(
    client: Client,
    telescope_id: str,
    schedule_status: str,
) -> "sdk.Schedule | None":
    """Return the most recently *created* schedule for a telescope+status pair.

    Fetches up to the first page of results (newest first based on
    ``created_on``) and returns the first item, or ``None`` when there are no
    schedules.
    """
    import across.sdk.v1 as sdk_types

    try:
        status_enum = sdk_types.ScheduleStatus(schedule_status)
    except ValueError:
        logger.warning("Unknown schedule status %r; skipping", schedule_status)
        return None

    try:
        page = client.schedule.get_many(
            telescope_ids=[telescope_id],
            status=status_enum,
            page=1,
            page_limit=1,
        )
    except Exception:
        logger.exception(
            "Failed to fetch schedules for telescope_id=%s status=%s",
            telescope_id,
            schedule_status,
        )
        return None

    if page.items:
        return page.items[0]
    return None


def check_cadence(
    telescope_name: str,
    telescope_id: str,
    cron: str | None,
    schedule_status: str,
    last_ingested: datetime | None,
    now: datetime | None = None,
) -> CadenceResult:
    """Evaluate whether schedule ingestion is on schedule.

    Parameters
    ----------
    telescope_name:
        Human-readable telescope name (for reporting).
    telescope_id:
        ACROSS telescope UUID.
    cron:
        Cron expression that defines the expected ingestion cadence, or
        ``None`` when no cadence is configured.
    schedule_status:
        The schedule status being checked (e.g. ``"planned"``).
    last_ingested:
        Timestamp of the most recently created schedule, or ``None`` when no
        schedule has ever been ingested.
    now:
        Current UTC time used as the reference point.  Defaults to the real
        current UTC time; injectable for deterministic testing.

    Returns
    -------
    CadenceResult
    """
    if now is None:
        now = _now_utc()

    # ------------------------------------------------------------------ #
    # No cadence configured
    # ------------------------------------------------------------------ #
    if not cron:
        return CadenceResult(
            telescope_name=telescope_name,
            telescope_id=telescope_id,
            schedule_status=schedule_status,
            cron=cron,
            last_ingested=last_ingested,
            next_expected=None,
            status=Status.NO_CADENCE,
            message="No cron cadence configured for this telescope/status.",
        )

    # ------------------------------------------------------------------ #
    # No schedule ever ingested
    # ------------------------------------------------------------------ #
    if last_ingested is None:
        return CadenceResult(
            telescope_name=telescope_name,
            telescope_id=telescope_id,
            schedule_status=schedule_status,
            cron=cron,
            last_ingested=None,
            next_expected=None,
            status=Status.MISSING,
            message="No schedule has ever been ingested for this telescope/status.",
        )

    # ------------------------------------------------------------------ #
    # Compute when the next schedule was due after the last ingestion
    # ------------------------------------------------------------------ #
    # Make sure we work with a timezone-aware datetime throughout.
    if last_ingested.tzinfo is None:
        last_ingested = last_ingested.replace(tzinfo=timezone.utc)

    try:
        cron_iter = croniter(cron, last_ingested)
        next_expected: datetime = cron_iter.get_next(datetime)
        if next_expected.tzinfo is None:
            next_expected = next_expected.replace(tzinfo=timezone.utc)
    except Exception:
        logger.exception("Failed to parse cron expression %r", cron)
        return CadenceResult(
            telescope_name=telescope_name,
            telescope_id=telescope_id,
            schedule_status=schedule_status,
            cron=cron,
            last_ingested=last_ingested,
            next_expected=None,
            status=Status.NO_CADENCE,
            message=f"Could not parse cron expression {cron!r}.",
        )

    if now >= next_expected:
        return CadenceResult(
            telescope_name=telescope_name,
            telescope_id=telescope_id,
            schedule_status=schedule_status,
            cron=cron,
            last_ingested=last_ingested,
            next_expected=next_expected,
            status=Status.LATE,
            message=(
                f"Next schedule was expected by {next_expected.strftime('%Y-%m-%dT%H:%M:%SZ')} "
                f"but none has been ingested yet (now={now.strftime('%Y-%m-%dT%H:%M:%SZ')})."
            ),
        )

    return CadenceResult(
        telescope_name=telescope_name,
        telescope_id=telescope_id,
        schedule_status=schedule_status,
        cron=cron,
        last_ingested=last_ingested,
        next_expected=next_expected,
        status=Status.OK,
        message=(
            f"Schedule is up-to-date. Next expected by "
            f"{next_expected.strftime('%Y-%m-%dT%H:%M:%SZ')}."
        ),
    )


def check_all_telescopes(
    client: Client | None = None,
    now: datetime | None = None,
) -> list[CadenceResult]:
    """Fetch all telescopes and evaluate each cadence against recent schedules.

    Parameters
    ----------
    client:
        An initialised :class:`~across.client.Client` instance.  When
        ``None`` a default (unauthenticated) client is created.
    now:
        Override the current UTC time used for lateness checks.  Useful in
        tests to produce deterministic results.

    Returns
    -------
    list[CadenceResult]
        One entry per (telescope, cadence) pair.
    """
    if client is None:
        client = Client()

    if now is None:
        now = _now_utc()

    telescopes = client.telescope.get_many()
    results: list[CadenceResult] = []

    for telescope in telescopes:
        cadences = telescope.schedule_cadences or []

        if not cadences:
            results.append(
                CadenceResult(
                    telescope_name=telescope.name,
                    telescope_id=telescope.id,
                    schedule_status="",
                    cron=None,
                    last_ingested=None,
                    next_expected=None,
                    status=Status.NO_CADENCE,
                    message="Telescope has no schedule cadences configured.",
                )
            )
            continue

        for cadence in cadences:
            latest = _latest_schedule(
                client,
                telescope_id=telescope.id,
                schedule_status=cadence.schedule_status,
            )
            last_ingested = latest.created_on if latest is not None else None
            result = check_cadence(
                telescope_name=telescope.name,
                telescope_id=telescope.id,
                cron=cadence.cron,
                schedule_status=cadence.schedule_status,
                last_ingested=last_ingested,
                now=now,
            )
            results.append(result)
            logger.debug("Checked %s: %s", telescope.name, result)

    return results
