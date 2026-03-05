"""
Core QA logic for comparing telescope schedule ingestion against expected cadence.

For each telescope registered in the ACROSS server this module:
  1. Fetches all telescopes' ``schedule_cadences`` (cron expressions + expected status).
  2. Issues a **single bulk query** for all schedules across every telescope ID.
  3. Parses the returned schedules in-memory to find the most recently created one
     per (telescope, status) pair.
  4. Uses ``croniter`` to compute when the *next* schedule was due after the last
     ingested one and decides whether ingestion is on-time, late, or missing.

The public entry point is :func:`check_all_telescopes`, which returns a
``pandas.DataFrame`` with one row per (telescope, cadence) pair.
"""

from __future__ import annotations

import dataclasses
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING

import pandas as pd #type: ignore
from croniter import croniter  #type: ignore

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
    telescope_short_name: str
    telescope_id: str
    schedule_status: str
    cron: str | None
    last_ingested: datetime | None
    ingested_attempts: list[datetime]
    next_ingestion_attempt: datetime | None
    status: Status
    message: str

    def __str__(self) -> str:
        last = (
            self.last_ingested.strftime("%Y-%m-%dT%H:%M:%SZ")
            if self.last_ingested
            else "never"
        )
        nxt = (
            self.next_ingestion_attempt.strftime("%Y-%m-%dT%H:%M:%SZ")
            if self.next_ingestion_attempt
            else "N/A"
        )
        attempts_str = ", ".join(
            a.strftime("%Y-%m-%dT%H:%M:%SZ") for a in self.ingested_attempts
        ) if self.ingested_attempts else "none"
        
        return (
            f"[{self.status.value:10s}] {self.telescope_name} "
            f"(status={self.schedule_status}, cron={self.cron!r}) "
            f"last_ingested={last} missed_attempts=[{attempts_str}] "
            f"next_attempt={nxt} — {self.message}"
        )


def _now_utc() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(tz=timezone.utc)


def _status_value(status_field) -> str:
    """Normalise a ScheduleStatus enum or plain string to its string value."""
    return status_field.value if hasattr(status_field, "value") else str(status_field)


def _fetch_all_schedules(
    client: Client,
    telescope_ids: list[str],
) -> list["sdk.Schedule"]:
    """Fetch schedules for the given telescope IDs via a single API call.

    The API returns the most recent schedule for each date-range and status
    combination, so no pagination is required.

    Parameters
    ----------
    client:
        Initialised ACROSS client.
    telescope_ids:
        IDs of telescopes whose schedules should be retrieved.

    Returns
    -------
    list[sdk.Schedule]
        Schedules returned by the API for those telescopes.
    """
    if not telescope_ids:
        return []

    try:
        result = client.schedule.get_many(telescope_ids=telescope_ids)  #type: ignore
    except Exception:
        logger.exception(
            "Failed to fetch schedules for telescope_ids=%s", telescope_ids
        )
        return []

    return result.items


def _build_latest_lookup(
    schedules: list["sdk.Schedule"],
) -> dict[tuple[str, str], "sdk.Schedule"]:
    """Return a mapping of ``(telescope_id, status_value)`` → most-recently created schedule."""
    latest: dict[tuple[str, str], "sdk.Schedule"] = {}
    for sched in schedules:
        status_val = _status_value(sched.status)
        if status_val == "scheduled":
            status_val = "planned"
        key = (sched.telescope_id, status_val)
        existing = latest.get(key)
        if existing is None or sched.created_on > existing.created_on:
            latest[key] = sched
    return latest


def check_cadence(
    telescope_name: str,
    telescope_id: str,
    cron: str | None,
    schedule_status: str,
    last_ingested: datetime | None,
    now: datetime | None = None,
    telescope_short_name: str | None = None,
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
    telescope_short_name:
        Abbreviated telescope name used for display purposes.  Defaults to
        ``telescope_name`` when not provided.

    Returns
    -------
    CadenceResult
        Contains last_ingested, ingested_attempts (missed runs), next_ingestion_attempt,
        and status (NO_CADENCE, OK, LATE, or MISSING).
    """
    if now is None:
        now = _now_utc()

    short_name = telescope_short_name or telescope_name

    # ------------------------------------------------------------------ #
    # No cadence configured
    # ------------------------------------------------------------------ #
    if not cron:
        return CadenceResult(
            telescope_name=telescope_name,
            telescope_short_name=short_name,
            telescope_id=telescope_id,
            schedule_status=schedule_status,
            cron=cron,
            last_ingested=last_ingested,
            ingested_attempts=[],
            next_ingestion_attempt=None,
            status=Status.NO_CADENCE,
            message="No cron cadence configured for this telescope/status.",
        )

    # ------------------------------------------------------------------ #
    # Validate cron expression
    # ------------------------------------------------------------------ #
    try:
        cron_iter = croniter(cron, now)
    except Exception:
        logger.exception("Failed to parse cron expression %r", cron)
        return CadenceResult(
            telescope_name=telescope_name,
            telescope_short_name=short_name,
            telescope_id=telescope_id,
            schedule_status=schedule_status,
            cron=cron,
            last_ingested=last_ingested,
            ingested_attempts=[],
            next_ingestion_attempt=None,
            status=Status.NO_CADENCE,
            message=f"Could not parse cron expression {cron!r}.",
        )

    # ------------------------------------------------------------------ #
    # Calculate next ingestion attempt based on now
    # ------------------------------------------------------------------ #
    cron_iter = croniter(cron, now)
    next_ingestion_attempt: datetime = cron_iter.get_next(datetime)
    if next_ingestion_attempt.tzinfo is None:
        next_ingestion_attempt = next_ingestion_attempt.replace(tzinfo=timezone.utc)

    # ------------------------------------------------------------------ #
    # Calculate missed ingestion attempts (if any)
    # ------------------------------------------------------------------ #
    ingested_attempts: list[datetime] = []
    
    if last_ingested is not None:
        # Make timezone-aware if needed
        if last_ingested.tzinfo is None:
            last_ingested = last_ingested.replace(tzinfo=timezone.utc)
        
        # Find all cron runs between last_ingested and now
        reference = last_ingested
        while True:
            cron_iter = croniter(cron, reference)
            next_run = cron_iter.get_next(datetime)
            if next_run.tzinfo is None:
                next_run = next_run.replace(tzinfo=timezone.utc)
            
            # Stop if we've reached or passed now
            if next_run >= now:
                break
            
            ingested_attempts.append(next_run)
            reference = next_run

    # ------------------------------------------------------------------ #
    # Determine status
    # ------------------------------------------------------------------ #
    if last_ingested is None:
        # Never ingested
        status = Status.MISSING
        message = f"No schedule has ever been ingested. Next expected by {next_ingestion_attempt.strftime('%Y-%m-%dT%H:%M:%SZ')}."
    elif ingested_attempts:
        # There are missed attempts
        status = Status.LATE
        missed_count = len(ingested_attempts)
        earliest_missed = ingested_attempts[0].strftime("%Y-%m-%dT%H:%M:%SZ")
        message = f"Ingestion is late. {missed_count} missed attempt(s) since last ingestion. Earliest missed: {earliest_missed}. Next attempt expected: {next_ingestion_attempt.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    else:
        # All cron runs have been ingested
        status = Status.OK
        message = f"Schedule is up-to-date. Next expected by {next_ingestion_attempt.strftime('%Y-%m-%dT%H:%M:%SZ')}."

    return CadenceResult(
        telescope_name=telescope_name,
        telescope_short_name=short_name,
        telescope_id=telescope_id,
        schedule_status=schedule_status,
        cron=cron,
        last_ingested=last_ingested,
        ingested_attempts=ingested_attempts,
        next_ingestion_attempt=next_ingestion_attempt,
        status=status,
        message=message,
    )


_RESULT_COLUMNS = [
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


def check_telescope_ingestion_status(
    client: Client | None = None,
    now: datetime | None = None,
) -> pd.DataFrame:
    """Fetch all telescopes and evaluate each cadence against recent schedules.

    Performs exactly **two** API calls:

    1. ``client.telescope.get_many()`` — retrieves all telescopes with their
       cadence configurations.
    2. ``client.schedule.get_many(telescope_ids=[...])`` — retrieves the most
       recent schedule per date-range and status for every telescope that has
       at least one cadence.

    The returned schedules are parsed in-memory to find the most recently
    created schedule per (telescope, status) pair before cadence evaluation.

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
    pd.DataFrame
        One row per (telescope, cadence) pair with columns:
        ``telescope_name``, ``telescope_id``, ``schedule_status``, ``cron``,
        ``last_ingested``, ``ingested_attempts``, ``next_ingestion_attempt``,
        ``status``, ``message``.
    """
    if client is None:
        client = Client()

    if now is None:
        now = _now_utc()

    # ------------------------------------------------------------------ #
    # 1. Fetch all telescopes
    # ------------------------------------------------------------------ #
    telescopes = client.telescope.get_many()

    # ------------------------------------------------------------------ #
    # 2. Single bulk schedule query for all telescopes that have cadences
    # ------------------------------------------------------------------ #
    telescope_ids_with_cadences = [
        t.id for t in telescopes if t.schedule_cadences
    ]
    all_schedules = _fetch_all_schedules(client, telescope_ids_with_cadences)

    # ------------------------------------------------------------------ #
    # 3. Build (telescope_id, status) → most-recent schedule lookup
    # ------------------------------------------------------------------ #
    latest_by_key = _build_latest_lookup(all_schedules)

    # ------------------------------------------------------------------ #
    # 4. Evaluate each telescope's cadences
    # ------------------------------------------------------------------ #
    rows: list[CadenceResult] = []

    for telescope in telescopes:
        cadences = telescope.schedule_cadences or []

        if not cadences:
            rows.append(
                CadenceResult(
                    telescope_name=telescope.name,
                    telescope_short_name=telescope.short_name,
                    telescope_id=telescope.id,
                    schedule_status="",
                    cron=None,
                    last_ingested=None,
                    ingested_attempts=[],
                    next_ingestion_attempt=None,
                    status=Status.NO_CADENCE,
                    message="Telescope has no schedule cadences configured.",
                )
            )
            continue

        for cadence in cadences:
            status_val = _status_value(cadence.schedule_status)
            latest = latest_by_key.get((telescope.id, status_val))
            last_ingested = latest.created_on if latest is not None else None
            result = check_cadence(
                telescope_name=telescope.name,
                telescope_id=telescope.id,
                cron=cadence.cron,
                schedule_status=status_val,
                last_ingested=last_ingested,
                now=now,
                telescope_short_name=telescope.short_name,
            )
            rows.append(result)
            logger.debug("Checked %s: %s", telescope.name, result)

    if not rows:
        return pd.DataFrame(columns=_RESULT_COLUMNS)

    df = pd.DataFrame(
        [
            {
                **{k: v for k, v in dataclasses.asdict(r).items() if k != "status"},
                "status": r.status.value,
            }
            for r in rows
        ]
    )
    return df
