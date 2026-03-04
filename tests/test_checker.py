"""Tests for across_qa.checker module."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from across_qa.checker import (
    CadenceResult,
    Status,
    check_all_telescopes,
    check_cadence,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc(dt: datetime) -> datetime:
    """Return *dt* with UTC timezone attached (if not already set)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


NOW = _utc(datetime(2024, 6, 1, 11, 45, 0))
# A timestamp meaning "schedule was ingested 30 minutes ago"
LAST_30MIN_AGO = NOW - timedelta(minutes=30)
# A timestamp meaning "schedule was ingested 2 hours ago"
LAST_2H_AGO = NOW - timedelta(hours=2)


# ---------------------------------------------------------------------------
# check_cadence unit tests
# ---------------------------------------------------------------------------

class TestCheckCadence:
    """Unit tests for :func:`check_cadence`."""

    def test_ok_when_within_cron_window(self):
        """If the next expected time is in the future, status should be OK."""
        # Cron: every hour.  Last ingested 30 min ago → next is 30 min from now.
        result = check_cadence(
            telescope_name="TestScope",
            telescope_id="abc-123",
            cron="0 * * * *",
            schedule_status="planned",
            last_ingested=LAST_30MIN_AGO,
            now=NOW,
        )
        assert result.status == Status.OK
        assert result.next_expected is not None
        assert result.next_expected > NOW

    def test_late_when_past_next_expected(self):
        """If the next expected time has passed, status should be LATE."""
        # Cron: every hour.  Last ingested 2 h ago → next was 1 h ago.
        result = check_cadence(
            telescope_name="TestScope",
            telescope_id="abc-123",
            cron="0 * * * *",
            schedule_status="planned",
            last_ingested=LAST_2H_AGO,
            now=NOW,
        )
        assert result.status == Status.LATE
        assert result.next_expected is not None
        assert result.next_expected < NOW

    def test_missing_when_no_schedule_ingested(self):
        """If no schedule has been ingested, status should be MISSING."""
        result = check_cadence(
            telescope_name="TestScope",
            telescope_id="abc-123",
            cron="0 * * * *",
            schedule_status="planned",
            last_ingested=None,
            now=NOW,
        )
        assert result.status == Status.MISSING
        assert result.last_ingested is None

    def test_no_cadence_when_cron_is_none(self):
        """When no cron is provided, status should be NO_CADENCE."""
        result = check_cadence(
            telescope_name="TestScope",
            telescope_id="abc-123",
            cron=None,
            schedule_status="planned",
            last_ingested=LAST_30MIN_AGO,
            now=NOW,
        )
        assert result.status == Status.NO_CADENCE

    def test_no_cadence_when_cron_is_empty_string(self):
        """An empty cron string is treated as missing cadence."""
        result = check_cadence(
            telescope_name="TestScope",
            telescope_id="abc-123",
            cron="",
            schedule_status="planned",
            last_ingested=LAST_30MIN_AGO,
            now=NOW,
        )
        assert result.status == Status.NO_CADENCE

    def test_no_cadence_on_invalid_cron(self):
        """An unparseable cron string returns NO_CADENCE with an error message."""
        result = check_cadence(
            telescope_name="TestScope",
            telescope_id="abc-123",
            cron="not-a-cron",
            schedule_status="planned",
            last_ingested=LAST_30MIN_AGO,
            now=NOW,
        )
        assert result.status == Status.NO_CADENCE
        assert "parse" in result.message.lower() or "cron" in result.message.lower()

    def test_result_fields_populated(self):
        """CadenceResult fields should all be set correctly."""
        result = check_cadence(
            telescope_name="MyTelescope",
            telescope_id="tid-999",
            cron="*/5 * * * *",
            schedule_status="performed",
            last_ingested=LAST_30MIN_AGO,
            now=NOW,
        )
        assert result.telescope_name == "MyTelescope"
        assert result.telescope_id == "tid-999"
        assert result.schedule_status == "performed"
        assert result.cron == "*/5 * * * *"
        assert result.last_ingested == LAST_30MIN_AGO

    def test_naive_last_ingested_treated_as_utc(self):
        """A naive (tz-unaware) last_ingested datetime should not raise."""
        naive_dt = datetime(2024, 6, 1, 11, 30, 0)  # no tz
        result = check_cadence(
            telescope_name="TestScope",
            telescope_id="abc-123",
            cron="0 * * * *",
            schedule_status="planned",
            last_ingested=naive_dt,
            now=NOW,
        )
        # Should return a valid result without raising
        assert result.status in Status.__members__.values()

    def test_str_representation(self):
        """CadenceResult.__str__ should include key information."""
        result = check_cadence(
            telescope_name="TestScope",
            telescope_id="abc-123",
            cron="0 * * * *",
            schedule_status="planned",
            last_ingested=LAST_30MIN_AGO,
            now=NOW,
        )
        s = str(result)
        assert "TestScope" in s
        assert result.status.value in s


# ---------------------------------------------------------------------------
# check_all_telescopes integration-style tests (mocked)
# ---------------------------------------------------------------------------

def _make_cadence(cron: str | None, schedule_status: str = "planned") -> MagicMock:
    cadence = MagicMock()
    cadence.cron = cron
    cadence.schedule_status = schedule_status
    return cadence


def _make_telescope(
    name: str,
    tid: str,
    cadences: list | None = None,
) -> MagicMock:
    tele = MagicMock()
    tele.name = name
    tele.id = tid
    tele.schedule_cadences = cadences or []
    return tele


def _make_schedule(created_on: datetime) -> MagicMock:
    sched = MagicMock()
    sched.created_on = created_on
    return sched


def _make_page(schedules: list) -> MagicMock:
    page = MagicMock()
    page.items = schedules
    return page


class TestCheckAllTelescopes:
    """Tests for :func:`check_all_telescopes` using a mocked Client."""

    def _make_client(
        self,
        telescopes: list,
        schedule_map: dict | None = None,
    ) -> MagicMock:
        """Build a mock Client.

        Parameters
        ----------
        telescopes:
            List of mock telescope objects.
        schedule_map:
            Dict mapping ``(telescope_id, status)`` → ``list[mock_schedule]``.
        """
        client = MagicMock()
        client.telescope.get_many.return_value = telescopes

        schedule_map = schedule_map or {}

        def _get_many(telescope_ids=None, status=None, page=None, page_limit=None):
            status_val = status.value if hasattr(status, "value") else str(status)
            key = (telescope_ids[0] if telescope_ids else None, status_val)
            items = schedule_map.get(key, [])
            return _make_page(items)

        client.schedule.get_many.side_effect = _get_many
        return client

    def test_ok_telescope(self):
        """A telescope with a recent ingestion within cron window returns OK."""
        cadence = _make_cadence("0 * * * *", "planned")
        telescope = _make_telescope("Swift", "t1", [cadence])
        recent_schedule = _make_schedule(LAST_30MIN_AGO)
        client = self._make_client(
            telescopes=[telescope],
            schedule_map={("t1", "planned"): [recent_schedule]},
        )

        results = check_all_telescopes(client=client, now=NOW)

        assert len(results) == 1
        assert results[0].status == Status.OK
        assert results[0].telescope_name == "Swift"

    def test_late_telescope(self):
        """A telescope whose last schedule is older than the cron interval returns LATE."""
        cadence = _make_cadence("0 * * * *", "planned")
        telescope = _make_telescope("Chandra", "t2", [cadence])
        old_schedule = _make_schedule(LAST_2H_AGO)
        client = self._make_client(
            telescopes=[telescope],
            schedule_map={("t2", "planned"): [old_schedule]},
        )

        results = check_all_telescopes(client=client, now=NOW)

        assert len(results) == 1
        assert results[0].status == Status.LATE

    def test_missing_schedule(self):
        """A telescope with no schedules at all returns MISSING."""
        cadence = _make_cadence("0 * * * *", "planned")
        telescope = _make_telescope("Fermi", "t3", [cadence])
        client = self._make_client(
            telescopes=[telescope],
            schedule_map={},
        )

        results = check_all_telescopes(client=client, now=NOW)

        assert len(results) == 1
        assert results[0].status == Status.MISSING

    def test_no_cadence_telescope(self):
        """A telescope with no cadence entries returns NO_CADENCE."""
        telescope = _make_telescope("Hubble", "t4", cadences=[])
        client = self._make_client(telescopes=[telescope])

        results = check_all_telescopes(client=client, now=NOW)

        assert len(results) == 1
        assert results[0].status == Status.NO_CADENCE

    def test_multiple_cadences(self):
        """A telescope with multiple cadences returns one result per cadence."""
        cadence_planned = _make_cadence("0 * * * *", "planned")
        cadence_performed = _make_cadence("0 0 * * *", "performed")
        telescope = _make_telescope("XMM", "t5", [cadence_planned, cadence_performed])
        recent = _make_schedule(LAST_30MIN_AGO)
        old = _make_schedule(LAST_2H_AGO)
        client = self._make_client(
            telescopes=[telescope],
            schedule_map={
                ("t5", "planned"): [recent],
                ("t5", "performed"): [old],
            },
        )

        results = check_all_telescopes(client=client, now=NOW)

        assert len(results) == 2
        statuses = {r.schedule_status: r.status for r in results}
        assert statuses["planned"] == Status.OK
        assert statuses["performed"] == Status.OK  # daily cron; 2h ago is fine

    def test_multiple_telescopes(self):
        """Results include one entry per (telescope, cadence) pair."""
        c1 = _make_cadence("0 * * * *", "planned")
        c2 = _make_cadence("0 * * * *", "planned")
        t1 = _make_telescope("Swift", "t1", [c1])
        t2 = _make_telescope("NuSTAR", "t2", [c2])
        client = self._make_client(
            telescopes=[t1, t2],
            schedule_map={
                ("t1", "planned"): [_make_schedule(LAST_30MIN_AGO)],
                ("t2", "planned"): [],
            },
        )

        results = check_all_telescopes(client=client, now=NOW)

        assert len(results) == 2
        result_map = {r.telescope_name: r for r in results}
        assert result_map["Swift"].status == Status.OK
        assert result_map["NuSTAR"].status == Status.MISSING

    def test_empty_telescope_list(self):
        """No telescopes → empty result list."""
        client = self._make_client(telescopes=[])

        results = check_all_telescopes(client=client, now=NOW)

        assert results == []

    def test_default_client_created(self):
        """When no client is supplied, a default Client() is instantiated."""
        with patch("across_qa.checker.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client.telescope.get_many.return_value = []
            mock_client_cls.return_value = mock_client

            results = check_all_telescopes(now=NOW)

            mock_client_cls.assert_called_once_with()
            assert results == []
