"""Transactional account-scoped storage for normalized daily wellness values."""

from __future__ import annotations

import math
import sqlite3
import unicodedata
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, fields
from datetime import date, datetime, timedelta
from pathlib import Path

from omarchy_garmin.database import GarminDatabase, GarminDatabaseError
from omarchy_garmin.wellness import (
    BodyBatteryDay,
    DailyWellness,
    HrvDay,
    RestingHeartRateDay,
    SleepDay,
    SleepLevel,
    SleepNap,
    SleepRangeDay,
    SleepStage,
    StepsDay,
    TrainingReadinessDay,
    UserSummaryDay,
    Vo2MaxDay,
    WellnessSource,
    WellnessWriteDay,
)

WELLNESS_RETENTION_DAYS = 30
MAX_WELLNESS_UPSERT_DAYS = 31


class WellnessDatabaseError(GarminDatabaseError):
    """Raised when stored wellness state is malformed or unavailable."""


class WellnessAccountMismatchError(WellnessDatabaseError):
    """Raised before wellness state can be mixed across account scopes."""


@dataclass(frozen=True, slots=True)
class WellnessUpsertResult:
    """Observable result of one source-specific wellness transaction."""

    stored_count: int
    deleted_count: int


@dataclass(frozen=True, slots=True)
class StoredSleepIntervals:
    """Level and nap rows stored for one calendar date."""

    levels: tuple[SleepLevel, ...]
    naps: tuple[SleepNap, ...]


@dataclass(frozen=True, slots=True)
class WellnessSourceFreshness:
    """Private successful-refresh timestamp for one wellness source."""

    source: WellnessSource
    refreshed_at: datetime


@dataclass(frozen=True, slots=True)
class WellnessCadenceState:
    """Private request-attempt metadata used to enforce wellness cadence."""

    historical_date: date | None = None
    full_reconciliation_date: date | None = None
    backfill_at: datetime | None = None
    current_steps_at: datetime | None = None
    current_body_battery_at: datetime | None = None
    current_sleep_at: datetime | None = None
    current_training_readiness_at: datetime | None = None
    current_vo2_max_at: datetime | None = None


_CADENCE_KEYS = {
    "wellness_historical_date": "historical_date",
    "wellness_full_reconciliation_date": "full_reconciliation_date",
    "wellness_backfill_at": "backfill_at",
    "wellness_current_steps_at": "current_steps_at",
    "wellness_current_body_battery_at": "current_body_battery_at",
    "wellness_current_sleep_at": "current_sleep_at",
    "wellness_current_training_readiness_at": "current_training_readiness_at",
    "wellness_current_vo2_max_at": "current_vo2_max_at",
}
_DATE_CADENCE_KEYS = {
    "wellness_historical_date",
    "wellness_full_reconciliation_date",
}


_UPSERT_WELLNESS = """
INSERT INTO wellness_daily (
    account_fingerprint,
    calendar_date,
    steps,
    step_goal,
    body_battery_charged,
    body_battery_drained,
    body_battery_lowest,
    body_battery_highest,
    body_battery_latest,
    sleep_score,
    sleep_total_seconds,
    sleep_deep_seconds,
    sleep_light_seconds,
    sleep_rem_seconds,
    sleep_awake_seconds,
    training_readiness_score,
    training_readiness_level,
    hrv_weekly_average_ms,
    hrv_last_night_average_ms,
    hrv_status,
    hrv_balanced_low_ms,
    hrv_balanced_upper_ms,
    resting_heart_rate_bpm,
    vo2_max_running,
    vo2_max_cycling,
    sleep_start_local,
    sleep_end_local,
    nap_time_seconds
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)
ON CONFLICT(account_fingerprint, calendar_date) DO UPDATE SET
    steps = coalesce(excluded.steps, wellness_daily.steps),
    step_goal = coalesce(excluded.step_goal, wellness_daily.step_goal),
    body_battery_charged = coalesce(
        excluded.body_battery_charged, wellness_daily.body_battery_charged
    ),
    body_battery_drained = coalesce(
        excluded.body_battery_drained, wellness_daily.body_battery_drained
    ),
    body_battery_lowest = coalesce(
        excluded.body_battery_lowest, wellness_daily.body_battery_lowest
    ),
    body_battery_highest = coalesce(
        excluded.body_battery_highest, wellness_daily.body_battery_highest
    ),
    body_battery_latest = coalesce(
        excluded.body_battery_latest, wellness_daily.body_battery_latest
    ),
    sleep_score = coalesce(excluded.sleep_score, wellness_daily.sleep_score),
    sleep_total_seconds = coalesce(
        excluded.sleep_total_seconds, wellness_daily.sleep_total_seconds
    ),
    sleep_deep_seconds = coalesce(
        excluded.sleep_deep_seconds, wellness_daily.sleep_deep_seconds
    ),
    sleep_light_seconds = coalesce(
        excluded.sleep_light_seconds, wellness_daily.sleep_light_seconds
    ),
    sleep_rem_seconds = coalesce(excluded.sleep_rem_seconds, wellness_daily.sleep_rem_seconds),
    sleep_awake_seconds = coalesce(
        excluded.sleep_awake_seconds, wellness_daily.sleep_awake_seconds
    ),
    training_readiness_score = coalesce(
        excluded.training_readiness_score, wellness_daily.training_readiness_score
    ),
    training_readiness_level = coalesce(
        excluded.training_readiness_level, wellness_daily.training_readiness_level
    ),
    hrv_weekly_average_ms = coalesce(
        excluded.hrv_weekly_average_ms, wellness_daily.hrv_weekly_average_ms
    ),
    hrv_last_night_average_ms = coalesce(
        excluded.hrv_last_night_average_ms, wellness_daily.hrv_last_night_average_ms
    ),
    hrv_status = coalesce(excluded.hrv_status, wellness_daily.hrv_status),
    hrv_balanced_low_ms = coalesce(
        excluded.hrv_balanced_low_ms, wellness_daily.hrv_balanced_low_ms
    ),
    hrv_balanced_upper_ms = coalesce(
        excluded.hrv_balanced_upper_ms, wellness_daily.hrv_balanced_upper_ms
    ),
    resting_heart_rate_bpm = coalesce(
        excluded.resting_heart_rate_bpm, wellness_daily.resting_heart_rate_bpm
    ),
    vo2_max_running = coalesce(excluded.vo2_max_running, wellness_daily.vo2_max_running),
    vo2_max_cycling = coalesce(excluded.vo2_max_cycling, wellness_daily.vo2_max_cycling),
    sleep_start_local = coalesce(excluded.sleep_start_local, wellness_daily.sleep_start_local),
    sleep_end_local = coalesce(excluded.sleep_end_local, wellness_daily.sleep_end_local),
    nap_time_seconds = coalesce(excluded.nap_time_seconds, wellness_daily.nap_time_seconds)
"""

_DELETE_SLEEP_LEVELS = """
DELETE FROM wellness_sleep_levels
WHERE account_fingerprint = ? AND calendar_date = ?
"""
_DELETE_SLEEP_NAPS = """
DELETE FROM wellness_sleep_naps
WHERE account_fingerprint = ? AND calendar_date = ?
"""
_INSERT_SLEEP_LEVEL = """
INSERT INTO wellness_sleep_levels (
    account_fingerprint, calendar_date, seq, stage, start_utc, end_utc
) VALUES (?, ?, ?, ?, ?, ?)
"""
_INSERT_SLEEP_NAP = """
INSERT INTO wellness_sleep_naps (
    account_fingerprint,
    calendar_date,
    seq,
    nap_seconds,
    start_utc,
    end_utc,
    start_local,
    end_local
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""


def _validate_fingerprint(account_fingerprint: str) -> None:
    if (
        not isinstance(account_fingerprint, str)
        or len(account_fingerprint) != 64
        or any(character not in "0123456789abcdef" for character in account_fingerprint)
    ):
        raise ValueError("account fingerprint is invalid")


def _validate_timestamp(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("refreshed_at must include a timezone")
    result = value.isoformat(timespec="seconds")
    if len(result) > 40:
        raise ValueError("refreshed_at is invalid")
    return result


def _optional_integer(value: object, *, minimum: int, maximum: int) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError("wellness integer is invalid")
    return value


def _optional_real(value: object) -> float | None:
    return _optional_bounded_real(value, minimum=0, maximum=1_000)


def _optional_vo2(value: object) -> float | None:
    return _optional_bounded_real(value, minimum=10, maximum=100)


def _optional_bounded_real(value: object, *, minimum: float, maximum: float) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError("wellness number is invalid")
    try:
        result = float(value)
    except OverflowError as error:
        raise ValueError("wellness number is invalid") from error
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise ValueError("wellness number is invalid")
    return result


def _required_utc_clock(value: object) -> str:
    if not isinstance(value, str) or not 20 <= len(value) <= 40 or not value.endswith("Z"):
        raise ValueError("sleep utc timestamp is invalid")
    if any(unicodedata.category(character).startswith("C") for character in value):
        raise ValueError("sleep utc timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(value[:-1])
    except ValueError as error:
        raise ValueError("sleep utc timestamp is invalid") from error
    if parsed.tzinfo is not None or f"{parsed.isoformat(timespec='seconds')}Z" != value:
        raise ValueError("sleep utc timestamp is invalid")
    return value


def _validate_level_clocks(levels: tuple[SleepLevel, ...]) -> None:
    if len(levels) > 200:
        raise ValueError("sleep levels are invalid")
    previous_end: str | None = None
    for level in levels:
        if not isinstance(level, SleepLevel) or not isinstance(level.stage, SleepStage):
            raise ValueError("sleep levels are invalid")
        start = _required_utc_clock(level.start_utc)
        end = _required_utc_clock(level.end_utc)
        if start >= end or (previous_end is not None and start != previous_end):
            raise ValueError("sleep levels are invalid")
        previous_end = end


def _validate_naps(naps: tuple[SleepNap, ...]) -> None:
    if len(naps) > 8:
        raise ValueError("sleep naps are invalid")
    for nap in naps:
        if not isinstance(nap, SleepNap):
            raise ValueError("sleep naps are invalid")
        _optional_integer(nap.nap_seconds, minimum=0, maximum=86_400)
        start = _required_utc_clock(nap.start_utc)
        end = _required_utc_clock(nap.end_utc)
        if start >= end:
            raise ValueError("sleep naps are invalid")
        _optional_local_clock(nap.start_local)
        _optional_local_clock(nap.end_local)


def _validate_sleep_children(day: SleepDay) -> None:
    if day.levels is not None:
        _validate_level_clocks(day.levels)
    if day.naps is not None:
        _validate_naps(day.naps)


def _level_from_row(row: Sequence[object]) -> SleepLevel:
    if len(row) != 4 or type(row[0]) is not int or not isinstance(row[1], str):
        raise ValueError("stored sleep level has an unexpected shape")
    if not 0 <= row[0] <= 199:
        raise ValueError("stored sleep level has an unexpected shape")
    return SleepLevel(SleepStage(row[1]), _required_utc_clock(row[2]), _required_utc_clock(row[3]))


def _nap_from_row(row: Sequence[object]) -> SleepNap:
    if len(row) != 6 or type(row[0]) is not int or not 0 <= row[0] <= 7:
        raise ValueError("stored sleep nap has an unexpected shape")
    nap_seconds = _optional_integer(row[1], minimum=0, maximum=86_400)
    start_local = _optional_local_clock(row[4])
    end_local = _optional_local_clock(row[5])
    if nap_seconds is None or start_local is None or end_local is None:
        raise ValueError("stored sleep nap has an unexpected shape")
    return SleepNap(
        nap_seconds,
        _required_utc_clock(row[2]),
        _required_utc_clock(row[3]),
        start_local,
        end_local,
    )


def _optional_local_clock(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not 19 <= len(value) <= 40:
        raise ValueError("wellness local timestamp is invalid")
    if any(unicodedata.category(character).startswith("C") for character in value):
        raise ValueError("wellness local timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError("wellness local timestamp is invalid") from error
    if parsed.tzinfo is not None or parsed.isoformat(timespec="seconds") != value:
        raise ValueError("wellness local timestamp is invalid")
    return value


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 64
        or any(unicodedata.category(character).startswith("C") for character in value)
    ):
        raise ValueError("wellness text is invalid")
    return value


def _require_date(value: object) -> date:
    if type(value) is not date:
        raise ValueError("wellness date is invalid")
    return value


def _daily_for_source(source: WellnessSource, day: WellnessWriteDay) -> DailyWellness:
    calendar_date = _require_date(getattr(day, "calendar_date", None))
    if source is WellnessSource.USER_SUMMARY and isinstance(day, UserSummaryDay):
        return DailyWellness(
            calendar_date=calendar_date,
            steps=_optional_integer(day.steps, minimum=0, maximum=1_000_000),
            step_goal=_optional_integer(day.step_goal, minimum=0, maximum=1_000_000),
            resting_heart_rate_bpm=_optional_integer(
                day.resting_heart_rate_bpm, minimum=20, maximum=300
            ),
        )
    if source is WellnessSource.STEPS and isinstance(day, StepsDay):
        return DailyWellness(
            calendar_date=calendar_date,
            steps=_optional_integer(day.steps, minimum=0, maximum=1_000_000),
        )
    if source is WellnessSource.BODY_BATTERY and isinstance(day, BodyBatteryDay):
        lowest = _optional_integer(day.lowest, minimum=0, maximum=100)
        highest = _optional_integer(day.highest, minimum=0, maximum=100)
        if lowest is not None and highest is not None and lowest > highest:
            raise ValueError("Body Battery range is invalid")
        return DailyWellness(
            calendar_date=calendar_date,
            body_battery_charged=_optional_integer(day.charged, minimum=0, maximum=1_000),
            body_battery_drained=_optional_integer(day.drained, minimum=0, maximum=1_000),
            body_battery_lowest=lowest,
            body_battery_highest=highest,
            body_battery_latest=_optional_integer(day.latest, minimum=0, maximum=100),
        )
    if source is WellnessSource.SLEEP and isinstance(day, SleepRangeDay | SleepDay):
        if isinstance(day, SleepRangeDay):
            return DailyWellness(
                calendar_date=calendar_date,
                sleep_score=_optional_integer(day.score, minimum=0, maximum=100),
            )
        sleep_parts = (
            _optional_integer(day.deep_seconds, minimum=0, maximum=86_400),
            _optional_integer(day.light_seconds, minimum=0, maximum=86_400),
            _optional_integer(day.rem_seconds, minimum=0, maximum=86_400),
            _optional_integer(day.awake_seconds, minimum=0, maximum=86_400),
        )
        if sum(part or 0 for part in sleep_parts) > 86_400:
            raise ValueError("Sleep composition is invalid")
        return DailyWellness(
            calendar_date=calendar_date,
            sleep_score=_optional_integer(day.score, minimum=0, maximum=100),
            sleep_total_seconds=_optional_integer(day.total_seconds, minimum=0, maximum=86_400),
            sleep_deep_seconds=sleep_parts[0],
            sleep_light_seconds=sleep_parts[1],
            sleep_rem_seconds=sleep_parts[2],
            sleep_awake_seconds=sleep_parts[3],
            sleep_start_local=_optional_local_clock(day.start_local),
            sleep_end_local=_optional_local_clock(day.end_local),
            nap_time_seconds=_optional_integer(day.nap_time_seconds, minimum=0, maximum=86_400),
        )
    if source is WellnessSource.HRV and isinstance(day, HrvDay):
        balanced_low = _optional_real(day.balanced_low_ms)
        balanced_upper = _optional_real(day.balanced_upper_ms)
        if (
            balanced_low is not None
            and balanced_upper is not None
            and balanced_low > balanced_upper
        ):
            raise ValueError("HRV baseline is invalid")
        return DailyWellness(
            calendar_date=calendar_date,
            hrv_weekly_average_ms=_optional_real(day.weekly_average_ms),
            hrv_last_night_average_ms=_optional_real(day.last_night_average_ms),
            hrv_status=_optional_text(day.status),
            hrv_balanced_low_ms=balanced_low,
            hrv_balanced_upper_ms=balanced_upper,
        )
    if source is WellnessSource.RESTING_HEART_RATE and isinstance(day, RestingHeartRateDay):
        return DailyWellness(
            calendar_date=calendar_date,
            resting_heart_rate_bpm=_optional_integer(day.beats_per_minute, minimum=20, maximum=300),
        )
    metric = _daily_for_metric(source, day, calendar_date)
    if metric is not None:
        return metric
    raise ValueError("wellness source does not match the supplied domain values")


def _daily_for_metric(
    source: WellnessSource,
    day: WellnessWriteDay,
    calendar_date: date,
) -> DailyWellness | None:
    if source is WellnessSource.TRAINING_READINESS and isinstance(day, TrainingReadinessDay):
        return DailyWellness(
            calendar_date=calendar_date,
            training_readiness_score=_optional_integer(day.score, minimum=0, maximum=100),
            training_readiness_level=_optional_text(day.level),
        )
    if source is WellnessSource.VO2_MAX and isinstance(day, Vo2MaxDay):
        return DailyWellness(
            calendar_date=calendar_date,
            vo2_max_running=_optional_vo2(day.running),
            vo2_max_cycling=_optional_vo2(day.cycling),
        )
    return None


def _validated_daily(day: DailyWellness) -> DailyWellness:
    """Revalidate a stored row through the same reviewed scalar bounds."""
    calendar_date = _require_date(day.calendar_date)
    sleep_parts = (
        _optional_integer(day.sleep_deep_seconds, minimum=0, maximum=86_400),
        _optional_integer(day.sleep_light_seconds, minimum=0, maximum=86_400),
        _optional_integer(day.sleep_rem_seconds, minimum=0, maximum=86_400),
        _optional_integer(day.sleep_awake_seconds, minimum=0, maximum=86_400),
    )
    if sum(part or 0 for part in sleep_parts) > 86_400:
        raise ValueError("Sleep composition is invalid")
    lowest = _optional_integer(day.body_battery_lowest, minimum=0, maximum=100)
    highest = _optional_integer(day.body_battery_highest, minimum=0, maximum=100)
    if lowest is not None and highest is not None and lowest > highest:
        raise ValueError("Body Battery range is invalid")
    baseline_low = _optional_real(day.hrv_balanced_low_ms)
    baseline_upper = _optional_real(day.hrv_balanced_upper_ms)
    if baseline_low is not None and baseline_upper is not None and baseline_low > baseline_upper:
        raise ValueError("HRV baseline is invalid")
    return DailyWellness(
        calendar_date=calendar_date,
        steps=_optional_integer(day.steps, minimum=0, maximum=1_000_000),
        step_goal=_optional_integer(day.step_goal, minimum=0, maximum=1_000_000),
        body_battery_charged=_optional_integer(day.body_battery_charged, minimum=0, maximum=1_000),
        body_battery_drained=_optional_integer(day.body_battery_drained, minimum=0, maximum=1_000),
        body_battery_lowest=lowest,
        body_battery_highest=highest,
        body_battery_latest=_optional_integer(day.body_battery_latest, minimum=0, maximum=100),
        sleep_score=_optional_integer(day.sleep_score, minimum=0, maximum=100),
        sleep_total_seconds=_optional_integer(day.sleep_total_seconds, minimum=0, maximum=86_400),
        sleep_deep_seconds=sleep_parts[0],
        sleep_light_seconds=sleep_parts[1],
        sleep_rem_seconds=sleep_parts[2],
        sleep_awake_seconds=sleep_parts[3],
        training_readiness_score=_optional_integer(
            day.training_readiness_score, minimum=0, maximum=100
        ),
        training_readiness_level=_optional_text(day.training_readiness_level),
        hrv_weekly_average_ms=_optional_real(day.hrv_weekly_average_ms),
        hrv_last_night_average_ms=_optional_real(day.hrv_last_night_average_ms),
        hrv_status=_optional_text(day.hrv_status),
        hrv_balanced_low_ms=baseline_low,
        hrv_balanced_upper_ms=baseline_upper,
        resting_heart_rate_bpm=_optional_integer(
            day.resting_heart_rate_bpm, minimum=20, maximum=300
        ),
        vo2_max_running=_optional_vo2(day.vo2_max_running),
        vo2_max_cycling=_optional_vo2(day.vo2_max_cycling),
        sleep_start_local=_optional_local_clock(day.sleep_start_local),
        sleep_end_local=_optional_local_clock(day.sleep_end_local),
        nap_time_seconds=_optional_integer(day.nap_time_seconds, minimum=0, maximum=86_400),
    )


class WellnessRepository:
    """Store daily wellness values and private source state for one account scope."""

    def __init__(self, database_path: Path, account_fingerprint: str) -> None:
        """Initialize the repository with a validated pseudonymous account scope."""
        _validate_fingerprint(account_fingerprint)
        self._database = GarminDatabase(database_path)
        self._account_fingerprint = account_fingerprint

    def upsert_source(
        self,
        source: WellnessSource,
        days: Sequence[WellnessWriteDay],
        *,
        as_of_date: date,
        refreshed_at: datetime,
    ) -> WellnessUpsertResult:
        """Upsert one successful source and enforce rolling 30-day retention atomically."""
        if not isinstance(source, WellnessSource):
            raise ValueError("wellness source is invalid")
        if type(as_of_date) is not date:
            raise ValueError("as_of_date is invalid")
        if len(days) > MAX_WELLNESS_UPSERT_DAYS:
            raise ValueError("wellness source contains too many days")
        refreshed_at_text = _validate_timestamp(refreshed_at)
        normalized = tuple(_daily_for_source(source, day) for day in days)
        dates = {day.calendar_date for day in normalized}
        if len(dates) != len(normalized):
            raise ValueError("wellness source contains duplicate dates")
        retention_start = as_of_date - timedelta(days=WELLNESS_RETENTION_DAYS - 1)
        if any(not retention_start <= day.calendar_date <= as_of_date for day in normalized):
            raise ValueError("wellness date is outside the retention period")
        for day in days:
            if isinstance(day, SleepDay):
                _validate_sleep_children(day)

        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._bind_account(connection)
                connection.executemany(
                    _UPSERT_WELLNESS,
                    (self._write_row(day) for day in normalized),
                )
                self._replace_sleep_children(connection, days)
                deleted = connection.execute(
                    """
                    DELETE FROM wellness_daily
                    WHERE account_fingerprint = ?
                      AND (calendar_date < ? OR calendar_date > ?)
                    """,
                    (
                        self._account_fingerprint,
                        retention_start.isoformat(),
                        as_of_date.isoformat(),
                    ),
                ).rowcount
                connection.execute(
                    """
                    INSERT INTO wellness_source_state (account_fingerprint, source, refreshed_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(account_fingerprint, source) DO UPDATE SET
                        refreshed_at = excluded.refreshed_at
                    """,
                    (self._account_fingerprint, source.value, refreshed_at_text),
                )
                connection.execute("COMMIT")
            finally:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
        return WellnessUpsertResult(stored_count=len(normalized), deleted_count=deleted)

    def wellness_between(self, start_date: date, end_date: date) -> list[DailyWellness]:
        """Return validated stored days for an inclusive bounded date period."""
        if type(start_date) is not date or type(end_date) is not date or start_date > end_date:
            raise ValueError("wellness date period is invalid")
        if (end_date - start_date).days >= WELLNESS_RETENTION_DAYS:
            raise ValueError("wellness date period exceeds retention")
        with self._read_connection() as connection:
            if connection is None or not self._account_matches(connection):
                return []
            rows = connection.execute(
                """
                SELECT
                    calendar_date,
                    steps,
                    step_goal,
                    body_battery_charged,
                    body_battery_drained,
                    body_battery_lowest,
                    body_battery_highest,
                    body_battery_latest,
                    sleep_score,
                    sleep_total_seconds,
                    sleep_deep_seconds,
                    sleep_light_seconds,
                    sleep_rem_seconds,
                    sleep_awake_seconds,
                    training_readiness_score,
                    training_readiness_level,
                    hrv_weekly_average_ms,
                    hrv_last_night_average_ms,
                    hrv_status,
                    hrv_balanced_low_ms,
                    hrv_balanced_upper_ms,
                    resting_heart_rate_bpm,
                    vo2_max_running,
                    vo2_max_cycling,
                    sleep_start_local,
                    sleep_end_local,
                    nap_time_seconds
                FROM wellness_daily
                WHERE account_fingerprint = ? AND calendar_date BETWEEN ? AND ?
                ORDER BY calendar_date
                """,
                (
                    self._account_fingerprint,
                    start_date.isoformat(),
                    end_date.isoformat(),
                ),
            ).fetchall()
        try:
            return [self._read_row(row) for row in rows]
        except (TypeError, ValueError) as error:
            raise WellnessDatabaseError("stored wellness data is invalid") from error

    def sleep_intervals(self, calendar_date: date) -> StoredSleepIntervals:
        """Return stored stage intervals and naps for one retained date."""
        if type(calendar_date) is not date:
            raise ValueError("wellness date is invalid")
        with self._read_connection() as connection:
            if connection is None or not self._account_matches(connection):
                return StoredSleepIntervals((), ())
            level_rows = connection.execute(
                """
                SELECT seq, stage, start_utc, end_utc
                FROM wellness_sleep_levels
                WHERE account_fingerprint = ? AND calendar_date = ?
                ORDER BY seq
                """,
                (self._account_fingerprint, calendar_date.isoformat()),
            ).fetchall()
            nap_rows = connection.execute(
                """
                SELECT seq, nap_seconds, start_utc, end_utc, start_local, end_local
                FROM wellness_sleep_naps
                WHERE account_fingerprint = ? AND calendar_date = ?
                ORDER BY seq
                """,
                (self._account_fingerprint, calendar_date.isoformat()),
            ).fetchall()
        try:
            return StoredSleepIntervals(
                tuple(_level_from_row(row) for row in level_rows),
                tuple(_nap_from_row(row) for row in nap_rows),
            )
        except (TypeError, ValueError) as error:
            raise WellnessDatabaseError("stored sleep intervals are invalid") from error

    def dates_missing_sleep_levels(self, start_date: date, end_date: date) -> tuple[date, ...]:
        """Return newest-first dates that have sleep scalars but no accepted levels."""
        if type(start_date) is not date or type(end_date) is not date or start_date > end_date:
            raise ValueError("wellness date period is invalid")
        with self._read_connection() as connection:
            if connection is None or not self._account_matches(connection):
                return ()
            rows = connection.execute(
                """
                SELECT calendar_date
                FROM wellness_daily
                WHERE account_fingerprint = ?
                  AND calendar_date BETWEEN ? AND ?
                  AND sleep_levels_known = 0
                  AND (
                      sleep_score IS NOT NULL
                      OR sleep_total_seconds IS NOT NULL
                      OR sleep_deep_seconds IS NOT NULL
                      OR sleep_light_seconds IS NOT NULL
                      OR sleep_rem_seconds IS NOT NULL
                      OR sleep_awake_seconds IS NOT NULL
                      OR sleep_start_local IS NOT NULL
                      OR sleep_end_local IS NOT NULL
                  )
                ORDER BY calendar_date DESC
                """,
                (
                    self._account_fingerprint,
                    start_date.isoformat(),
                    end_date.isoformat(),
                ),
            ).fetchall()
        try:
            return tuple(date.fromisoformat(str(row[0])) for row in rows)
        except (TypeError, ValueError) as error:
            raise WellnessDatabaseError("stored sleep interval state is invalid") from error

    def source_freshness(self) -> list[WellnessSourceFreshness]:
        """Return validated private freshness timestamps for this account."""
        with self._read_connection() as connection:
            if connection is None or not self._account_matches(connection):
                return []
            rows = connection.execute(
                """
                SELECT source, refreshed_at
                FROM wellness_source_state
                WHERE account_fingerprint = ?
                ORDER BY source
                """,
                (self._account_fingerprint,),
            ).fetchall()
        try:
            return [self._freshness_from_row(row) for row in rows]
        except (TypeError, ValueError) as error:
            raise WellnessDatabaseError("stored wellness freshness is invalid") from error

    def collection_enabled(self) -> bool:
        """Return whether future wellness collection is enabled for this account."""
        with self._read_connection() as connection:
            if connection is None or not self._account_matches(connection):
                return True
            row = connection.execute(
                """
                SELECT enabled FROM wellness_collection_state WHERE account_fingerprint = ?
                """,
                (self._account_fingerprint,),
            ).fetchone()
        if row is None or len(row) != 1 or type(row[0]) is not int or row[0] not in {0, 1}:
            raise WellnessDatabaseError("stored wellness collection state is invalid")
        return bool(row[0])

    def cadence_state(self) -> WellnessCadenceState:
        """Return validated private request-attempt metadata for this account."""
        with self._read_connection() as connection:
            if connection is None or not self._account_matches(connection):
                return WellnessCadenceState()
            rows = connection.execute(
                """
                SELECT key, value FROM sync_state
                WHERE key IN (?, ?, ?, ?, ?, ?, ?, ?)
                ORDER BY key
                """,
                tuple(_CADENCE_KEYS),
            ).fetchall()
        values: dict[str, date | datetime | None] = {}
        try:
            for raw_key, raw_value in rows:
                if not isinstance(raw_key, str) or not isinstance(raw_value, str):
                    raise ValueError("cadence row has an unexpected shape")
                field_name = _CADENCE_KEYS[raw_key]
                if raw_key in _DATE_CADENCE_KEYS:
                    parsed_date = date.fromisoformat(raw_value)
                    if parsed_date.isoformat() != raw_value:
                        raise ValueError("cadence date is invalid")
                    values[field_name] = parsed_date
                else:
                    parsed_at = datetime.fromisoformat(raw_value)
                    _validate_timestamp(parsed_at)
                    values[field_name] = parsed_at
            return WellnessCadenceState(**values)  # type: ignore[arg-type]
        except (KeyError, TypeError, ValueError) as error:
            raise WellnessDatabaseError("stored wellness cadence is invalid") from error

    def reserve_cadence(
        self,
        *,
        today: date,
        attempted_at: datetime,
        historical: bool,
        full_reconciliation: bool,
        backfill: bool,
        current_steps: bool,
        current_body_battery: bool,
        current_sleep: bool,
        current_training_readiness: bool,
        current_vo2_max: bool,
    ) -> None:
        """Record planned request groups atomically before their first data call."""
        if type(today) is not date:
            raise ValueError("today is invalid")
        attempted_at_text = _validate_timestamp(attempted_at)
        updates = {
            "wellness_historical_date": today.isoformat() if historical else None,
            "wellness_full_reconciliation_date": (
                today.isoformat() if full_reconciliation else None
            ),
            "wellness_backfill_at": attempted_at_text if backfill else None,
            "wellness_current_steps_at": attempted_at_text if current_steps else None,
            "wellness_current_body_battery_at": (
                attempted_at_text if current_body_battery else None
            ),
            "wellness_current_sleep_at": attempted_at_text if current_sleep else None,
            "wellness_current_training_readiness_at": (
                attempted_at_text if current_training_readiness else None
            ),
            "wellness_current_vo2_max_at": attempted_at_text if current_vo2_max else None,
        }
        selected = tuple((key, value) for key, value in updates.items() if value is not None)
        if not selected:
            return
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._bind_account(connection)
                connection.executemany(
                    """
                    INSERT INTO sync_state (key, value) VALUES (?, ?)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value
                    """,
                    selected,
                )
                connection.execute("COMMIT")
            finally:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")

    def set_collection_enabled(self, enabled: bool) -> None:
        """Set collection state idempotently without deleting retained wellness values."""
        if not isinstance(enabled, bool):
            raise ValueError("enabled must be a boolean")
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._bind_account(connection)
                connection.execute(
                    """
                    INSERT INTO wellness_collection_state (account_fingerprint, enabled)
                    VALUES (?, ?)
                    ON CONFLICT(account_fingerprint) DO UPDATE SET enabled = excluded.enabled
                    """,
                    (self._account_fingerprint, int(enabled)),
                )
                connection.execute("COMMIT")
            finally:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        try:
            with self._database.connection() as connection:
                yield connection
        except WellnessDatabaseError:
            raise
        except GarminDatabaseError as error:
            raise WellnessDatabaseError("wellness database is unsafe or unavailable") from error

    @contextmanager
    def _read_connection(self) -> Iterator[sqlite3.Connection | None]:
        try:
            with self._database.read_connection() as connection:
                yield connection
        except WellnessDatabaseError:
            raise
        except GarminDatabaseError as error:
            raise WellnessDatabaseError("wellness database is unsafe or unavailable") from error

    def _account_matches(self, connection: sqlite3.Connection) -> bool:
        rows = connection.execute(
            "SELECT account_fingerprint FROM wellness_account_scope"
        ).fetchall()
        if not rows:
            return False
        if len(rows) != 1 or rows[0][0] != self._account_fingerprint:
            raise WellnessAccountMismatchError("wellness data belongs to a different account")
        return True

    def _bind_account(self, connection: sqlite3.Connection) -> None:
        rows = connection.execute(
            "SELECT account_fingerprint FROM wellness_account_scope"
        ).fetchall()
        if rows:
            if len(rows) != 1 or rows[0][0] != self._account_fingerprint:
                raise WellnessAccountMismatchError("wellness data belongs to a different account")
            return
        connection.execute(
            """
            INSERT INTO wellness_account_scope (singleton_id, account_fingerprint) VALUES (1, ?)
            """,
            (self._account_fingerprint,),
        )
        connection.execute(
            """
            INSERT INTO wellness_collection_state (account_fingerprint, enabled) VALUES (?, 1)
            """,
            (self._account_fingerprint,),
        )

    def _write_row(self, day: DailyWellness) -> tuple[object, ...]:
        return (
            self._account_fingerprint,
            day.calendar_date.isoformat(),
            *(getattr(day, field.name) for field in fields(day) if field.name != "calendar_date"),
        )

    def _replace_sleep_children(
        self,
        connection: sqlite3.Connection,
        days: Sequence[WellnessWriteDay],
    ) -> None:
        for day in days:
            if not isinstance(day, SleepDay):
                continue
            calendar_date = day.calendar_date.isoformat()
            if day.levels is not None:
                connection.execute(
                    _DELETE_SLEEP_LEVELS,
                    (self._account_fingerprint, calendar_date),
                )
                connection.executemany(
                    _INSERT_SLEEP_LEVEL,
                    (
                        (
                            self._account_fingerprint,
                            calendar_date,
                            seq,
                            level.stage.value,
                            level.start_utc,
                            level.end_utc,
                        )
                        for seq, level in enumerate(day.levels)
                    ),
                )
                connection.execute(
                    """
                    UPDATE wellness_daily
                    SET sleep_levels_known = 1
                    WHERE account_fingerprint = ? AND calendar_date = ?
                    """,
                    (self._account_fingerprint, calendar_date),
                )
            if day.naps is not None:
                connection.execute(
                    _DELETE_SLEEP_NAPS,
                    (self._account_fingerprint, calendar_date),
                )
                connection.executemany(
                    _INSERT_SLEEP_NAP,
                    (
                        (
                            self._account_fingerprint,
                            calendar_date,
                            seq,
                            nap.nap_seconds,
                            nap.start_utc,
                            nap.end_utc,
                            nap.start_local,
                            nap.end_local,
                        )
                        for seq, nap in enumerate(day.naps)
                    ),
                )

    @staticmethod
    def _read_row(row: Sequence[object]) -> DailyWellness:
        if len(row) != 27:
            raise ValueError("stored wellness row has an unexpected shape")
        calendar_date = date.fromisoformat(str(row[0]))
        return _validated_daily(DailyWellness(calendar_date, *row[1:]))  # type: ignore[arg-type]

    @staticmethod
    def _freshness_from_row(row: Sequence[object]) -> WellnessSourceFreshness:
        if len(row) != 2 or not isinstance(row[0], str) or not isinstance(row[1], str):
            raise ValueError("stored freshness row has an unexpected shape")
        refreshed_at = datetime.fromisoformat(row[1])
        _validate_timestamp(refreshed_at)
        return WellnessSourceFreshness(WellnessSource(row[0]), refreshed_at)
