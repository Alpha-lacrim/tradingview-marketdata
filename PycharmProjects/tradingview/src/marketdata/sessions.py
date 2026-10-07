"""Conservative interpretation of source-reported sessions; not an exchange calendar.

Unsupported syntax/correction dates are UNKNOWN. Never infer closure from absent bars.
"""

import re
from datetime import UTC, date, datetime, time, timedelta
from enum import StrEnum
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


class Expectation(StrEnum):
    EXPECTED = "EXPECTED"
    NOT_EXPECTED = "NOT_EXPECTED"
    UNKNOWN = "UNKNOWN"


def session_windows(info: dict[str, Any], day: date) -> list[tuple[datetime, datetime]] | None:
    """Return windows for a source trading day, or None if not understood."""
    try:
        zone = ZoneInfo(info["timezone"])
        session = info["session"]
        if session == "24x7":
            return [
                (
                    datetime.combine(day, time(), zone),
                    datetime.combine(day + timedelta(days=1), time(), zone),
                )
            ]
        # Documented history rule, restricted to default Monday-first weeks.
        # Other week origins/complex syntax remain UNKNOWN.
        if "#" in session:
            history = session.split("/")
            selected = history[0].split("#")[0]
            for index, part in enumerate(history[:-1]):
                if "#" not in part:
                    return None
                _, changed = part.rsplit("#", 1)
                switched = datetime.strptime(changed, "%Y%m%d").date()
                switched += timedelta(days=(-switched.weekday()) % 7)
                if day >= switched:
                    selected = history[index + 1].split("#")[0]
            session = selected
        if any(c in session for c in ";/"):
            return None
        corrections = str(info.get("corrections", ""))
        for sub in info.get("subsessions", []):
            if sub.get("id") == info.get("subsession_id"):
                corrections += ";" + str(sub.get("session-correction", ""))
        if day.strftime("%Y%m%d") in corrections:
            return None
        if day.strftime("%Y%m%d") in str(info.get("session_holidays", "")).split(","):
            return []
        windows = []
        parts = session.split("|")
        parsed = []
        explicit_days: set[str] = set()
        for part in parts:
            hours, _, days = part.partition(":")
            if days and not re.fullmatch(r"[1-7]+", days):
                return None
            explicit_days.update(days)
            parsed.append((hours, days))
        weekday = str((day.weekday() + 1) % 7 + 1)
        for hours, days in parsed:
            eligible = days or "".join(d for d in "23456" if d not in explicit_days)
            if weekday not in eligible:
                continue
            for period in hours.split(","):
                match = re.fullmatch(r"(\d{4})(F)?-(\d{4})", period)
                if not match:
                    return None
                a, previous, b = match.groups()
                ah, am, bh, bm = int(a[:2]), int(a[2:]), int(b[:2]), int(b[2:])
                if max(ah, bh) > 23 or max(am, bm) > 59:
                    return None
                overnight = bool(previous) or ((ah, am) >= (bh, bm) and a != "0000")
                opening = datetime.combine(day - timedelta(days=int(overnight)), time(ah, am), zone)
                closing = datetime.combine(
                    day + timedelta(days=int(a == b == "0000")), time(bh, bm), zone
                )
                if closing <= opening:
                    return None
                windows.append((opening, closing))
        return [(a.astimezone(UTC), b.astimezone(UTC)) for a, b in windows]
    except (AttributeError, KeyError, TypeError, ValueError, ZoneInfoNotFoundError):
        return None


def expectation(info: dict[str, Any], bucket: datetime) -> Expectation:
    if not info:
        return Expectation.UNKNOWN
    try:
        local_day = bucket.astimezone(ZoneInfo(info["timezone"])).date()
    except (KeyError, TypeError, ValueError, ZoneInfoNotFoundError):
        return Expectation.UNKNOWN
    local = bucket.astimezone(ZoneInfo(info["timezone"]))
    uncertain_break = str(info.get("uncertain_break") or "")
    if re.fullmatch(r"\d{4}-\d{4}", uncertain_break):
        a, b = uncertain_break.split("-")
        if a <= local.strftime("%H%M") < b and local.weekday() < 5:
            return Expectation.UNKNOWN
    unknown = False
    for day in (local_day, local_day + timedelta(days=1)):
        windows = session_windows(info, day)
        if windows is None:
            unknown = True
        elif any(a <= bucket and bucket + timedelta(minutes=1) <= b for a, b in windows):
            return Expectation.EXPECTED
    return Expectation.UNKNOWN if unknown else Expectation.NOT_EXPECTED


def classify_ranges(
    info: dict[str, Any], start: datetime, end: datetime
) -> dict[str, list[tuple[datetime, datetime]]]:
    """Compress minute classifications into ranges, keeping operational rows bounded.

    Called for newly checked ranges, not the entire history every minute.
    """
    result: dict[str, list[tuple[datetime, datetime]]] = {s.value: [] for s in Expectation}
    if start >= end:
        return result
    try:
        zone = ZoneInfo(info["timezone"])
    except (KeyError, TypeError, ValueError, ZoneInfoNotFoundError):
        result["UNKNOWN"] = [(start, end)]
        return result
    # Evaluate only schedule boundaries, not every historical minute. This keeps
    # long outage/closed-weekend reconciliation proportional to days and sessions.
    boundaries = {start.astimezone(UTC), end.astimezone(UTC)}
    day = start.astimezone(zone).date() - timedelta(days=1)
    final_day = end.astimezone(zone).date() + timedelta(days=1)
    while day <= final_day:
        candidates = [datetime.combine(day, time(), zone)]
        for opening, closing in session_windows(info, day) or []:
            candidates.extend((opening, closing))
        uncertain_break = str(info.get("uncertain_break") or "")
        if re.fullmatch(r"\d{4}-\d{4}", uncertain_break):
            for value in uncertain_break.split("-"):
                candidates.append(datetime.combine(day, time(int(value[:2]), int(value[2:])), zone))
        for candidate in candidates:
            candidate = candidate.astimezone(UTC)
            if start < candidate < end:
                boundaries.add(candidate)
        day += timedelta(days=1)
    ordered = sorted(boundaries)
    for lo, hi in zip(ordered, ordered[1:], strict=False):
        state = expectation(info, lo).value
        ranges = result[state]
        if ranges and ranges[-1][1] == lo:
            ranges[-1] = (ranges[-1][0], hi)
        else:
            ranges.append((lo, hi))
    return result
