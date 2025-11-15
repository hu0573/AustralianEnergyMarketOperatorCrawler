"""Utilities for splitting raw 5MIN records."""

from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
from typing import Any, Dict, Iterable, List, Optional, Tuple

ACTUAL_CAPTURE_FIELD = "FETCHED_AT_UTC"


ACTUAL_SETTLEMENT_FIELD = "SETTLEMENTDATE_UTC"
ACTUAL_CAPTURE_FIELD = "FETCHED_AT_UTC"


ACTUAL_COLUMNS = [
    ACTUAL_SETTLEMENT_FIELD,
    "REGIONID",
    "RRP",
    "TOTALDEMAND",
    "NETINTERCHANGE",
    "SCHEDULEDGENERATION",
    "SEMISCHEDULEDGENERATION",
    "APCFLAG",
    "PERIODTYPE",
    ACTUAL_CAPTURE_FIELD,
]


@dataclass
class RegionSnapshot:
    region: str
    actual_rows: List[Dict[str, Any]]
    forecast_price_row: Optional[Dict[str, Any]]
    forecast_demand_row: Optional[Dict[str, Any]]


def _parse_settlement(value: str) -> dt.datetime:
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    parsed = dt.datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    else:
        parsed = parsed.astimezone(dt.timezone.utc)
    return parsed


def _format_utc(ts: dt.datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    else:
        ts = ts.astimezone(dt.timezone.utc)
    return ts.isoformat()


def _normalize_actual_row(raw: Dict[str, Any]) -> Dict[str, Any]:
    result = {field: raw.get(field, "") for field in ACTUAL_COLUMNS if field not in {ACTUAL_SETTLEMENT_FIELD, ACTUAL_CAPTURE_FIELD}}
    settlement_raw = raw.get("SETTLEMENTDATE")
    if settlement_raw:
        result[ACTUAL_SETTLEMENT_FIELD] = _format_utc(_parse_settlement(settlement_raw))
    else:
        result[ACTUAL_SETTLEMENT_FIELD] = ""
    result[ACTUAL_CAPTURE_FIELD] = ""
    return result


def _build_forecast_rows(
    rows: Iterable[Dict[str, Any]],
    capture_time: dt.datetime,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    rows = list(rows)
    if not rows:
        return None, None

    capture_time_utc = capture_time.astimezone(dt.timezone.utc)
    sorted_rows = sorted(rows, key=lambda row: _parse_settlement(row["SETTLEMENTDATE"]))
    timestamps = [_parse_settlement(row["SETTLEMENTDATE"]) for row in sorted_rows]
    base_dt = next((ts for ts in timestamps if ts >= capture_time_utc), timestamps[0])

    price_row = {
        "capture_time_utc": capture_time_utc.isoformat(),
        "base_settlementdate_utc": base_dt.isoformat(),
    }
    demand_row = dict(price_row)
    for row, ts in zip(sorted_rows, timestamps):
        offset_minutes = int((ts - base_dt).total_seconds() // 60)
        if offset_minutes < 0:
            continue
        key = f"+{offset_minutes}m"
        price_row[f"{key}_RRP"] = row.get("RRP", "")
        demand_row[f"{key}_TOTALDEMAND"] = row.get("TOTALDEMAND", "")

    return price_row, demand_row


def build_region_snapshots(
    records: List[Dict[str, Any]],
    *,
    capture_time: Optional[dt.datetime] = None,
) -> Dict[str, RegionSnapshot]:
    capture_time = capture_time or dt.datetime.now(dt.timezone.utc)
    per_region: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}

    for row in records:
        region = row.get("REGIONID") or row.get("REGION")
        if not region:
            continue
        period_type = (row.get("PERIODTYPE") or "").upper()
        per_region.setdefault(region, {"ACTUAL": [], "FORECAST": []})
        if period_type == "FORECAST":
            per_region[region]["FORECAST"].append(row)
        else:
            per_region[region]["ACTUAL"].append(row)

    snapshots: Dict[str, RegionSnapshot] = {}
    for region, buckets in per_region.items():
        actual_rows = sorted(
            (_normalize_actual_row(row) for row in buckets["ACTUAL"]),
            key=lambda row: row[ACTUAL_SETTLEMENT_FIELD],
        )
        fetch_ts = _format_utc(capture_time)
        for row in actual_rows:
            row[ACTUAL_CAPTURE_FIELD] = fetch_ts
        price_row, demand_row = _build_forecast_rows(buckets["FORECAST"], capture_time)
        snapshots[region] = RegionSnapshot(
            region=region,
            actual_rows=actual_rows,
            forecast_price_row=price_row,
            forecast_demand_row=demand_row,
        )

    return snapshots


