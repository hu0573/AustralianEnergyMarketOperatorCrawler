"""Utilities for splitting raw 5MIN records."""

from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
from typing import Any, Dict, Iterable, List, Optional, Tuple

ACTUAL_CAPTURE_FIELD = "FETCHED_AT_UTC"


ACTUAL_COLUMNS = [
    "SETTLEMENTDATE",
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
    return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed


def _normalize_actual_row(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {field: raw.get(field, "") for field in ACTUAL_COLUMNS}


def _build_forecast_rows(
    rows: Iterable[Dict[str, Any]],
    capture_time: dt.datetime,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    rows = list(rows)
    if not rows:
        return None, None

    capture_time_utc = capture_time.astimezone(dt.timezone.utc)
    capture_time_naive = capture_time_utc.replace(tzinfo=None)

    sorted_rows = sorted(rows, key=lambda row: _parse_settlement(row["SETTLEMENTDATE"]))
    timestamps = [_parse_settlement(row["SETTLEMENTDATE"]) for row in sorted_rows]
    base_dt = next((ts for ts in timestamps if ts >= capture_time_naive), timestamps[0])

    price_row = {
        "capture_time_utc": capture_time_utc.isoformat().replace("+00:00", "Z"),
        "base_settlementdate": base_dt.isoformat(),
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
            key=lambda row: row["SETTLEMENTDATE"],
        )
        fetch_ts = capture_time.isoformat()
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


