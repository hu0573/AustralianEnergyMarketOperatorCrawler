"""One-shot execution pipeline for the crawler."""

from __future__ import annotations

from pathlib import Path
from typing import List

from .api import fetch_5min_snapshot
from .storage import ActualStorage, ForecastStorage
from .transformer import build_region_snapshots


def run_once(data_dir: Path, *, time_scale: str = "30MIN") -> List[str]:
    data_dir = Path(data_dir)
    actual_store = ActualStorage(data_dir / "actual")
    forecast_store = ForecastStorage(
        price_dir=data_dir / "forecast" / "price",
        demand_dir=data_dir / "forecast" / "demand",
    )

    records = fetch_5min_snapshot(time_scale=time_scale)
    snapshots = build_region_snapshots(records)

    summary: List[str] = []
    for region in sorted(snapshots.keys()):
        snapshot = snapshots[region]
        added = actual_store.persist(region, snapshot.actual_rows)
        has_forecast = snapshot.forecast_price_row or snapshot.forecast_demand_row
        if has_forecast:
            forecast_store.persist(
                region,
                price_row=snapshot.forecast_price_row,
                demand_row=snapshot.forecast_demand_row,
            )
        summary.append(
            f"{region}: +{added} ACTUAL rows"
            + ("; captured FORECAST snapshot" if has_forecast else "")
        )

    return summary


