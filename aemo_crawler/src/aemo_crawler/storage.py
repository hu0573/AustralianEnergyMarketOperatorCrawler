"""CSV storage helpers for ACTUAL and FORECAST datasets."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

from .transformer import ACTUAL_COLUMNS


class ActualStorage:
    def __init__(self, root_dir: Path) -> None:
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def _file_for_region(self, region: str) -> Path:
        return self.root_dir / f"electricity_actual_{region}.csv"

    def persist(self, region: str, rows: Iterable[Dict[str, object]]) -> int:
        rows = list(rows)
        if not rows:
            return 0

        path = self._file_for_region(region)
        seen = set()
        if path.exists():
            with path.open("r", newline="") as fh:
                reader = csv.DictReader(fh)
                seen = {row["SETTLEMENTDATE"] for row in reader if row.get("SETTLEMENTDATE")}

        new_rows = [row for row in rows if row.get("SETTLEMENTDATE") not in seen]
        if not new_rows:
            return 0

        mode = "a" if path.exists() else "w"
        with path.open(mode, newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=ACTUAL_COLUMNS)
            if mode == "w":
                writer.writeheader()
            for row in new_rows:
                writer.writerow({field: row.get(field, "") for field in ACTUAL_COLUMNS})

        return len(new_rows)


class ForecastStorage:
    def __init__(self, price_dir: Path, demand_dir: Path) -> None:
        self.price_dir = Path(price_dir)
        self.demand_dir = Path(demand_dir)
        self.price_dir.mkdir(parents=True, exist_ok=True)
        self.demand_dir.mkdir(parents=True, exist_ok=True)

    def _append_matrix_row(self, path: Path, row: Dict[str, object]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            with path.open("r", newline="") as fh:
                reader = csv.DictReader(fh)
                existing_rows = list(reader)
                fieldnames: List[str] = reader.fieldnames or []
        else:
            existing_rows = []
            fieldnames = []

        base_fields = ["capture_time_utc", "base_settlementdate"]
        for column in base_fields:
            if column not in fieldnames:
                fieldnames.insert(len(fieldnames) if fieldnames else len(fieldnames), column)

        new_columns = [col for col in row.keys() if col not in fieldnames]
        fieldnames.extend(new_columns)

        existing_rows.append(row)
        with path.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for entry in existing_rows:
                writer.writerow({field: entry.get(field, "") for field in fieldnames})

    def persist(
        self,
        region: str,
        *,
        price_row: Dict[str, object] | None,
        demand_row: Dict[str, object] | None,
    ) -> None:
        if price_row:
            price_path = self.price_dir / f"forecast_price_{region}.csv"
            self._append_matrix_row(price_path, price_row)
        if demand_row:
            demand_path = self.demand_dir / f"forecast_demand_{region}.csv"
            self._append_matrix_row(demand_path, demand_row)


