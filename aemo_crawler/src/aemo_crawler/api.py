"""Networking helpers for the AEMO 5MIN report."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import httpx

API_URL = "https://visualisations.aemo.com.au/aemo/apps/api/report/5MIN"
DEFAULT_HEADERS = {
    "Referer": "https://visualisations.aemo.com.au/aemo/apps/visualisation/index.html",
    "Origin": "https://visualisations.aemo.com.au",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Content-Type": "application/json",
}


def fetch_5min_snapshot(
    *,
    time_scale: str = "30MIN",
    timeout: float = 15.0,
    client: Optional[httpx.Client] = None,
) -> List[Dict[str, Any]]:
    """Fetch the combined ACTUAL + FORECAST window."""
    payload = {"timeScale": [time_scale]}
    owns_client = client is None
    if client is None:
        client = httpx.Client(timeout=timeout)

    try:
        response = client.post(API_URL, headers=DEFAULT_HEADERS, json=payload)
        response.raise_for_status()
    finally:
        if owns_client:
            client.close()

    data = response.json()
    records = data.get("5MIN")
    if not isinstance(records, list):
        raise ValueError("响应中未包含 5MIN 数组")
    return records


