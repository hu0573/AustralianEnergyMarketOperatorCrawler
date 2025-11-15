"""简单的 5MIN 接口抓取脚本，用于验证 AEMO 数据抓取方案。"""

from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List

from .api import fetch_5min_snapshot


def summarize(records: List[Dict[str, Any]]) -> str:
    """输出简要统计信息，帮助人工确认数据质量。"""
    total = len(records)
    if total == 0:
        return "未返回任何记录"

    first_ts = records[0].get("SETTLEMENTDATE")
    last_ts = records[-1].get("SETTLEMENTDATE")
    regions = {row.get("REGIONID") for row in records}

    msg = [
        f"记录数: {total}",
        f"区域: {', '.join(sorted(str(r) for r in regions if r))}",
        f"时间范围: {first_ts} -> {last_ts}",
    ]
    # 提供一个示例价格，便于快速确认
    first = records[0]
    try:
        price = float(first["RRP"])
        demand = float(first.get("TOTALDEMAND", 0))
        msg.append(f"首条记录: region={first.get('REGIONID')} RRP={price:.2f} TOTALDEMAND={demand:.1f}")
    except (KeyError, TypeError, ValueError):
        pass
    msg.append(
        f"抓取时间(fetched_at_utc): {dt.datetime.now(dt.timezone.utc).isoformat()}"
    )
    return "\n".join(msg)


def main() -> None:
    records = fetch_5min_snapshot()
    print(summarize(records))


if __name__ == "__main__":
    main()

