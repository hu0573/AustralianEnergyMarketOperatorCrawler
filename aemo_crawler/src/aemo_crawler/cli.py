from __future__ import annotations

import argparse
from pathlib import Path

from .runner import run_once


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch AEMO 5MIN data and persist ACTUAL/FORECAST snapshots."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path.cwd() / "data" / "aemo_5min",
        help="Directory where CSV outputs will be stored (default: ./data/aemo_5min)",
    )
    parser.add_argument(
        "--time-scale",
        default="30MIN",
        help="timeScale 参数（默认为 30MIN，以保持 FORECAST 粒度一致）",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    summary = run_once(args.data_dir, time_scale=args.time_scale)
    print(f"✅ 数据已写入 {args.data_dir}")
    print("—— 抓取摘要 ——")
    for line in summary:
        print(line)


