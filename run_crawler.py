from __future__ import annotations

import argparse
import datetime as dt
import time
from pathlib import Path

import sys

REPO_ROOT = Path(__file__).resolve().parent
SRC_DIR = REPO_ROOT / "aemo_crawler" / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from aemo_crawler.runner import run_once


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="连续抓取 AEMO 5MIN 数据并按 DEV_PLAN 方案写入 CSV。"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(__file__).parent / "data",
        help="数据输出目录（默认：项目根目录下的 data/）",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="抓取间隔（秒），默认 300 秒即 5 分钟。",
    )
    parser.add_argument(
        "--time-scale",
        default="30MIN",
        help="timeScale 参数，保持与 FORECAST 粒度一致（默认 30MIN）。",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_dir = args.data_dir.expanduser().resolve()
    interval = max(1, args.interval)

    print("🚀 AEMO 5MIN 连续抓取启动，按 Ctrl+C 终止。")
    print(f"   输出目录: {data_dir}")
    print(f"   抓取间隔: {interval} 秒")

    try:
        while True:
            start = dt.datetime.now(dt.timezone.utc)
            print(f"\n[{start.isoformat()}] 开始抓取……")
            summary = run_once(data_dir, time_scale=args.time_scale)
            for line in summary:
                print(f"  • {line}")
            print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] 本次抓取完成。")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n🛑 已收到中断信号，停止抓取。")


if __name__ == "__main__":
    main()

