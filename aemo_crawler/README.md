## 使用方法

```bash
uv run aemo-crawler --data-dir ./data/aemo_5min
```

命令将执行一次抓取，默认调用 AEMO 5MIN 接口并在目标目录下生成：

- `actual/electricity_actual_<REGION>.csv`
- `forecast/price/forecast_price_<REGION>.csv`
- `forecast/demand/forecast_demand_<REGION>.csv`

若只想快速检查接口是否可用，可运行：

```bash
uv run python -m aemo_crawler.fetch_5min
```
