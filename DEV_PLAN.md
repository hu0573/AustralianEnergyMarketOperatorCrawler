# Australian Energy Market Operator Crawler – 开发方案（电力部分）

## 1. 数据源与连接方式

- **主要接口**：`https://visualisations.aemo.com.au/aemo/apps/api/report/5MIN`
  - HTTPS POST 请求，**必须**携带 JSON 请求体 `{"timeScale": ["30MIN"]}`，否则服务器返回 500。还需提供常规浏览器 UA、`Referer: https://visualisations.aemo.com.au/aemo/apps/visualisation/index.html` 以及 `Origin: https://visualisations.aemo.com.au`。
  - 响应头 `cache-control: public, max-age=4`，意味着每 ~4 秒就会生成新的 5 分钟结算点；在抓取端可将轮询间隔设置在 4–10 秒之间，以免重复命中。
  - 返回 JSON，顶层键 `5MIN`，值为数组；每条记录包含 `SETTLEMENTDATE`、`REGIONID`、`RRP`、`TOTALDEMAND` 等。
  - 字段 `PERIODTYPE` 区分 `ACTUAL`（已结算的历史）与 `FORECAST`（未来的预测）。

- **安全/限流**：
  - 接口无需身份验证，但高频轮询仍需遵守 AEMO 公共数据使用规范；建议参考 `cache-control` 头选择 4–10 秒抓一次，再将结果按分钟级写盘。
  - 若遇到 500 错误，首先确认请求体是否包含 `timeScale`；若已携带，则视为瞬时异常，按指数退避重试。
  - 仓库已提供验证脚本 `workspaces/AustralianEnergyMarketOperatorCrawler/aemo_crawler/src/aemo_crawler/fetch_5min.py`，可用 `uv run python -m aemo_crawler.fetch_5min` 快速确认接口是否可用。

## 2. 数据示例

```json
{
  "SETTLEMENTDATE": "2025-11-14T11:35:00",
  "REGIONID": "NSW1",
  "RRP": -11.72846,
  "TOTALDEMAND": 5166.18,
  "NETINTERCHANGE": 645.76,
  "SCHEDULEDGENERATION": 2463.00009,
  "SEMISCHEDULEDGENERATION": 3343.68991,
  "PERIODTYPE": "ACTUAL",
  "APCFLAG": 0.0
}
```

预测段示例（字段一致，仅 `PERIODTYPE: "FORECAST"`，价格/需求值为预测）：

```json
{
  "SETTLEMENTDATE": "2025-11-16T03:30:00",
  "REGIONID": "SA1",
  "RRP": 75.12,
  "TOTALDEMAND": 1698.4,
  "PERIODTYPE": "FORECAST",
  ...
}
```

## 3. 数据特性

- **时间窗口**：AEMO 可视化展示「过去约 24 小时 + 未来约 16 小时」。
  - 历史段（ACTUAL）：
    - 粒度固定 5 分钟。
    - 窗口覆盖约 24 小时（例如抓包样本中，NSW1 从 `2025-11-14T11:25:00` 到 `2025-11-15T11:20:00` 共 288 条）。
    - 同一 `SETTLEMENTDATE` 只会出现一次，属于最终结算值。
  - 预测段（FORECAST）：
    - 粒度固定 30 分钟。
    - 当前窗口约 16.5 小时（样本中从 `2025-11-15T11:30:00` 到 `2025-11-16T04:00:00` 共 34 条）。
    - 同一 `SETTLEMENTDATE` 会随着新预测刷新，但旧预测需要保留以供回溯。
- **分区**：字段 `REGIONID` / `REGION` 指示所属市场：
  - `NSW1`, `QLD1`, `SA1`, `VIC1`, `TAS1`（若未来需要 WEM/其他区域，可扩展新的接口）。
- **指标**：
  - `RRP`（Regional Reference Price，$/MWh）
  - `TOTALDEMAND`（区域需求，MW）
  - `NETINTERCHANGE`、`SCHEDULEDGENERATION`、`SEMISCHEDULEDGENERATION` 等作为辅助字段。

## 4. 爬虫系统设计（电力）

### 4.1 输出形式

- 最终产出若干 CSV 文件，按以下维度组合：
  1. **数据类型**：`actual`（历史） vs `forecast`（预测）。
  2. **区域**：`NSW1`、`QLD1`、`SA1`、`VIC1`、`TAS1`。
  3. （可选）按日期拆分文件，方便归档，例如 `electricity_actual_NSW1_2025-11-15.csv`。

### 4.2 记录策略

- **历史数据（actual）**：
  - 每条 `SETTLEMENTDATE` 只会出现一次，保存后无需再次更新。
  - 处理方式：抓取结果中过滤 `PERIODTYPE=ACTUAL`，若 CSV 已包含该时间戳则跳过，否则追加。

- **预测数据（forecast）**：
  - 不覆盖旧值，按“二维矩阵”方式全量保留。
  - 结构设计：
    - 每个区域生成 **两个** CSV：`forecast_price_<REGION>.csv`（只存 RRP），`forecast_demand_<REGION>.csv`（只存 TOTALDEMAND）。这样矩阵保持二维。
    - **行（索引）**：抓取时间（UTC），表示“当前快照”。记录 `capture_time_utc` 与 `base_settlementdate`（快照中最接近当前的预测点，通常是未来下一个 30 分钟刻度）。
    - **列**：以偏移量命名，粒度 30 分钟（例如 `+0m`, `+30m`, `+60m`, … 直至 `+990m`，即 16.5 小时）。每列只包含对应指标值（价格或需求）。
    - CSV 示例（价格矩阵）：

      | capture_time_utc | base_settlementdate | +0m_RRP | +30m_RRP | +60m_RRP | … |
      |------------------|---------------------|--------:|---------:|---------:|---|
      | 2025-11-15T11:05Z | 2025-11-15T11:30Z   |   -19.7 |    -17.4 |    -16.3 | … |
      | 2025-11-15T11:35Z | 2025-11-15T12:00Z   |   -17.4 |    -16.3 |    -15.1 | … |

  - 写入逻辑：
    1. 解析一次接口响应后，筛出 `PERIODTYPE=FORECAST`，按 `REGIONID` 分组。
    2. 以“最接近当前的预测时刻”作为 `base_settlementdate`，其余预测点按照 30 分钟步长计算 `offset = (forecast_settlementdate - base_settlementdate)`。
    3. 按指标拆分：价格写入 `forecast_price_<REGION>.csv` 的行字典 `{"+0m": rrp_value, "+30m": rrp_value, …}`；需求写入 `forecast_demand_<REGION>.csv`。
    4. 将两份行数据 append 到各自 CSV，永不覆盖；需要时再通过 pivot/stack 转换成其他分析格式。

### 4.3 模块划分

1. **Fetcher**：负责构造请求、处理重试、解析 JSON。
2. **Transformer**：拆分 ACTUAL / FORECAST，按区域切片。
3. **Storage**：
   - `ActualStorage`: 追加写入（幂等检查 `SETTLEMENTDATE`）。
   - `ForecastStorage`: 维护二维矩阵 CSV（行=抓取时间，列=偏移量），新增偏移列时自动扩张。
4. **Scheduler**：例如每分钟运行一次，或依据缓存头设置 20–30 秒抓一次。

### 4.4 运行流程（示例）

1. 定时任务触发抓取。
2. Fetcher 获取 JSON（含 ACTUAL+FORECAST）；Transformer 输出 `dict(region -> {actual_rows, forecast_rows})`。
3. Storage 层：
   - 对 ACTUAL：若 CSV 中不存在该 `SETTLEMENTDATE` 则 append。
   - 对 FORECAST：直接 append，记录 `capture_time_utc`。
4. 适时压缩/归档老文件（例如每日生成归档，减少单文件体积）。

## 5. 下一步

- 对接 `visualisations.aemo.com.au` 中其它电力相关接口（价格上限、应急等）。
- 若未来要扩展到 Gas，可在该方案基础上新增 CSV/JSON fetcher 与预测逻辑。
- 建议在仓库内加入 `docs/` 或 `README` 记录字段说明、运行脚本示例、数据归档策略。

