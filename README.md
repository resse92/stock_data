# Rust 版全量 K 线抓取（仅获取层）

本实现对应你现有 Python 脚本的数据抓取逻辑，但**只做获取与标准化**，不做 DuckDB/SwanLake 落地。

## 目标

- 拉取全市场股票列表（优先宽板块，失败后遍历全部板块）
- 按批次请求 `/api/v1/data/market`
- 覆盖 `1d` 和 `1m` 两个周期
- 兼容多种响应结构并归一化为统一行格式
- 输出 JSONL（可选保存原始批次 JSON）

## 与架构文档的对应关系

- 当前实现对应 `raw -> normalize` 的前半段
- 未实现 `curated` 分区写入、去重 (`symbol,time`)、并发写入控制、watermark
- 后续接 SwanLake 时建议保持本程序输出 schema 不变，把 sink 抽成 trait：
  - `RawSink`（保存 raw batch）
  - `CuratedSink`（写 `daily_bars` / `minute_bars_1m`）

## 输出文件

- `output/normalized/kline_1d.jsonl`
- `output/normalized/kline_1m.jsonl`
- 可选：`output/raw/{period}_batch_00001.json`

每行字段：

- `stock_code`
- `period`
- `ts_raw`
- `open/high/low/close`
- `volume/amount`
- `turnover_rate/open_interest/settle/adj_factor`
- `extra_json`

## 运行（MinIO）

```bash
cd /Users/resse/Desktop/stock_data
cargo run --release -- sync-daily \
  --start-date 2026-03-01 \
  --end-date 2026-03-24 \
  --chunk-size 200
```

`.env` 里至少配置：

- `QMT_API_HOST`
- `QMT_API_AUTHORIZATION`
- `QMT_API_TIMEOUT`
- `s3_host` 或 `S3_HOST`（MinIO endpoint，如 `192.168.2.139:40711`）
- `S3_ACCESS_KEY`
- `S3_SECRET_KEY`
- `S3_BUCKET`（默认 `stock`）

## 代码结构

- `ApiClient`：HTTP 请求与鉴权
- `discover_all_stock_codes`：全市场股票列表发现
- `normalize_full_kline_response`：响应结构兼容与归一化
- `SwanLakeSink`：按架构目录写入 raw/curated（当前为方法层接入）
