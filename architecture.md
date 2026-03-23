# 总体原则

核心目标有四个：

- **增量写入简单**
- **查询单票/单日/单区间快**
- **避免小文件爆炸**
- **后续能平滑升级到 Iceberg/Delta**

所以推荐你把数据拆成两类主表：

- `daily_bars`：日线
- `minute_bars_1m`：1分钟线（其他周期单独存储）

再配两类辅助表：

- `instruments`：证券主数据
- `ingestion_log`：更新日志 / 水位表

---

# 一、目录结构建议

先用一个明确的 lake 根目录，比如：

```text
s3://stock-lake/
  raw/
  curated/
  meta/
```

推荐分三层：

```text
raw/       原始抓取结果，保留回放能力
curated/   清洗后的标准 Parquet，给查询用
meta/      元数据、更新水位、错误日志
```

你的核心查询层放在 `curated/`。

---

# 二、日线表怎么设计

## 1. 表结构

`daily_bars`

```text
symbol              STRING   -- 000001.SZ
exchange            STRING   -- SZ/SH/BJ
time                DATE
open                DOUBLE
high                DOUBLE
low                 DOUBLE
close               DOUBLE
pre_close           DOUBLE
volume              BIGINT
amount              DOUBLE
adj_factor          DOUBLE   -- 可选，后复权因子
turnover_rate       DOUBLE   -- 可选
suspended           BOOLEAN  -- 可选
source              STRING   -- tushare/akshare/vendor_x
ingested_at         TIMESTAMP
```

如果你有前复权/后复权需求，不要混在一张表里存多套 OHLC。更推荐：

- 原始行情表只存 **未复权**
- 复权因子单独一列或单独一张表
- 查询时再算前复权/后复权

这样后续修正更方便。

## 2. 分区方式

日线推荐：

```text
curated/daily_bars/exchange=SZ/year=2026/month=03/part-xxxx.parquet
curated/daily_bars/exchange=SH/year=2026/month=03/part-xxxx.parquet
curated/daily_bars/exchange=BJ/year=2026/month=03/part-xxxx.parquet
```

不要按 `symbol=` 分区。原因：

- 股票太多，分区数量会炸
- 每天每只股票一小文件，后面很难维护
- 扫全市场某天数据时会非常慢

日线最合适的是：

- **一级分区：exchange**
- **二级分区：year/month**
- 文件内按 `time, symbol` 排序

这样兼顾：

- 查全市场某月
- 查某交易所某区间
- 查单票时也还能接受

## 3. 文件大小

日线单文件建议：

- **128MB 到 512MB**

日线量不算大，不需要切得太碎。

---

# 三、分钟线表怎么设计

分钟线才是重点，量会大很多。

## 1. 表结构

`minute_bars_1m`

```text
symbol              STRING   -- 000001.SZ
exchange            STRING   -- SZ/SH/BJ
time                TIMESTAMP  -- 2026-03-20 10:31:00
open                DOUBLE
high                DOUBLE
low                 DOUBLE
close               DOUBLE
volume              BIGINT
amount              DOUBLE
vwap                DOUBLE     -- 可选
trade_count         BIGINT     -- 可选
source              STRING
ingested_at         TIMESTAMP
```

你后续如果要存 5m、15m，建议直接单独建数据集：

- `minute_bars_5m`
- `minute_bars_15m`

不要混在同一张表里加 `interval` 字段。

## 2. 分区方式

分钟线推荐：

```text
curated/minute_bars_1m/trade_date=2026-03-20/exchange=SZ/part-000.parquet
curated/minute_bars_1m/trade_date=2026-03-20/exchange=SH/part-000.parquet
```

这是最实用的方式。

优先顺序：

- `trade_date`
- `exchange`

原因：

- 你的增量更新本质上是“按交易日”写入
- 绝大部分回测/分析会带日期过滤
- 每天追加最自然
- 某一天补数据也简单

**不要按 symbol 分区**，分钟线会直接变成海量小文件地狱。

## 3. 文件内排序

分钟线文件内建议排序：

```text
symbol, time
```

这样查单票某段分钟数据会更省扫描。

## 4. 文件大小

分钟线建议：

- **256MB 到 1GB**

宁可少量中等大文件，也不要几十万个小文件。

---

# 四、raw 层怎么放

raw 层不要追求优雅，追求“可回放、可追责”。

例如：

```text
raw/vendor=tushare/type=daily/trade_date=2026-03-20/batch_001.json
raw/vendor=tushare/type=minute/trade_date=2026-03-20/batch_001.parquet
```

raw 层作用：

- 数据源出错时可重跑
- 清洗逻辑变更时可重建 curated
- 保留原始字段

---

# 五、增量更新策略

最稳的策略不是直接改旧文件，而是：

```text
抓取当天数据
→ 落 raw
→ 清洗标准化
→ 写入 curated 当日分区
→ 记录水位
```

## 日线增量

每天收盘后：

- 拉取最新交易日全 A 股日线
- 写入：
  - `curated/daily_bars/exchange=SZ/year=2026/month=03/...`
  - `...SH...`
  - `...BJ...`

如果发生补历史、除权修正，不建议在原文件上零碎更新。更稳的是：

- 重写受影响月分区
- 或做版本化分区

## 分钟线增量

分钟线建议按交易日批量落地，而不是边来边 append parquet。

日内可以先存到：

- 内存
- 本地临时 parquet
- 或 raw staging

收盘后一次性写入：

```text
curated/minute_bars_1m/trade_date=YYYY-MM-DD/exchange=SZ/part-xxx.parquet
```

这样最稳。

## 幂等与并发写入约束

- 去重键统一：`symbol,time`
- 同一分区同一时刻只允许一个 writer
- 推荐写入流程：先写临时目录（staging）再原子替换目标分区
- 如果任务重跑，按分区重写，避免行级原地更新 parquet

---

# 六、元数据表必须有

## 1. instruments

证券主数据表：

```text
symbol
exchange
name
list_date
delist_date
asset_type
board
is_active
updated_at
```

作用：

- 过滤退市股
- 查询股票池
- 补齐交易所信息

## 2. ingestion_log

更新日志表：

```text
table_name
partition_key
source
row_count
min_time
max_time
status
error_message
ingested_at
```

## 3. watermark

增量水位表：

```text
dataset         -- daily_bars / minute_bars_1m
symbol          -- 可空；若按全市场更新可不存
last_time
updated_at
```

如果你是全市场整批更新，水位可以按 dataset 维护，不必按 symbol 维护。

---

# 七、日线和分钟线要不要放一起

不要。

原因：

- 粒度不同
- 更新频率不同
- 查询模式不同
- 文件体量差异太大

至少拆成：

- `daily_bars`
- `minute_bars_1m`
- `minute_bars_5m`（如果需要）
- `minute_bars_15m`（如果需要）

---

# 八、字段设计的几个关键点

## 1. symbol 统一格式

统一成：

```text
000001.SZ
600000.SH
430001.BJ
```

不要一会儿 `sz000001`，一会儿 `000001.XSHE`。

## 2. 时间统一

- 字段名统一使用 `time`
- 日线：`time` 用 `DATE`
- 分钟线：`time` 用交易所本地时间对应的 `TIMESTAMP`

分钟线最好额外留一个：

```text
bar_minute_key  STRING  -- 202603201031
```

某些工具链过滤会更方便，但这不是必须。

## 3. 数值类型

价格类一般用：

- `DOUBLE`

如果你特别在意精度，也可以用 decimal，但 DuckDB/Parquet 场景下多数人直接用 double。

成交量：

- `BIGINT`

成交额：

- `DOUBLE`

---

# 九、你最该避免的坑

## 1. 按 symbol 分区

这是最常见错误。全 A 股分钟线会直接产生海量小文件。

## 2. 每天每只股票一个 parquet

后面查询、合并、维护都会很痛苦。

## 3. 原地修改 parquet

Parquet 更适合“重写分区”而不是“行级更新”。

## 4. 把复权价直接覆盖原始价

后面很难审计和纠错。

---

# 十、一个可落地的目录示例

## 日线

```text
s3://stock-lake/curated/daily_bars/
  exchange=SZ/year=2026/month=03/part-000.parquet
  exchange=SH/year=2026/month=03/part-000.parquet
  exchange=BJ/year=2026/month=03/part-000.parquet
```

## 1分钟线

```text
s3://stock-lake/curated/minute_bars_1m/
  trade_date=2026-03-20/exchange=SZ/part-000.parquet
  trade_date=2026-03-20/exchange=SH/part-000.parquet
  trade_date=2026-03-20/exchange=BJ/part-000.parquet
```

## 5分钟线（可选）

```text
s3://stock-lake/curated/minute_bars_5m/
  trade_date=2026-03-20/exchange=SZ/part-000.parquet
  trade_date=2026-03-20/exchange=SH/part-000.parquet
  trade_date=2026-03-20/exchange=BJ/part-000.parquet
```

## 元数据

```text
s3://stock-lake/meta/instruments/part-000.parquet
s3://stock-lake/meta/ingestion_log/part-000.parquet
s3://stock-lake/meta/watermark/part-000.parquet
```

---

# 十一、如果你现在只做 SwanLake + MinIO，推荐最小方案

先别上太复杂的湖格式，直接：

- 日线一张表
- 1分钟线一张表（其他周期单独加表）
- `instruments`
- `ingestion_log`

目录就用我上面那套。

等你后面出现这些需求，再考虑升级到 Iceberg：

- 需要回滚版本
- 需要并发写入
- 需要 upsert / merge
- 需要 schema evolution 更强

---

# 十二、我的最终建议

你的场景最推荐这套：

**日线**

- 表：`daily_bars`
- 分区：`exchange/year/month`

**分钟线**

- 表：`minute_bars_1m`（其他周期单独表）
- 分区：`trade_date/exchange`

**元数据**

- `instruments`
- `ingestion_log`
- `watermark`

**写入策略**

- raw 先落
- curated 按分区批量重写/追加
- 去重键统一 `symbol,time`
- 不按 symbol 分区
- 不原地更新 parquet

---

如果你愿意，我下一条可以直接给你一份**完整的 MinIO 目录规划 + DuckDB/SwanLake 建表 SQL + 每日增量更新脚本框架**。
