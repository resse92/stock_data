use std::cmp::{max, min};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::{Datelike, NaiveDate};
use clap::Args;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

use crate::api::ApiClient;
use crate::models::{DailyBar, MarketRequest, DEFAULT_TIMEOUT_SECS};
use crate::normalize::normalize_full_kline_response;
use crate::s3::{build_s3_client, ensure_bucket, upload_daily_partition_file, S3Settings};
use crate::utils::{chunked, load_stock_codes_from_file};

#[derive(Debug, Args)]
pub struct SyncDailyArgs {
    #[arg(long, help = "开始日期，YYYY-MM-DD 或 YYYYMMDD")]
    pub start_date: String,

    #[arg(long, help = "结束日期，YYYY-MM-DD 或 YYYYMMDD")]
    pub end_date: String,

    #[arg(long, default_value_t = 200)]
    pub chunk_size: usize,

    #[arg(long, default_value_t = 8)]
    pub fetch_concurrency: usize,

    #[arg(long, default_value_t = false)]
    pub incremental: bool,

    #[arg(long, default_value = "meta/ingestion/daily_watermark.txt")]
    pub watermark_file: PathBuf,

    #[arg(long)]
    pub stock_codes_file: Option<PathBuf>,

    #[arg(long, env = "QMT_API_HOST", default_value = "http://127.0.0.1:8000")]
    pub base_url: String,

    #[arg(long, env = "QMT_API_AUTHORIZATION")]
    pub authorization: Option<String>,

    #[arg(long, env = "QMT_API_TIMEOUT", default_value_t = DEFAULT_TIMEOUT_SECS)]
    pub timeout: u64,

    #[arg(long, env = "S3_BUCKET", default_value = "stock")]
    pub s3_bucket: String,

    #[arg(long, env = "S3_REGION", default_value = "us-east-1")]
    pub s3_region: String,

    #[arg(long, env = "S3_ACCESS_KEY")]
    pub s3_access_key: Option<String>,

    #[arg(long, env = "S3_SECRET_KEY")]
    pub s3_secret_key: Option<String>,

    #[arg(long, help = "S3 endpoint，默认读取 S3_HOST 或 s3_host")]
    pub s3_host: Option<String>,
}

pub async fn run_sync_daily(args: SyncDailyArgs) -> Result<()> {
    if args.chunk_size == 0 {
        return Err(anyhow!("--chunk-size 必须大于 0"));
    }
    if args.fetch_concurrency == 0 {
        return Err(anyhow!("--fetch-concurrency 必须大于 0"));
    }

    let mut start_date = compact_date(&args.start_date)?;
    let end_date = compact_date(&args.end_date)?;
    let mut start_day = parse_compact_date(&start_date)?;
    let end_day = parse_compact_date(&end_date)?;

    if args.incremental {
        if let Some(watermark_day) = read_watermark_day(&args.watermark_file)? {
            if let Some(next_day) = watermark_day.succ_opt() {
                if next_day > start_day {
                    start_day = next_day;
                    start_date = format_compact_date(start_day);
                }
            }
            println!(
                "[INFO] incremental 模式: watermark={}, 实际开始日期={}",
                format_compact_date(watermark_day),
                start_date
            );
        } else {
            println!(
                "[INFO] incremental 模式: 未找到 watermark 文件 {}, 按参数全量开始",
                args.watermark_file.display()
            );
        }
    }

    if start_day > end_day {
        println!(
            "[DONE] 无需同步: 开始日期 {} 晚于结束日期 {}",
            start_date, end_date
        );
        return Ok(());
    }

    let s3_host = args
        .s3_host
        .or_else(|| env_var_any(&["S3_HOST", "s3_host"]))
        .ok_or_else(|| anyhow!("缺少 S3 host，请在 .env 设置 s3_host 或 S3_HOST"))?;

    let api = ApiClient::new(
        args.base_url,
        args.authorization,
        Duration::from_secs(args.timeout),
    )?;

    let stock_codes = if let Some(path) = args.stock_codes_file.as_ref() {
        let codes = load_stock_codes_from_file(path)?;
        println!("[INFO] 从文件加载股票 {} 只", codes.len());
        codes
    } else {
        let codes = api.discover_all_stock_codes().await?;
        println!("[INFO] 自动发现股票 {} 只", codes.len());
        codes
    };

    let s3_settings = S3Settings {
        endpoint: s3_host,
        bucket: args.s3_bucket,
        access_key: args.s3_access_key,
        secret_key: args.s3_secret_key,
        region: args.s3_region,
    };

    let s3 = build_s3_client(&s3_settings).await?;
    ensure_bucket(&s3, &s3_settings.bucket)
        .await
        .with_context(|| format!("ensure bucket {} failed", s3_settings.bucket))?;
    let bucket_name = s3_settings.bucket.clone();
    let windows = month_windows(&start_date, &end_date)?;
    let total_months = windows.len();
    let batches = chunked(&stock_codes, args.chunk_size);
    let total_batches = batches.len();

    println!(
        "[INFO] 开始拉取日线: {} 只股票, {} 批/月, {} 个月, 区间 {} ~ {}",
        stock_codes.len(),
        total_batches,
        total_months,
        start_date,
        end_date
    );

    #[derive(Debug)]
    enum WriteJob {
        Batch {
            bars: Vec<DailyBar>,
        },
        FlushMonth {
            month_idx: usize,
            total_months: usize,
            ack: oneshot::Sender<Result<()>>,
        },
    }

    let writer_bucket = bucket_name.clone();
    let (tx, mut rx) = mpsc::channel::<WriteJob>(8);
    let writer = tokio::spawn(async move {
        let mut uploaded_files: BTreeSet<String> = BTreeSet::new();
        let mut written_rows = 0usize;
        let mut month_partition_cache: BTreeMap<(String, String, String), Vec<DailyBar>> =
            BTreeMap::new();

        while let Some(job) = rx.recv().await {
            match job {
                WriteJob::Batch { bars } => {
                    for bar in bars {
                        if bar.time.len() < 7 {
                            continue;
                        }
                        let year = bar.time[0..4].to_string();
                        let month = bar.time[5..7].to_string();
                        let key = (bar.exchange.clone(), year, month);
                        month_partition_cache.entry(key).or_default().push(bar);
                        written_rows += 1;
                    }
                }
                WriteJob::FlushMonth {
                    month_idx,
                    total_months,
                    ack,
                } => {
                    let upload_result = async {
                        let mut uploaded_now = 0usize;
                        for ((exchange, year, month), rows) in month_partition_cache {
                            let deduped_rows = dedup_daily_rows(rows);
                            let key = upload_daily_partition_file(
                                &s3,
                                &writer_bucket,
                                &exchange,
                                &year,
                                &month,
                                &deduped_rows,
                            )
                            .await?;
                            uploaded_files.insert(key);
                            uploaded_now += 1;
                        }

                        println!(
                            "[WRITER] 月 {}/{} 上传完成: {} 个分区文件, 累计 {} 条/{} 文件",
                            month_idx,
                            total_months,
                            uploaded_now,
                            written_rows,
                            uploaded_files.len()
                        );
                        Ok::<(), anyhow::Error>(())
                    }
                    .await;

                    month_partition_cache = BTreeMap::new();
                    if let Err(err) = upload_result {
                        let _ = ack.send(Err(anyhow!("{err:#}")));
                        return Err(err);
                    }
                    let _ = ack.send(Ok(()));
                }
            }
        }

        Ok::<(usize, usize), anyhow::Error>((written_rows, uploaded_files.len()))
    });

    for (month_idx, (month_start, month_end)) in windows.into_iter().enumerate() {
        println!(
            "[MONTH] {}/{} 拉取 {} ~ {}",
            month_idx + 1,
            total_months,
            month_start,
            month_end
        );

        let mut join_set = JoinSet::new();
        let mut next_batch_idx = 0usize;
        while next_batch_idx < total_batches || !join_set.is_empty() {
            while next_batch_idx < total_batches && join_set.len() < args.fetch_concurrency {
                let batch_no = next_batch_idx + 1;
                let chunk = batches[next_batch_idx].clone();
                let month_start_clone = month_start.clone();
                let month_end_clone = month_end.clone();
                let api_clone = api.clone();
                join_set.spawn(async move {
                    let req =
                        MarketRequest::new(chunk, "1d", month_start_clone, month_end_clone, "none");
                    let resp = api_clone.fetch_market_batch(&req).await?;
                    let rows = normalize_full_kline_response(&resp, "1d");
                    let mut batch_bars: Vec<DailyBar> = Vec::new();
                    for row in &rows {
                        if let Some(bar) = DailyBar::from_normalized(row) {
                            batch_bars.push(bar);
                        }
                    }
                    Ok::<(usize, usize, Vec<DailyBar>), anyhow::Error>((batch_no, rows.len(), batch_bars))
                });
                next_batch_idx += 1;
            }

            let finished = join_set
                .join_next()
                .await
                .ok_or_else(|| anyhow!("fetch 并发任务异常结束"))?;
            let (batch_no, row_count, bars) =
                finished.map_err(|e| anyhow!("fetch task join error: {e}"))??;

            tx.send(WriteJob::Batch { bars })
                .await
                .map_err(|_| anyhow!("写入队列已关闭"))?;

            println!(
                "[FETCHER] 月 {}/{} 批次 {}/{}: 解析 {} 条",
                month_idx + 1,
                total_months,
                batch_no,
                total_batches,
                row_count
            );
        }

        let (ack_tx, ack_rx) = oneshot::channel();
        tx.send(WriteJob::FlushMonth {
            month_idx: month_idx + 1,
            total_months,
            ack: ack_tx,
        })
        .await
        .map_err(|_| anyhow!("写入队列已关闭"))?;

        ack_rx
            .await
            .map_err(|_| anyhow!("writer flush ack 通道已关闭"))??;

        if args.incremental {
            write_watermark_day(&args.watermark_file, &month_end)?;
            println!(
                "[INFO] 更新 watermark: {} -> {}",
                args.watermark_file.display(),
                month_end
            );
        }
    }

    drop(tx);
    let (written_rows, uploaded_files) = writer
        .await
        .map_err(|e| anyhow!("writer task join error: {e}"))??;

    if args.incremental {
        write_watermark_day(&args.watermark_file, &end_date)?;
        println!(
            "[INFO] 最终 watermark 已更新: {} -> {}",
            args.watermark_file.display(),
            end_date
        );
    }

    println!(
        "[DONE] 上传完成: 累计写入 {} 条日线, {} 个分区文件, bucket={}",
        written_rows, uploaded_files, bucket_name
    );

    Ok(())
}

fn dedup_daily_rows(rows: Vec<DailyBar>) -> Vec<DailyBar> {
    let mut keyed: BTreeMap<(String, String), DailyBar> = BTreeMap::new();
    for row in rows {
        keyed.insert((row.symbol.clone(), row.time.clone()), row);
    }
    let mut out: Vec<DailyBar> = keyed.into_values().collect();
    out.sort_by(|a, b| {
        a.symbol
            .cmp(&b.symbol)
            .then(a.time.cmp(&b.time))
            .then(a.exchange.cmp(&b.exchange))
    });
    out
}

fn read_watermark_day(path: &PathBuf) -> Result<Option<NaiveDate>> {
    if !path.exists() {
        return Ok(None);
    }
    let raw =
        fs::read_to_string(path).with_context(|| format!("读取 watermark 失败: {}", path.display()))?;
    let val = raw.trim();
    if val.is_empty() {
        return Ok(None);
    }
    let day = compact_date(val)?;
    Ok(Some(parse_compact_date(&day)?))
}

fn write_watermark_day(path: &PathBuf, day_compact: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("创建 watermark 目录失败: {}", parent.display()))?;
    }
    fs::write(path, day_compact.as_bytes())
        .with_context(|| format!("写入 watermark 失败: {}", path.display()))?;
    Ok(())
}

fn compact_date(input: &str) -> Result<String> {
    let s = input.trim();
    if s.len() == 8 && s.chars().all(|c| c.is_ascii_digit()) {
        return Ok(s.to_string());
    }
    if s.len() == 10 && s.as_bytes().get(4) == Some(&b'-') && s.as_bytes().get(7) == Some(&b'-') {
        let out = s.replace('-', "");
        if out.len() == 8 && out.chars().all(|c| c.is_ascii_digit()) {
            return Ok(out);
        }
    }
    Err(anyhow!("无效日期格式: {input}，应为 YYYY-MM-DD 或 YYYYMMDD"))
}

fn env_var_any(keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Ok(v) = env::var(key) {
            if !v.trim().is_empty() {
                return Some(v);
            }
        }
    }
    None
}

fn parse_compact_date(input: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(input, "%Y%m%d")
        .with_context(|| format!("无效日期: {input}，应为 YYYYMMDD"))
}

fn format_compact_date(date: NaiveDate) -> String {
    date.format("%Y%m%d").to_string()
}

fn month_windows(start_compact: &str, end_compact: &str) -> Result<Vec<(String, String)>> {
    let start = parse_compact_date(start_compact)?;
    let end = parse_compact_date(end_compact)?;
    if start > end {
        return Err(anyhow!("开始日期不能晚于结束日期: {start_compact} > {end_compact}"));
    }

    let mut out = Vec::new();
    let mut cursor = NaiveDate::from_ymd_opt(start.year(), start.month(), 1)
        .ok_or_else(|| anyhow!("无效月份: {}-{}", start.year(), start.month()))?;

    while cursor <= end {
        let month_start = max(cursor, start);
        let next_month = if cursor.month() == 12 {
            NaiveDate::from_ymd_opt(cursor.year() + 1, 1, 1)
        } else {
            NaiveDate::from_ymd_opt(cursor.year(), cursor.month() + 1, 1)
        }
        .ok_or_else(|| anyhow!("无效下个月日期"))?;
        let month_end = min(next_month.pred_opt().ok_or_else(|| anyhow!("无效月末日期"))?, end);

        out.push((format_compact_date(month_start), format_compact_date(month_end)));
        cursor = next_month;
    }

    Ok(out)
}
