use std::env;
use std::path::PathBuf;
use std::time::Duration;
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{anyhow, Context, Result};
use clap::{Args, Parser, Subcommand};
use dotenvy::dotenv;
use rust_kline_fetcher::api::ApiClient;
use rust_kline_fetcher::models::{DailyBar, MarketRequest, DEFAULT_TIMEOUT_SECS};
use rust_kline_fetcher::normalize::normalize_full_kline_response;
use rust_kline_fetcher::s3::{
    build_s3_client, ensure_bucket, upload_daily_partition_file, S3Settings,
};
use rust_kline_fetcher::utils::{chunked, load_stock_codes_from_file};
use tokio::sync::mpsc;

#[derive(Debug, Parser)]
#[command(name = "kline-sync", version, about = "A股K线同步命令")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// 按日期区间下载日线并写入 S3 parquet
    SyncDaily(SyncDailyArgs),
}

#[derive(Debug, Args)]
struct SyncDailyArgs {
    #[arg(long, help = "开始日期，YYYY-MM-DD 或 YYYYMMDD")]
    start_date: String,

    #[arg(long, help = "结束日期，YYYY-MM-DD 或 YYYYMMDD")]
    end_date: String,

    #[arg(long, default_value_t = 200)]
    chunk_size: usize,

    #[arg(long)]
    stock_codes_file: Option<PathBuf>,

    #[arg(long, env = "QMT_API_HOST", default_value = "http://127.0.0.1:8000")]
    base_url: String,

    #[arg(long, env = "QMT_API_AUTHORIZATION")]
    authorization: Option<String>,

    #[arg(long, env = "QMT_API_TIMEOUT", default_value_t = DEFAULT_TIMEOUT_SECS)]
    timeout: u64,

    #[arg(long, env = "S3_BUCKET", default_value = "stock")]
    s3_bucket: String,

    #[arg(long, env = "S3_REGION", default_value = "us-east-1")]
    s3_region: String,

    #[arg(long, env = "S3_ACCESS_KEY")]
    s3_access_key: Option<String>,

    #[arg(long, env = "S3_SECRET_KEY")]
    s3_secret_key: Option<String>,

    #[arg(long, help = "S3 endpoint，默认读取 S3_HOST 或 s3_host")]
    s3_host: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let cli = Cli::parse();

    match cli.command {
        Commands::SyncDaily(args) => run_sync_daily(args).await,
    }
}

async fn run_sync_daily(args: SyncDailyArgs) -> Result<()> {
    if args.chunk_size == 0 {
        return Err(anyhow!("--chunk-size 必须大于 0"));
    }

    let start_date = compact_date(&args.start_date)?;
    let end_date = compact_date(&args.end_date)?;

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
    let writer_bucket = bucket_name.clone();

    #[derive(Debug)]
    struct WriteJob {
        batch_no: usize,
        bars: Vec<DailyBar>,
    }

    let (tx, mut rx) = mpsc::channel::<WriteJob>(8);
    let writer = tokio::spawn(async move {
        let mut uploaded_files: BTreeSet<String> = BTreeSet::new();
        let mut written_rows = 0usize;
        let mut partition_cache: BTreeMap<(String, String, String), Vec<DailyBar>> = BTreeMap::new();

        while let Some(job) = rx.recv().await {
            let mut affected: BTreeSet<(String, String, String)> = BTreeSet::new();
            let mut job_rows = 0usize;
            for bar in job.bars {
                if bar.time.len() < 7 {
                    continue;
                }
                let year = bar.time[0..4].to_string();
                let month = bar.time[5..7].to_string();
                let key = (bar.exchange.clone(), year, month);
                partition_cache.entry(key.clone()).or_default().push(bar);
                affected.insert(key);
                written_rows += 1;
                job_rows += 1;
            }

            let mut uploaded_now = 0usize;
            for (exchange, year, month) in affected {
                if let Some(rows) = partition_cache.get(&(exchange.clone(), year.clone(), month.clone())) {
                    let key = upload_daily_partition_file(
                        &s3,
                        &writer_bucket,
                        &exchange,
                        &year,
                        &month,
                        rows,
                    )
                    .await?;
                    uploaded_files.insert(key);
                    uploaded_now += 1;
                }
            }

            println!(
                "[WRITER] 批次 {} 写入 {} 条, 生成 {} 个分区文件, 累计 {} 条/{} 文件",
                job.batch_no,
                job_rows,
                uploaded_now,
                written_rows,
                uploaded_files.len()
            );
        }

        Ok::<(usize, usize), anyhow::Error>((written_rows, uploaded_files.len()))
    });

    let batches = chunked(&stock_codes, args.chunk_size);
    let total = batches.len();

    println!(
        "[INFO] 开始拉取日线: {} 只股票, {} 批, 区间 {} ~ {}",
        stock_codes.len(),
        total,
        start_date,
        end_date
    );

    for (idx, chunk) in batches.into_iter().enumerate() {
        let batch_no = idx + 1;
        let req = MarketRequest::new(chunk, "1d", start_date.clone(), end_date.clone(), "none");
        let resp = api.fetch_market_batch(&req).await?;
        let rows = normalize_full_kline_response(&resp, "1d");

        let mut batch_bars: Vec<DailyBar> = Vec::new();
        for row in &rows {
            if let Some(bar) = DailyBar::from_normalized(row) {
                batch_bars.push(bar);
            }
        }

        println!(
            "[FETCHER] 批次 {}/{}: 解析 {} 条, 转换日线 {} 条",
            batch_no,
            total,
            rows.len(),
            batch_bars.len()
        );

        if !batch_bars.is_empty() {
            tx.send(WriteJob {
                batch_no,
                bars: batch_bars,
            })
            .await
            .map_err(|_| anyhow!("写入队列已关闭"))?;
        }
    }

    drop(tx);
    let (written_rows, uploaded_files) = writer
        .await
        .map_err(|e| anyhow!("writer task join error: {e}"))??;
    println!(
        "[DONE] 上传完成: 累计写入 {} 条日线, {} 个分区文件, bucket={}",
        written_rows, uploaded_files, bucket_name
    );

    Ok(())
}

fn compact_date(input: &str) -> Result<String> {
    let s = input.trim();
    if s.len() == 8 && s.chars().all(|c| c.is_ascii_digit()) {
        return Ok(s.to_string());
    }
    if s.len() == 10 && s.as_bytes().get(4) == Some(&b'-') && s.as_bytes().get(7) == Some(&b'-')
    {
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
