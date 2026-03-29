use std::collections::BTreeMap;
use std::env;
use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use clap::Args;
use csv::StringRecord;
use zip::ZipArchive;

use crate::models::DailyBar;
use crate::s3::{build_s3_client, ensure_bucket, upload_daily_partition_file, S3Settings};

#[derive(Debug, Args)]
pub struct ImportDailyZipArgs {
    #[arg(long, default_value = "日线数据", help = "包含 zip 文件的目录")]
    pub input_dir: PathBuf,

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

pub async fn run_import_daily_zip(args: ImportDailyZipArgs) -> Result<()> {
    let s3_host = args
        .s3_host
        .or_else(|| env_var_any(&["S3_HOST", "s3_host"]))
        .ok_or_else(|| anyhow!("缺少 S3 host，请在 .env 设置 s3_host 或 S3_HOST"))?;

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

    let zip_files = collect_zip_files(&args.input_dir)?;
    if zip_files.is_empty() {
        return Err(anyhow!("目录中未找到 zip 文件: {}", args.input_dir.display()));
    }

    println!(
        "[INFO] 开始导入: {} 个 zip 文件, input_dir={}",
        zip_files.len(),
        args.input_dir.display()
    );

    let mut total_rows = 0usize;
    let mut total_partitions = 0usize;

    for (idx, zip_path) in zip_files.iter().enumerate() {
        let partitions = parse_zip_to_partitions(zip_path)
            .with_context(|| format!("解析 zip 失败: {}", zip_path.display()))?;

        let mut uploaded = 0usize;
        for ((exchange, year, month), rows) in partitions {
            if rows.is_empty() {
                continue;
            }
            upload_daily_partition_file(
                &s3,
                &s3_settings.bucket,
                &exchange,
                &year,
                &month,
                &rows,
            )
            .await
            .with_context(|| {
                format!(
                    "上传分区失败: exchange={}, year={}, month={}, zip={}",
                    exchange,
                    year,
                    month,
                    zip_path.display()
                )
            })?;
            total_rows += rows.len();
            total_partitions += 1;
            uploaded += 1;
        }

        println!(
            "[ZIP] {}/{} 完成: {}, 上传 {} 个分区",
            idx + 1,
            zip_files.len(),
            zip_path.display(),
            uploaded
        );
    }

    println!(
        "[DONE] 导入完成: {} 条日线, {} 个分区文件, bucket={}",
        total_rows, total_partitions, s3_settings.bucket
    );

    Ok(())
}

fn collect_zip_files(input_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(input_dir)
        .with_context(|| format!("读取目录失败: {}", input_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path
            .extension()
            .and_then(|s| s.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("zip"))
        {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn parse_zip_to_partitions(zip_path: &Path) -> Result<BTreeMap<(String, String, String), Vec<DailyBar>>> {
    let file = File::open(zip_path)?;
    let mut archive = ZipArchive::new(file)?;

    let mut groups: BTreeMap<(String, String, String), Vec<DailyBar>> = BTreeMap::new();

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        if !entry.is_file() {
            continue;
        }
        if !entry
            .name()
            .rsplit('.')
            .next()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("csv"))
        {
            continue;
        }

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(&mut entry);

        for row in reader.records() {
            let row = row?;
            if let Some(bar) = parse_daily_bar_row(&row) {
                let year = bar.time[0..4].to_string();
                let month = bar.time[5..7].to_string();
                groups
                    .entry((bar.exchange.clone(), year, month))
                    .or_default()
                    .push(bar);
            }
        }
    }

    Ok(groups)
}

fn parse_daily_bar_row(row: &StringRecord) -> Option<DailyBar> {
    let ts_code = row.get(0)?.trim();
    let trade_date = row.get(1)?.trim();

    let exchange = ts_code.split('.').nth(1)?.trim();
    if trade_date.len() < 10 {
        return None;
    }

    Some(DailyBar {
        symbol: ts_code.to_string(),
        exchange: exchange.to_string(),
        time: trade_date[0..10].to_string(),
        open: parse_opt_f64(row.get(2)),
        high: parse_opt_f64(row.get(3)),
        low: parse_opt_f64(row.get(4)),
        close: parse_opt_f64(row.get(5)),
        volume: parse_opt_f64(row.get(6)),
        amount: parse_opt_f64(row.get(7)),
        settle: parse_opt_f64(row.get(9)),
        open_interest: None,
        source: None,
    })
}

fn parse_opt_f64(v: Option<&str>) -> Option<f64> {
    let raw = v?.trim();
    if raw.is_empty() {
        return None;
    }
    raw.parse::<f64>().ok()
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
