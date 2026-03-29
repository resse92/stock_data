use std::collections::BTreeMap;
use std::env;
use std::fs::File;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use arrow_array::builder::{Float64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use clap::Args;
use csv::StringRecord;
use minio::s3::segmented_bytes::SegmentedBytes;
use minio::s3::types::S3Api;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::s3::{build_s3_client, ensure_bucket, S3Client, S3Settings};

#[derive(Debug, Args)]
pub struct ImportIndexCsvArgs {
    #[arg(long, help = "包含指数 csv 文件的目录")]
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

#[derive(Debug, Clone)]
struct IndexDailyRow {
    symbol: String,
    exchange: String,
    time: String,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    volume: Option<f64>,
    turnover: Option<f64>,
    prev_close: Option<f64>,
}

pub async fn run_import_index_csv(args: ImportIndexCsvArgs) -> Result<()> {
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

    let csv_files = collect_csv_files(&args.input_dir)?;
    if csv_files.is_empty() {
        return Err(anyhow!("目录中未找到 csv 文件: {}", args.input_dir.display()));
    }

    println!(
        "[INFO] 开始导入指数数据: {} 个 csv 文件, input_dir={}",
        csv_files.len(),
        args.input_dir.display()
    );

    let mut groups: BTreeMap<(String, String, String), Vec<IndexDailyRow>> = BTreeMap::new();
    let mut scanned_rows = 0usize;

    for (idx, csv_path) in csv_files.iter().enumerate() {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(File::open(csv_path)?);

        let mut file_rows = 0usize;
        for row in reader.records() {
            let row = row?;
            if let Some(parsed) = parse_index_row(&row) {
                let year = parsed.time[0..4].to_string();
                let month = parsed.time[5..7].to_string();
                groups
                    .entry((parsed.exchange.clone(), year, month))
                    .or_default()
                    .push(parsed);
                file_rows += 1;
            }
        }
        scanned_rows += file_rows;
        println!(
            "[CSV] {}/{} 完成: {}, 解析 {} 条",
            idx + 1,
            csv_files.len(),
            csv_path.display(),
            file_rows
        );
    }

    let mut written_rows = 0usize;
    let mut partition_count = 0usize;
    for ((exchange, year, month), rows) in groups {
        let deduped = dedup_rows(rows);
        if deduped.is_empty() {
            continue;
        }
        upload_index_partition_file(&s3, &s3_settings.bucket, &exchange, &year, &month, &deduped)
            .await
            .with_context(|| {
                format!(
                    "上传分区失败: exchange={}, year={}, month={}",
                    exchange, year, month
                )
            })?;
        written_rows += deduped.len();
        partition_count += 1;
    }

    println!(
        "[DONE] 导入完成: 扫描 {} 条, 写入 {} 条, {} 个分区文件, bucket={}",
        scanned_rows, written_rows, partition_count, s3_settings.bucket
    );

    Ok(())
}

fn collect_csv_files(input_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(input_dir)
        .with_context(|| format!("读取目录失败: {}", input_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path
            .extension()
            .and_then(|s| s.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("csv"))
        {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn parse_index_row(row: &StringRecord) -> Option<IndexDailyRow> {
    let symbol = row.get(0)?.trim();
    let time = row.get(1)?.trim();
    let exchange = symbol.split('.').nth(1)?.trim();
    if exchange.is_empty() || time.len() < 10 {
        return None;
    }

    Some(IndexDailyRow {
        symbol: symbol.to_string(),
        exchange: exchange.to_string(),
        time: time[0..10].to_string(),
        open: parse_opt_f64(row.get(2)),
        high: parse_opt_f64(row.get(3)),
        low: parse_opt_f64(row.get(4)),
        close: parse_opt_f64(row.get(5)),
        volume: parse_opt_f64(row.get(6)),
        turnover: parse_opt_f64(row.get(7)),
        prev_close: parse_opt_f64(row.get(9)),
    })
}

fn parse_opt_f64(v: Option<&str>) -> Option<f64> {
    let raw = v?.trim();
    if raw.is_empty() {
        return None;
    }
    raw.parse::<f64>().ok()
}

fn dedup_rows(rows: Vec<IndexDailyRow>) -> Vec<IndexDailyRow> {
    let mut keyed: BTreeMap<(String, String), IndexDailyRow> = BTreeMap::new();
    for row in rows {
        keyed.insert((row.symbol.clone(), row.time.clone()), row);
    }
    keyed.into_values().collect()
}

async fn upload_index_partition_file(
    s3: &S3Client,
    bucket: &str,
    exchange: &str,
    year: &str,
    month: &str,
    rows: &[IndexDailyRow],
) -> Result<()> {
    let parquet_bytes = to_parquet_bytes(rows)?;
    let key = format!(
        "curated/index_daily_bars/exchange={exchange}/year={year}/month={month}/data.parquet"
    );
    let body = SegmentedBytes::from(Bytes::from(parquet_bytes));
    s3.put_object(bucket, &key, body).send().await?;
    Ok(())
}

fn to_parquet_bytes(rows: &[IndexDailyRow]) -> Result<Vec<u8>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("time", DataType::Utf8, false),
        Field::new("open", DataType::Float64, true),
        Field::new("high", DataType::Float64, true),
        Field::new("low", DataType::Float64, true),
        Field::new("close", DataType::Float64, true),
        Field::new("volume", DataType::Float64, true),
        Field::new("turnover", DataType::Float64, true),
        Field::new("prev_close", DataType::Float64, true),
    ]));

    let mut symbol = StringBuilder::new();
    let mut exchange = StringBuilder::new();
    let mut time = StringBuilder::new();
    let mut open = Float64Builder::new();
    let mut high = Float64Builder::new();
    let mut low = Float64Builder::new();
    let mut close = Float64Builder::new();
    let mut volume = Float64Builder::new();
    let mut turnover = Float64Builder::new();
    let mut prev_close = Float64Builder::new();

    for row in rows {
        symbol.append_value(&row.symbol);
        exchange.append_value(&row.exchange);
        time.append_value(&row.time);
        open.append_option(row.open);
        high.append_option(row.high);
        low.append_option(row.low);
        close.append_option(row.close);
        volume.append_option(row.volume);
        turnover.append_option(row.turnover);
        prev_close.append_option(row.prev_close);
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(symbol.finish()) as ArrayRef,
            Arc::new(exchange.finish()) as ArrayRef,
            Arc::new(time.finish()) as ArrayRef,
            Arc::new(open.finish()) as ArrayRef,
            Arc::new(high.finish()) as ArrayRef,
            Arc::new(low.finish()) as ArrayRef,
            Arc::new(close.finish()) as ArrayRef,
            Arc::new(volume.finish()) as ArrayRef,
            Arc::new(turnover.finish()) as ArrayRef,
            Arc::new(prev_close.finish()) as ArrayRef,
        ],
    )?;

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(128 * 1024)
        .build();
    let mut cursor = Cursor::new(Vec::new());
    {
        let mut writer = ArrowWriter::try_new(&mut cursor, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
    }
    Ok(cursor.into_inner())
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
