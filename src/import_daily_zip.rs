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
use zip::ZipArchive;

use crate::s3::{build_s3_client, ensure_bucket, S3Settings};

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
            upload_daily_partition_file_csv_full(
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

fn parse_zip_to_partitions(
    zip_path: &Path,
) -> Result<BTreeMap<(String, String, String), Vec<CsvDailyRow>>> {
    let file = File::open(zip_path)?;
    let mut archive = ZipArchive::new(file)?;

    let mut groups: BTreeMap<(String, String, String), Vec<CsvDailyRow>> = BTreeMap::new();

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
                let exchange = bar
                    .symbol
                    .split('.')
                    .nth(1)
                    .map(|x| x.to_string())
                    .unwrap_or_default();
                let year = bar.time[0..4].to_string();
                let month = bar.time[5..7].to_string();
                groups
                    .entry((exchange, year, month))
                    .or_default()
                    .push(bar);
            }
        }
    }

    Ok(groups)
}

#[derive(Debug, Clone)]
struct CsvDailyRow {
    symbol: String,
    time: String,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    volume: Option<f64>,
    turnover: Option<f64>,
    factor: Option<f64>,
    prev_close: Option<f64>,
    avg_price: Option<f64>,
    high_limit: Option<f64>,
    low_limit: Option<f64>,
    turnover_rate: Option<f64>,
    amp_rate: Option<f64>,
    quote_rate: Option<f64>,
    is_paused: Option<f64>,
    is_st: Option<f64>,
}

fn parse_daily_bar_row(row: &StringRecord) -> Option<CsvDailyRow> {
    let symbol = row.get(0)?.trim();
    let time = row.get(1)?.trim();

    if symbol.split('.').nth(1).is_none() {
        return None;
    }
    if time.len() < 10 {
        return None;
    }

    Some(CsvDailyRow {
        symbol: symbol.to_string(),
        time: time[0..10].to_string(),
        open: parse_opt_f64(row.get(2)),
        high: parse_opt_f64(row.get(3)),
        low: parse_opt_f64(row.get(4)),
        close: parse_opt_f64(row.get(5)),
        volume: parse_opt_f64(row.get(6)),
        turnover: parse_opt_f64(row.get(7)),
        factor: parse_opt_f64(row.get(8)),
        prev_close: parse_opt_f64(row.get(9)),
        avg_price: parse_opt_f64(row.get(10)),
        high_limit: parse_opt_f64(row.get(11)),
        low_limit: parse_opt_f64(row.get(12)),
        turnover_rate: parse_opt_f64(row.get(13)),
        amp_rate: parse_opt_f64(row.get(14)),
        quote_rate: parse_opt_f64(row.get(15)),
        is_paused: parse_opt_f64(row.get(16)),
        is_st: parse_opt_f64(row.get(17)),
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

async fn upload_daily_partition_file_csv_full(
    s3: &crate::s3::S3Client,
    bucket: &str,
    exchange: &str,
    year: &str,
    month: &str,
    rows: &[CsvDailyRow],
) -> Result<()> {
    let parquet_bytes = to_parquet_bytes_csv_full(rows)?;
    let key =
        format!("curated/daily_bars/exchange={exchange}/year={year}/month={month}/data.parquet");
    let body = SegmentedBytes::from(Bytes::from(parquet_bytes));
    s3.put_object(bucket, &key, body).send().await?;
    Ok(())
}

fn to_parquet_bytes_csv_full(rows: &[CsvDailyRow]) -> Result<Vec<u8>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("time", DataType::Utf8, false),
        Field::new("open", DataType::Float64, true),
        Field::new("high", DataType::Float64, true),
        Field::new("low", DataType::Float64, true),
        Field::new("close", DataType::Float64, true),
        Field::new("volume", DataType::Float64, true),
        Field::new("turnover", DataType::Float64, true),
        Field::new("factor", DataType::Float64, true),
        Field::new("prev_close", DataType::Float64, true),
        Field::new("avg_price", DataType::Float64, true),
        Field::new("high_limit", DataType::Float64, true),
        Field::new("low_limit", DataType::Float64, true),
        Field::new("turnover_rate", DataType::Float64, true),
        Field::new("amp_rate", DataType::Float64, true),
        Field::new("quote_rate", DataType::Float64, true),
        Field::new("is_paused", DataType::Float64, true),
        Field::new("is_st", DataType::Float64, true),
    ]));

    let mut symbol = StringBuilder::new();
    let mut time = StringBuilder::new();
    let mut open = Float64Builder::new();
    let mut high = Float64Builder::new();
    let mut low = Float64Builder::new();
    let mut close = Float64Builder::new();
    let mut volume = Float64Builder::new();
    let mut turnover = Float64Builder::new();
    let mut factor = Float64Builder::new();
    let mut prev_close = Float64Builder::new();
    let mut avg_price = Float64Builder::new();
    let mut high_limit = Float64Builder::new();
    let mut low_limit = Float64Builder::new();
    let mut turnover_rate = Float64Builder::new();
    let mut amp_rate = Float64Builder::new();
    let mut quote_rate = Float64Builder::new();
    let mut is_paused = Float64Builder::new();
    let mut is_st = Float64Builder::new();

    for row in rows {
        symbol.append_value(&row.symbol);
        time.append_value(&row.time);
        open.append_option(row.open);
        high.append_option(row.high);
        low.append_option(row.low);
        close.append_option(row.close);
        volume.append_option(row.volume);
        turnover.append_option(row.turnover);
        factor.append_option(row.factor);
        prev_close.append_option(row.prev_close);
        avg_price.append_option(row.avg_price);
        high_limit.append_option(row.high_limit);
        low_limit.append_option(row.low_limit);
        turnover_rate.append_option(row.turnover_rate);
        amp_rate.append_option(row.amp_rate);
        quote_rate.append_option(row.quote_rate);
        is_paused.append_option(row.is_paused);
        is_st.append_option(row.is_st);
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(symbol.finish()) as ArrayRef,
            Arc::new(time.finish()) as ArrayRef,
            Arc::new(open.finish()) as ArrayRef,
            Arc::new(high.finish()) as ArrayRef,
            Arc::new(low.finish()) as ArrayRef,
            Arc::new(close.finish()) as ArrayRef,
            Arc::new(volume.finish()) as ArrayRef,
            Arc::new(turnover.finish()) as ArrayRef,
            Arc::new(factor.finish()) as ArrayRef,
            Arc::new(prev_close.finish()) as ArrayRef,
            Arc::new(avg_price.finish()) as ArrayRef,
            Arc::new(high_limit.finish()) as ArrayRef,
            Arc::new(low_limit.finish()) as ArrayRef,
            Arc::new(turnover_rate.finish()) as ArrayRef,
            Arc::new(amp_rate.finish()) as ArrayRef,
            Arc::new(quote_rate.finish()) as ArrayRef,
            Arc::new(is_paused.finish()) as ArrayRef,
            Arc::new(is_st.finish()) as ArrayRef,
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
