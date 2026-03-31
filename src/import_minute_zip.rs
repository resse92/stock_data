use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::File;
use std::io::{Cursor, Write};
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

use crate::s3::{build_s3_client, ensure_bucket, S3Client, S3Settings};

#[derive(Debug, Args)]
pub struct ImportMinuteZipArgs {
    #[arg(long, help = "分钟 zip 根目录")]
    pub input_dir: PathBuf,

    #[arg(long, default_value_t = 200_000, help = "每个 parquet part 最大行数")]
    pub part_size: usize,

    #[arg(
        long,
        default_value = "meta/ingestion/minute_zip_manifest.txt",
        help = "已完成 zip 清单文件"
    )]
    pub manifest_file: PathBuf,

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
struct MinuteRow {
    symbol: String,
    exchange: String,
    time: String,
    trade_date: String,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    factor: Option<f64>,
    volume: Option<f64>,
    turnover: Option<f64>,
    turnover_rate: Option<f64>,
    is_paused: Option<f64>,
}

pub async fn run_import_minute_zip(args: ImportMinuteZipArgs) -> Result<()> {
    if args.part_size == 0 {
        return Err(anyhow!("--part-size 必须大于 0"));
    }

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

    let zip_files = collect_zip_files_recursive(&args.input_dir)?;
    if zip_files.is_empty() {
        return Err(anyhow!("目录中未找到 zip 文件: {}", args.input_dir.display()));
    }

    let mut imported = load_manifest(&args.manifest_file)?;
    println!(
        "[INFO] 开始导入分钟数据: {} 个 zip, input_dir={}, manifest={}",
        zip_files.len(),
        args.input_dir.display(),
        args.manifest_file.display()
    );

    let mut scanned_rows = 0usize;
    let mut written_rows = 0usize;
    let mut part_files = 0usize;
    let mut skipped = 0usize;

    for (idx, zip_path) in zip_files.iter().enumerate() {
        let manifest_key = zip_manifest_key(zip_path, &args.input_dir)?;
        if imported.contains(&manifest_key) {
            skipped += 1;
            println!(
                "[SKIP] {}/{} 已完成: {}",
                idx + 1,
                zip_files.len(),
                zip_path.display()
            );
            continue;
        }

        let source_id = zip_source_id(zip_path, &args.input_dir);
        let (groups, zip_rows) = parse_zip_groups(zip_path)
            .with_context(|| format!("解析 zip 失败: {}", zip_path.display()))?;
        scanned_rows += zip_rows;

        let mut zip_written_rows = 0usize;
        let mut zip_parts = 0usize;
        for ((trade_date, exchange), rows) in groups {
            let mut deduped = dedup_rows(rows);
            deduped.sort_by(|a, b| a.symbol.cmp(&b.symbol).then(a.time.cmp(&b.time)));

            for (part_idx, chunk) in deduped.chunks(args.part_size).enumerate() {
                upload_minute_part(
                    &s3,
                    &s3_settings.bucket,
                    &trade_date,
                    &exchange,
                    &source_id,
                    part_idx,
                    chunk,
                )
                .await
                .with_context(|| {
                    format!(
                        "上传分区失败: trade_date={}, exchange={}, source={}, part={}",
                        trade_date, exchange, source_id, part_idx
                    )
                })?;
                zip_written_rows += chunk.len();
                zip_parts += 1;
            }
        }

        append_manifest_line(&args.manifest_file, &manifest_key)?;
        imported.insert(manifest_key);

        written_rows += zip_written_rows;
        part_files += zip_parts;

        println!(
            "[ZIP] {}/{} 上传完成: {}, 解析 {} 条, 写入 {} 条, {} 个 part",
            idx + 1,
            zip_files.len(),
            zip_path.display(),
            zip_rows,
            zip_written_rows,
            zip_parts
        );
    }

    println!(
        "[DONE] 导入完成: 扫描 {} 条, 写入 {} 条, {} 个 part 文件, 跳过 {} 个已完成 zip, bucket={}",
        scanned_rows, written_rows, part_files, skipped, s3_settings.bucket
    );
    Ok(())
}

fn parse_zip_groups(zip_path: &Path) -> Result<(BTreeMap<(String, String), Vec<MinuteRow>>, usize)> {
    let file = File::open(zip_path)?;
    let mut archive = ZipArchive::new(file)?;
    let mut groups: BTreeMap<(String, String), Vec<MinuteRow>> = BTreeMap::new();
    let mut row_count = 0usize;

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
        for record in reader.records() {
            let record = record?;
            if let Some(row) = parse_minute_row(&record) {
                groups
                    .entry((row.trade_date.clone(), row.exchange.clone()))
                    .or_default()
                    .push(row);
                row_count += 1;
            }
        }
    }

    Ok((groups, row_count))
}

fn collect_zip_files_recursive(root: &Path) -> Result<Vec<PathBuf>> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
        for entry in
            std::fs::read_dir(dir).with_context(|| format!("读取目录失败: {}", dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            let ft = entry.file_type()?;
            if ft.is_dir() {
                walk(&path, out)?;
                continue;
            }
            if ft.is_file()
                && path
                    .extension()
                    .and_then(|s| s.to_str())
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("zip"))
            {
                out.push(path);
            }
        }
        Ok(())
    }

    let mut files = Vec::new();
    walk(root, &mut files)?;
    files.sort();
    Ok(files)
}

fn parse_minute_row(row: &StringRecord) -> Option<MinuteRow> {
    let symbol = row.get(0)?.trim();
    let time = row.get(1)?.trim();
    let exchange = symbol.split('.').nth(1)?.trim();
    if exchange.is_empty() || time.len() < 19 {
        return None;
    }
    let trade_date = time[0..10].to_string();

    Some(MinuteRow {
        symbol: symbol.to_string(),
        exchange: exchange.to_string(),
        time: time[0..19].to_string(),
        trade_date,
        open: parse_opt_f64(row.get(2)),
        high: parse_opt_f64(row.get(3)),
        low: parse_opt_f64(row.get(4)),
        close: parse_opt_f64(row.get(5)),
        factor: parse_opt_f64(row.get(6)),
        volume: parse_opt_f64(row.get(7)),
        turnover: parse_opt_f64(row.get(8)),
        turnover_rate: parse_opt_f64(row.get(9)),
        is_paused: parse_opt_f64(row.get(10)),
    })
}

fn parse_opt_f64(v: Option<&str>) -> Option<f64> {
    let raw = v?.trim();
    if raw.is_empty() {
        return None;
    }
    raw.parse::<f64>().ok()
}

fn dedup_rows(rows: Vec<MinuteRow>) -> Vec<MinuteRow> {
    let mut keyed: BTreeMap<(String, String), MinuteRow> = BTreeMap::new();
    for row in rows {
        keyed.insert((row.symbol.clone(), row.time.clone()), row);
    }
    keyed.into_values().collect()
}

async fn upload_minute_part(
    s3: &S3Client,
    bucket: &str,
    trade_date: &str,
    exchange: &str,
    source_id: &str,
    part_idx: usize,
    rows: &[MinuteRow],
) -> Result<()> {
    let parquet_bytes = to_parquet_bytes(rows)?;
    let key = format!(
        "curated/minute_bars_1m/trade_date={trade_date}/exchange={exchange}/source={source_id}/part-{part_idx:03}.parquet"
    );
    let body = SegmentedBytes::from(Bytes::from(parquet_bytes));
    s3.put_object(bucket, &key, body).send().await?;
    Ok(())
}

fn to_parquet_bytes(rows: &[MinuteRow]) -> Result<Vec<u8>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("time", DataType::Utf8, false),
        Field::new("trade_date", DataType::Utf8, false),
        Field::new("open", DataType::Float64, true),
        Field::new("high", DataType::Float64, true),
        Field::new("low", DataType::Float64, true),
        Field::new("close", DataType::Float64, true),
        Field::new("factor", DataType::Float64, true),
        Field::new("volume", DataType::Float64, true),
        Field::new("turnover", DataType::Float64, true),
        Field::new("turnover_rate", DataType::Float64, true),
        Field::new("is_paused", DataType::Float64, true),
    ]));

    let mut symbol = StringBuilder::new();
    let mut exchange = StringBuilder::new();
    let mut time = StringBuilder::new();
    let mut trade_date = StringBuilder::new();
    let mut open = Float64Builder::new();
    let mut high = Float64Builder::new();
    let mut low = Float64Builder::new();
    let mut close = Float64Builder::new();
    let mut factor = Float64Builder::new();
    let mut volume = Float64Builder::new();
    let mut turnover = Float64Builder::new();
    let mut turnover_rate = Float64Builder::new();
    let mut is_paused = Float64Builder::new();

    for row in rows {
        symbol.append_value(&row.symbol);
        exchange.append_value(&row.exchange);
        time.append_value(&row.time);
        trade_date.append_value(&row.trade_date);
        open.append_option(row.open);
        high.append_option(row.high);
        low.append_option(row.low);
        close.append_option(row.close);
        factor.append_option(row.factor);
        volume.append_option(row.volume);
        turnover.append_option(row.turnover);
        turnover_rate.append_option(row.turnover_rate);
        is_paused.append_option(row.is_paused);
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(symbol.finish()) as ArrayRef,
            Arc::new(exchange.finish()) as ArrayRef,
            Arc::new(time.finish()) as ArrayRef,
            Arc::new(trade_date.finish()) as ArrayRef,
            Arc::new(open.finish()) as ArrayRef,
            Arc::new(high.finish()) as ArrayRef,
            Arc::new(low.finish()) as ArrayRef,
            Arc::new(close.finish()) as ArrayRef,
            Arc::new(factor.finish()) as ArrayRef,
            Arc::new(volume.finish()) as ArrayRef,
            Arc::new(turnover.finish()) as ArrayRef,
            Arc::new(turnover_rate.finish()) as ArrayRef,
            Arc::new(is_paused.finish()) as ArrayRef,
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

fn zip_source_id(zip_path: &Path, input_dir: &Path) -> String {
    let relative = zip_path
        .strip_prefix(input_dir)
        .unwrap_or(zip_path)
        .to_string_lossy()
        .replace(std::path::MAIN_SEPARATOR, "_");
    sanitize_for_s3_component(&relative)
}

fn zip_manifest_key(zip_path: &Path, input_dir: &Path) -> Result<String> {
    let meta = std::fs::metadata(zip_path)?;
    let size = meta.len();
    let mtime_secs = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or_default();
    let relative = zip_path
        .strip_prefix(input_dir)
        .unwrap_or(zip_path)
        .to_string_lossy();
    Ok(format!("{}\t{}\t{}", relative, size, mtime_secs))
}

fn sanitize_for_s3_component(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for c in input.chars() {
        if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
            out.push(c);
        } else {
            out.push('_');
        }
    }
    while out.contains("__") {
        out = out.replace("__", "_");
    }
    out.trim_matches('_').to_string()
}

fn load_manifest(path: &Path) -> Result<BTreeSet<String>> {
    if !path.exists() {
        return Ok(BTreeSet::new());
    }
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("读取 manifest 失败: {}", path.display()))?;
    let mut out = BTreeSet::new();
    for line in raw.lines() {
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            out.insert(trimmed.to_string());
        }
    }
    Ok(out)
}

fn append_manifest_line(path: &Path, key: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("创建 manifest 目录失败: {}", parent.display()))?;
    }
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("打开 manifest 失败: {}", path.display()))?;
    writeln!(f, "{key}")
        .with_context(|| format!("写入 manifest 失败: {}", path.display()))?;
    Ok(())
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

