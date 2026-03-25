use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow_array::builder::{Float64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use minio::s3::client::{Client, ClientBuilder};
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::segmented_bytes::SegmentedBytes;
use minio::s3::types::S3Api;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::models::DailyBar;

#[derive(Debug, Clone)]
pub struct S3Settings {
    pub endpoint: String,
    pub bucket: String,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub region: String,
}

pub type S3Client = Client;

pub async fn build_s3_client(settings: &S3Settings) -> Result<S3Client> {
    let mut endpoint = settings.endpoint.clone();
    if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
        endpoint = format!("http://{endpoint}");
    }

    let base_url: BaseUrl = endpoint.parse()?;

    let provider = match (&settings.access_key, &settings.secret_key) {
        (Some(ak), Some(sk)) => Some(Box::new(StaticProvider::new(ak, sk, None)) as Box<_>),
        _ => None,
    };

    let client = ClientBuilder::new(base_url)
        .provider(provider)
        .build()?;

    Ok(client)
}

pub async fn ensure_bucket(s3: &S3Client, bucket: &str) -> Result<()> {
    let exists_resp = s3.bucket_exists(bucket).send().await?;
    if exists_resp.exists {
        return Ok(());
    }
    s3.create_bucket(bucket).send().await?;
    Ok(())
}

pub async fn upload_daily_bars_partitions(
    s3: &S3Client,
    settings: &S3Settings,
    bars: &[DailyBar],
) -> Result<Vec<String>> {
    if bars.is_empty() {
        return Ok(vec![]);
    }

    let mut groups: BTreeMap<(String, String, String), Vec<DailyBar>> = BTreeMap::new();

    for bar in bars {
        if bar.time.len() < 7 {
            continue;
        }
        let year = bar.time[0..4].to_string();
        let month = bar.time[5..7].to_string();
        groups
            .entry((bar.exchange.clone(), year, month))
            .or_default()
            .push(bar.clone());
    }

    let mut uploaded_keys = Vec::new();
    for ((exchange, year, month), chunk) in groups {
        let key = upload_daily_partition_file(
            s3,
            &settings.bucket,
            &exchange,
            &year,
            &month,
            &chunk,
        )
        .await?;
        uploaded_keys.push(key);
    }

    Ok(uploaded_keys)
}

pub async fn upload_daily_partition_file(
    s3: &S3Client,
    bucket: &str,
    exchange: &str,
    year: &str,
    month: &str,
    bars: &[DailyBar],
) -> Result<String> {
    let parquet_bytes = to_parquet_bytes(bars)?;
    let key =
        format!("curated/daily_bars/exchange={exchange}/year={year}/month={month}/data.parquet");

    let body = SegmentedBytes::from(Bytes::from(parquet_bytes));
    s3.put_object(bucket, &key, body).send().await?;

    Ok(key)
}

fn to_parquet_bytes(bars: &[DailyBar]) -> Result<Vec<u8>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("time", DataType::Utf8, false),
        Field::new("open", DataType::Float64, true),
        Field::new("high", DataType::Float64, true),
        Field::new("low", DataType::Float64, true),
        Field::new("close", DataType::Float64, true),
        Field::new("volume", DataType::Float64, true),
        Field::new("amount", DataType::Float64, true),
        Field::new("settle", DataType::Float64, true),
        Field::new("openInterest", DataType::Float64, true),
    ]));

    let mut symbol = StringBuilder::new();
    let mut exchange = StringBuilder::new();
    let mut time = StringBuilder::new();
    let mut open = Float64Builder::new();
    let mut high = Float64Builder::new();
    let mut low = Float64Builder::new();
    let mut close = Float64Builder::new();
    let mut volume = Float64Builder::new();
    let mut amount = Float64Builder::new();
    let mut settle = Float64Builder::new();
    let mut open_interest = Float64Builder::new();

    for b in bars {
        symbol.append_value(&b.symbol);
        exchange.append_value(&b.exchange);
        time.append_value(&b.time);
        open.append_option(b.open);
        high.append_option(b.high);
        low.append_option(b.low);
        close.append_option(b.close);
        volume.append_option(b.volume);
        amount.append_option(b.amount);
        settle.append_option(b.settle);
        open_interest.append_option(b.open_interest);
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
            Arc::new(amount.finish()) as ArrayRef,
            Arc::new(settle.finish()) as ArrayRef,
            Arc::new(open_interest.finish()) as ArrayRef,
        ],
    )?;

    let props = WriterProperties::builder().build();
    let mut cursor = Cursor::new(Vec::new());
    {
        let mut writer = ArrowWriter::try_new(&mut cursor, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
    }

    let bytes = cursor.into_inner();
    if bytes.is_empty() {
        return Err(anyhow!("empty parquet bytes"));
    }
    Ok(bytes)
}
