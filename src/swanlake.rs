use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::Result;
use chrono::Utc;
use serde::Serialize;
use serde_json::Value;

use crate::models::{date_from_ts_raw, DailyBar, MinuteBar1m, NormalizedBar};

#[derive(Debug, Clone)]
pub struct SwanLakeConfig {
    pub root: PathBuf,
}

impl SwanLakeConfig {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn raw_root(&self) -> PathBuf {
        self.root.join("raw")
    }

    pub fn curated_root(&self) -> PathBuf {
        self.root.join("curated")
    }
}

#[derive(Debug, Clone)]
pub struct SwanLakeSink {
    cfg: SwanLakeConfig,
}

impl SwanLakeSink {
    pub fn new(cfg: SwanLakeConfig) -> Self {
        Self { cfg }
    }

    pub fn write_raw_batch(
        &self,
        vendor: &str,
        data_type: &str,
        trade_date: &str,
        batch_no: usize,
        payload: &Value,
    ) -> Result<PathBuf> {
        let dir = self
            .cfg
            .raw_root()
            .join(format!("vendor={vendor}"))
            .join(format!("type={data_type}"))
            .join(format!("trade_date={trade_date}"));
        fs::create_dir_all(&dir)?;
        let file_path = dir.join(format!("batch_{batch_no:05}.json"));
        fs::write(&file_path, serde_json::to_vec_pretty(payload)?)?;
        Ok(file_path)
    }

    pub fn write_daily_bars(&self, bars: &[DailyBar]) -> Result<Vec<PathBuf>> {
        let mut groups: BTreeMap<(String, String, String), Vec<&DailyBar>> = BTreeMap::new();
        for bar in bars {
            let year = bar.time.get(0..4).unwrap_or("0000").to_string();
            let month = bar.time.get(5..7).unwrap_or("00").to_string();
            groups
                .entry((bar.exchange.clone(), year, month))
                .or_default()
                .push(bar);
        }

        let mut outputs = Vec::new();
        for ((exchange, year, month), chunk) in groups {
            let dir = self
                .cfg
                .curated_root()
                .join("daily_bars")
                .join(format!("exchange={exchange}"))
                .join(format!("year={year}"))
                .join(format!("month={month}"));
            fs::create_dir_all(&dir)?;
            let path = dir.join(part_file_name());
            write_jsonl(&path, &chunk)?;
            outputs.push(path);
        }

        Ok(outputs)
    }

    pub fn write_minute_bars_1m(&self, bars: &[MinuteBar1m]) -> Result<Vec<PathBuf>> {
        let mut groups: BTreeMap<(String, String), Vec<&MinuteBar1m>> = BTreeMap::new();
        for bar in bars {
            if let Some(trade_date) = date_from_ts_raw(&bar.time) {
                groups
                    .entry((trade_date, bar.exchange.clone()))
                    .or_default()
                    .push(bar);
            }
        }

        let mut outputs = Vec::new();
        for ((trade_date, exchange), chunk) in groups {
            let dir = self
                .cfg
                .curated_root()
                .join("minute_bars_1m")
                .join(format!("trade_date={trade_date}"))
                .join(format!("exchange={exchange}"));
            fs::create_dir_all(&dir)?;
            let path = dir.join(part_file_name());
            write_jsonl(&path, &chunk)?;
            outputs.push(path);
        }

        Ok(outputs)
    }

    pub fn write_normalized_bars(&self, bars: &[NormalizedBar]) -> Result<(Vec<PathBuf>, Vec<PathBuf>)> {
        let daily: Vec<DailyBar> = bars.iter().filter_map(DailyBar::from_normalized).collect();
        let minute: Vec<MinuteBar1m> = bars
            .iter()
            .filter_map(MinuteBar1m::from_normalized)
            .collect();

        let daily_files = self.write_daily_bars(&daily)?;
        let minute_files = self.write_minute_bars_1m(&minute)?;
        Ok((daily_files, minute_files))
    }
}

fn part_file_name() -> String {
    let ts = Utc::now().format("%Y%m%d%H%M%S");
    format!("part-{ts}.jsonl")
}

fn write_jsonl<T: Serialize>(path: &Path, rows: &[T]) -> Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;
    let mut w = BufWriter::new(file);
    for row in rows {
        serde_json::to_writer(&mut w, row)?;
        w.write_all(b"\n")?;
    }
    w.flush()?;
    Ok(())
}
