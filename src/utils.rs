use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{anyhow, Context, Result};

pub fn chunked(codes: &[String], size: usize) -> Vec<Vec<String>> {
    let mut out = Vec::new();
    let mut idx = 0;
    while idx < codes.len() {
        let end = (idx + size).min(codes.len());
        out.push(codes[idx..end].to_vec());
        idx = end;
    }
    out
}

pub fn format_seconds(seconds: f64) -> String {
    let total = seconds.max(0.0) as u64;
    let sec = total % 60;
    let minutes_total = total / 60;
    let min = minutes_total % 60;
    let hour = minutes_total / 60;
    if hour > 0 {
        format!("{hour:02}:{min:02}:{sec:02}")
    } else {
        format!("{min:02}:{sec:02}")
    }
}

pub fn load_stock_codes_from_file(path: &Path) -> Result<Vec<String>> {
    let file = File::open(path)
        .with_context(|| format!("stock code file not found: {}", path.display()))?;

    let mut out = BTreeSet::new();
    for line in BufReader::new(file).lines() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        out.insert(line.to_string());
    }

    if out.is_empty() {
        return Err(anyhow!("stock code file is empty"));
    }

    Ok(out.into_iter().collect())
}
