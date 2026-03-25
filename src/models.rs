use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const DEFAULT_TIMEOUT_SECS: u64 = 30;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketRequest {
    pub stock_codes: Vec<String>,
    pub start_date: String,
    pub end_date: String,
    pub period: String,
    pub adjust_type: String,
    pub fill_data: bool,
    pub disable_download: bool,
}

impl MarketRequest {
    pub fn new(
        stock_codes: Vec<String>,
        period: impl Into<String>,
        start_date: impl Into<String>,
        end_date: impl Into<String>,
        adjust_type: impl Into<String>,
    ) -> Self {
        Self {
            stock_codes,
            period: period.into(),
            start_date: start_date.into(),
            end_date: end_date.into(),
            adjust_type: adjust_type.into(),
            fill_data: true,
            disable_download: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedBar {
    pub stock_code: String,
    pub period: String,
    pub ts_raw: Option<String>,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: Option<f64>,
    pub volume: Option<f64>,
    pub amount: Option<f64>,
    pub turnover_rate: Option<f64>,
    pub open_interest: Option<f64>,
    pub settle: Option<f64>,
    pub adj_factor: Option<f64>,
    pub extra_json: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickQuote {
    pub symbol: String,
    pub exchange: String,
    pub time: String,
    pub last_price: Option<f64>,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub last_close: Option<f64>,
    pub amount: Option<f64>,
    pub volume: Option<f64>,
    pub pvolume: Option<f64>,
    pub stock_status: Option<String>,
    pub open_interest: Option<f64>,
    pub last_settlement_price: Option<f64>,
    pub ask_price_1: Option<f64>,
    pub ask_price_2: Option<f64>,
    pub ask_price_3: Option<f64>,
    pub ask_price_4: Option<f64>,
    pub ask_price_5: Option<f64>,
    pub bid_price_1: Option<f64>,
    pub bid_price_2: Option<f64>,
    pub bid_price_3: Option<f64>,
    pub bid_price_4: Option<f64>,
    pub bid_price_5: Option<f64>,
    pub ask_vol_1: Option<f64>,
    pub ask_vol_2: Option<f64>,
    pub ask_vol_3: Option<f64>,
    pub ask_vol_4: Option<f64>,
    pub ask_vol_5: Option<f64>,
    pub bid_vol_1: Option<f64>,
    pub bid_vol_2: Option<f64>,
    pub bid_vol_3: Option<f64>,
    pub bid_vol_4: Option<f64>,
    pub bid_vol_5: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyBar {
    pub symbol: String,
    pub exchange: String,
    pub time: String, // YYYY-MM-DD
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: Option<f64>,
    pub volume: Option<f64>,
    pub amount: Option<f64>,
    pub settle: Option<f64>,
    pub open_interest: Option<f64>,
    pub source: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinuteBar1m {
    pub symbol: String,
    pub exchange: String,
    pub time: String, // YYYY-MM-DD HH:MM:SS
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: Option<f64>,
    pub volume: Option<f64>,
    pub amount: Option<f64>,
    pub settle: Option<f64>,
    pub open_interest: Option<f64>,
    pub source: Option<String>,
}

pub fn exchange_from_symbol(symbol: &str) -> Option<String> {
    symbol.split('.').nth(1).map(|x| x.to_string())
}

pub fn date_from_ts_raw(ts_raw: &str) -> Option<String> {
    let s = ts_raw.trim();
    if s.len() >= 10 && s.as_bytes().get(4) == Some(&b'-') && s.as_bytes().get(7) == Some(&b'-')
    {
        return Some(s[..10].to_string());
    }
    if s.len() >= 8 && s.chars().take(8).all(|c| c.is_ascii_digit()) {
        return Some(format!("{}-{}-{}", &s[0..4], &s[4..6], &s[6..8]));
    }
    None
}

impl DailyBar {
    pub fn from_normalized(raw: &NormalizedBar) -> Option<Self> {
        if raw.period != "1d" {
            return None;
        }
        let exchange = exchange_from_symbol(&raw.stock_code)?;
        let ts = raw.ts_raw.as_deref()?;
        let date = date_from_ts_raw(ts)?;
        Some(Self {
            symbol: raw.stock_code.clone(),
            exchange,
            time: date,
            open: raw.open,
            high: raw.high,
            low: raw.low,
            close: raw.close,
            volume: raw.volume,
            amount: raw.amount,
            settle: raw.settle,
            open_interest: raw.open_interest,
            source: None,
        })
    }
}

impl MinuteBar1m {
    pub fn from_normalized(raw: &NormalizedBar) -> Option<Self> {
        if raw.period != "1m" {
            return None;
        }
        let exchange = exchange_from_symbol(&raw.stock_code)?;
        let ts = raw.ts_raw.as_deref()?;
        Some(Self {
            symbol: raw.stock_code.clone(),
            exchange,
            time: ts.to_string(),
            open: raw.open,
            high: raw.high,
            low: raw.low,
            close: raw.close,
            volume: raw.volume,
            amount: raw.amount,
            settle: raw.settle,
            open_interest: raw.open_interest,
            source: None,
        })
    }
}
