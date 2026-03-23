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
