use std::collections::BTreeSet;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use reqwest::{Client, Method};
use serde_json::{json, Value};

use crate::models::MarketRequest;

#[derive(Debug, Clone)]
pub struct ApiClient {
    base_url: String,
    authorization: Option<String>,
    client: Client,
}

impl ApiClient {
    pub fn new(
        base_url: impl Into<String>,
        authorization: Option<String>,
        timeout: Duration,
    ) -> Result<Self> {
        let client = Client::builder().timeout(timeout).build()?;
        let authorization = authorization.map(|v| {
            if v.to_ascii_lowercase().starts_with("bearer ") {
                v
            } else {
                format!("Bearer {v}")
            }
        });
        Ok(Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            authorization,
            client,
        })
    }

    pub async fn request_json(
        &self,
        method: Method,
        path: &str,
        payload: Option<Value>,
    ) -> Result<Value> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self
            .client
            .request(method.clone(), &url)
            .header(ACCEPT, "application/json");

        if let Some(auth) = &self.authorization {
            req = req.header(AUTHORIZATION, auth);
        }

        if let Some(body) = payload {
            req = req.header(CONTENT_TYPE, "application/json").json(&body);
        }

        let resp = req
            .send()
            .await
            .with_context(|| format!("request failed: {method} {path}"))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();

        if !status.is_success() {
            return Err(anyhow!(
                "HTTP {} calling {} {}: {}",
                status,
                method,
                path,
                text
            ));
        }

        if text.trim().is_empty() {
            return Ok(Value::Null);
        }

        serde_json::from_str(&text)
            .with_context(|| format!("invalid json from {} {}: {}", method, path, text))
    }

    pub async fn fetch_market_batch(&self, req: &MarketRequest) -> Result<Value> {
        self.request_json(
            Method::POST,
            "/api/v1/data/market",
            Some(serde_json::to_value(req)?),
        )
        .await
    }

    pub async fn fetch_sectors(&self) -> Result<Value> {
        self.request_json(Method::GET, "/api/v1/data/sectors", None)
            .await
    }

    pub async fn fetch_sector_stocks(&self, sector_name: &str) -> Result<Value> {
        self.request_json(
            Method::POST,
            "/api/v1/data/sector",
            Some(json!({ "sector_name": sector_name })),
        )
        .await
    }

    pub async fn discover_all_stock_codes(&self) -> Result<Vec<String>> {
        let sector_name = "沪深A股";
        let v = self
            .fetch_sector_stocks(sector_name)
            .await
            .with_context(|| format!("调用 /api/v1/data/sector 失败: {sector_name}"))?;
        let codes: BTreeSet<String> = extract_stock_list(&v)
            .into_iter()
            .filter(|code| is_hsba_a_share(code))
            .collect();
        if codes.is_empty() {
            return Err(anyhow!("板块 {sector_name} 未返回任何沪深京A股代码"));
        }
        Ok(codes.into_iter().collect())
    }
}

fn is_hsba_a_share(code: &str) -> bool {
    let trimmed = code.trim();
    if trimmed.is_empty() {
        return false;
    }

    let mut exchange = None;
    let mut symbol = trimmed;
    if let Some((left, right)) = trimmed.rsplit_once('.') {
        symbol = left.trim();
        exchange = Some(right.trim().to_ascii_uppercase());
    }

    let digits: String = symbol.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.len() < 6 {
        return false;
    }
    let d6 = &digits[..6];

    let sh_a = d6.starts_with("600")
        || d6.starts_with("601")
        || d6.starts_with("603")
        || d6.starts_with("605")
        || d6.starts_with("688")
        || d6.starts_with("689");
    let sz_a = d6.starts_with("000")
        || d6.starts_with("001")
        || d6.starts_with("002")
        || d6.starts_with("003")
        || d6.starts_with("300")
        || d6.starts_with("301");
    let bj_a = d6.starts_with("430")
        || d6.starts_with("440")
        || d6.starts_with("830")
        || d6.starts_with("831")
        || d6.starts_with("832")
        || d6.starts_with("833")
        || d6.starts_with("834")
        || d6.starts_with("835")
        || d6.starts_with("836")
        || d6.starts_with("837")
        || d6.starts_with("838")
        || d6.starts_with("839")
        || d6.starts_with("870")
        || d6.starts_with("871")
        || d6.starts_with("872")
        || d6.starts_with("873")
        || d6.starts_with("874")
        || d6.starts_with("875")
        || d6.starts_with("876")
        || d6.starts_with("877")
        || d6.starts_with("878")
        || d6.starts_with("879")
        || d6.starts_with("880")
        || d6.starts_with("881")
        || d6.starts_with("882")
        || d6.starts_with("883")
        || d6.starts_with("884")
        || d6.starts_with("885")
        || d6.starts_with("886")
        || d6.starts_with("887")
        || d6.starts_with("888")
        || d6.starts_with("920");

    match exchange.as_deref() {
        Some("SH") => sh_a,
        Some("SZ") => sz_a,
        Some("BJ") => bj_a,
        Some(_) => false,
        None => sh_a || sz_a || bj_a,
    }
}

fn extract_stock_list(v: &Value) -> Vec<String> {
    match v {
        Value::Null => vec![],
        Value::Array(arr) => {
            let mut out = Vec::new();
            for item in arr {
                match item {
                    Value::String(s) => out.push(s.to_string()),
                    Value::Object(obj) => {
                        if let Some(Value::Array(codes)) = obj.get("stock_list") {
                            for c in codes {
                                if let Some(s) = c.as_str() {
                                    out.push(s.to_string());
                                }
                            }
                        } else if let Some(s) = obj.get("stock_code").and_then(|x| x.as_str()) {
                            out.push(s.to_string());
                        }
                    }
                    _ => {}
                }
            }
            out
        }
        Value::Object(obj) => {
            if let Some(Value::Array(codes)) = obj.get("stock_list") {
                return codes
                    .iter()
                    .filter_map(|x| x.as_str().map(|s| s.to_string()))
                    .collect();
            }
            for key in ["data", "result", "items"] {
                if let Some(nested) = obj.get(key) {
                    let inner = extract_stock_list(nested);
                    if !inner.is_empty() {
                        return inner;
                    }
                }
            }
            vec![]
        }
        _ => vec![],
    }
}
