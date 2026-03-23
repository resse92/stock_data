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
        let broad_candidates = ["沪深京A股"];

        for sector_name in broad_candidates {
            match self.fetch_sector_stocks(sector_name).await {
                Ok(v) => {
                    let codes: BTreeSet<String> = extract_stock_list(&v).into_iter().collect();
                    if !codes.is_empty() {
                        return Ok(codes.into_iter().collect());
                    }
                }
                Err(_) => continue,
            }
        }

        let sector_resp = self
            .fetch_sectors()
            .await
            .context("调用 /api/v1/data/sectors 失败")?;
        let sector_names = extract_sector_names(&sector_resp);
        if sector_names.is_empty() {
            return Err(anyhow!("无法从 /api/v1/data/sectors 推导板块列表"));
        }

        let mut all_codes = BTreeSet::new();
        for sector_name in sector_names {
            if let Ok(v) = self.fetch_sector_stocks(&sector_name).await {
                for code in extract_stock_list(&v) {
                    all_codes.insert(code);
                }
            }
        }

        if all_codes.is_empty() {
            return Err(anyhow!("遍历板块后仍未获取到任何股票代码"));
        }

        Ok(all_codes.into_iter().collect())
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

fn extract_sector_names(v: &Value) -> Vec<String> {
    match v {
        Value::Null => vec![],
        Value::Array(arr) => {
            let mut out = Vec::new();
            for item in arr {
                match item {
                    Value::String(s) => out.push(s.to_string()),
                    Value::Object(obj) => {
                        if let Some(name) = obj
                            .get("sector_name")
                            .or_else(|| obj.get("name"))
                            .and_then(|x| x.as_str())
                        {
                            out.push(name.to_string());
                        }
                    }
                    _ => {}
                }
            }
            out
        }
        Value::Object(obj) => {
            if let Some(name) = obj.get("sector_name").and_then(|x| x.as_str()) {
                return vec![name.to_string()];
            }
            for key in ["data", "result", "items", "sectors"] {
                if let Some(nested) = obj.get(key) {
                    let inner = extract_sector_names(nested);
                    if !inner.is_empty() {
                        return inner;
                    }
                }
            }
            let meta_keys = ["code", "msg", "message", "success", "status"];
            obj.iter()
                .filter(|(k, v)| {
                    !meta_keys.contains(&k.as_str()) && (v.is_object() || v.is_array())
                })
                .map(|(k, _)| k.to_string())
                .collect()
        }
        _ => vec![],
    }
}
