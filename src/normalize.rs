use serde_json::{Map, Value};

use crate::models::NormalizedBar;

fn pick<'a>(m: &'a Map<String, Value>, keys: &[&str]) -> Option<&'a Value> {
    for key in keys {
        if let Some(v) = m.get(*key) {
            return Some(v);
        }
    }
    None
}

fn to_opt_f64(v: Option<&Value>) -> Option<f64> {
    match v {
        None => None,
        Some(Value::Null) => None,
        Some(Value::String(s)) if s.trim().is_empty() => None,
        Some(Value::Number(n)) => n.as_f64(),
        Some(Value::String(s)) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn to_opt_string(v: Option<&Value>) -> Option<String> {
    match v {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.to_string()),
        Some(other) => Some(other.to_string()),
    }
}

fn likely_code(s: &str) -> bool {
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() != 2 {
        return false;
    }
    parts
        .iter()
        .all(|p| !p.is_empty() && p.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_'))
}

fn normalize_bar(stock_code: &str, period: &str, obj: &Map<String, Value>) -> NormalizedBar {
    let ts_raw = to_opt_string(pick(
        obj,
        &["time", "timestamp", "trade_time", "datetime", "date", "t"],
    ));

    let known_keys = [
        "time",
        "timestamp",
        "trade_time",
        "datetime",
        "date",
        "t",
        "open",
        "o",
        "high",
        "h",
        "low",
        "l",
        "close",
        "c",
        "volume",
        "vol",
        "v",
        "amount",
        "amt",
        "turnover",
        "value",
        "turnover_rate",
        "turnrate",
        "open_interest",
        "oi",
        "settle",
        "settlement",
        "settle_price",
        "adj_factor",
        "factor",
    ];

    let mut extra = Map::new();
    for (k, v) in obj {
        if !known_keys.contains(&k.as_str()) {
            extra.insert(k.clone(), v.clone());
        }
    }

    NormalizedBar {
        stock_code: stock_code.to_string(),
        period: period.to_string(),
        ts_raw,
        open: to_opt_f64(pick(obj, &["open", "o"])),
        high: to_opt_f64(pick(obj, &["high", "h"])),
        low: to_opt_f64(pick(obj, &["low", "l"])),
        close: to_opt_f64(pick(obj, &["close", "c"])),
        volume: to_opt_f64(pick(obj, &["volume", "vol", "v"])),
        amount: to_opt_f64(pick(obj, &["amount", "amt", "turnover", "value"])),
        turnover_rate: to_opt_f64(pick(obj, &["turnover_rate", "turnrate"])),
        open_interest: to_opt_f64(pick(obj, &["open_interest", "oi"])),
        settle: to_opt_f64(pick(obj, &["settle", "settlement", "settle_price"])),
        adj_factor: to_opt_f64(pick(obj, &["adj_factor", "factor"])),
        extra_json: if extra.is_empty() {
            None
        } else {
            Some(Value::Object(extra))
        },
    }
}

fn rows_from_dict_of_arrays(stock_code: &str, period: &str, payload: &Map<String, Value>) -> Vec<NormalizedBar> {
    let series_keys = [
        "time",
        "timestamp",
        "datetime",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "turnover",
        "adj_factor",
    ];

    let lengths: Vec<usize> = series_keys
        .iter()
        .filter_map(|k| payload.get(*k).and_then(|v| v.as_array()).map(|a| a.len()))
        .collect();

    if lengths.is_empty() {
        return vec![];
    }

    let count = *lengths.iter().max().unwrap_or(&0);
    let mut rows = Vec::new();

    for idx in 0..count {
        let mut bar = Map::new();
        for key in series_keys {
            if let Some(values) = payload.get(key).and_then(|v| v.as_array()) {
                if let Some(v) = values.get(idx) {
                    bar.insert(key.to_string(), v.clone());
                }
            }
        }
        if !bar.is_empty() {
            rows.push(normalize_bar(stock_code, period, &bar));
        }
    }
    rows
}

fn extract_rows_for_stock(stock_code: &str, period: &str, payload: &Value) -> Vec<NormalizedBar> {
    match payload {
        Value::Array(arr) => arr
            .iter()
            .filter_map(|item| item.as_object())
            .map(|obj| normalize_bar(stock_code, period, obj))
            .collect(),
        Value::Object(obj) => {
            for key in ["kline", "klines", "bars", "data", "items", "result"] {
                if let Some(v) = obj.get(key) {
                    let rows = extract_rows_for_stock(stock_code, period, v);
                    if !rows.is_empty() {
                        return rows;
                    }
                }
            }

            if ["open", "close", "time", "date"]
                .iter()
                .any(|k| obj.get(*k).is_some_and(|x| x.is_array()))
            {
                let rows = rows_from_dict_of_arrays(stock_code, period, obj);
                if !rows.is_empty() {
                    return rows;
                }
            }

            if ["open", "high", "low", "close"].iter().any(|k| obj.contains_key(*k)) {
                return vec![normalize_bar(stock_code, period, obj)];
            }
            vec![]
        }
        _ => vec![],
    }
}

pub fn normalize_full_kline_response(data: &Value, period: &str) -> Vec<NormalizedBar> {
    let mut rows = Vec::new();

    match data {
        Value::Null => {}
        Value::Array(arr) => {
            for item in arr {
                if let Some(obj) = item.as_object() {
                    let stock_code = obj
                        .get("stock_code")
                        .or_else(|| obj.get("code"))
                        .and_then(|v| v.as_str());
                    if let Some(code) = stock_code {
                        rows.extend(extract_rows_for_stock(code, period, item));
                    } else {
                        rows.extend(normalize_full_kline_response(item, period));
                    }
                }
            }
        }
        Value::Object(obj) => {
            for key in ["data", "result", "items"] {
                if let Some(v) = obj.get(key) {
                    rows.extend(normalize_full_kline_response(v, period));
                }
            }

            for (k, v) in obj {
                if ["data", "result", "items"].contains(&k.as_str()) {
                    continue;
                }
                if likely_code(k) {
                    rows.extend(extract_rows_for_stock(k, period, v));
                }
            }
        }
        _ => {}
    }

    rows
}
