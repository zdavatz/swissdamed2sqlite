use serde_json::Value;
use std::collections::{BTreeSet, HashMap};

pub fn sanitize(s: &str) -> String {
    s.chars()
        .filter_map(|c| {
            if c >= ' ' || c == '\t' || c == '\n' || c == '\r' {
                Some(c)
            } else if c == '\0' {
                Some(' ')
            } else {
                None
            }
        })
        .collect()
}

fn format_float(f: f64) -> String {
    let s = format!("{:.10}", f);
    let s = s.trim_end_matches('0');
    let s = s.trim_end_matches('.');
    s.to_string()
}

fn extract_array_element(elem: &Value) -> Option<String> {
    match elem {
        Value::Object(obj) => {
            let text = obj
                .get("textValue")
                .or_else(|| obj.get("value"))
                .or_else(|| obj.get("name"))
                .and_then(|v| v.as_str())
                .map(|s| sanitize(s.trim()))
                .unwrap_or_default();

            let lang = obj
                .get("language")
                .or_else(|| obj.get("lang"))
                .and_then(|v| v.as_str())
                .map(|s| sanitize(s.trim()))
                .unwrap_or_else(|| "ANY".to_string());

            if text.is_empty() {
                None
            } else {
                Some(format!("{}: {}", lang, text))
            }
        }
        Value::String(s) => {
            let t = sanitize(s.trim());
            if t.is_empty() { None } else { Some(t) }
        }
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(i.to_string())
            } else if let Some(f) = n.as_f64() {
                Some(format_float(f))
            } else {
                Some(n.to_string())
            }
        }
        Value::Bool(b) => Some(if *b { "TRUE" } else { "FALSE" }.to_string()),
        Value::Null => None,
        _ => {
            let d = sanitize(&elem.to_string());
            if d.is_empty() { None } else { Some(d) }
        }
    }
}

pub fn value_to_string(val: &Value) -> String {
    match val {
        Value::Null => String::new(),
        Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.to_string()
            } else if let Some(f) = n.as_f64() {
                format_float(f)
            } else {
                n.to_string()
            }
        }
        Value::String(s) => sanitize(s.trim()),
        Value::Array(arr) => {
            let parts: Vec<String> = arr.iter().filter_map(extract_array_element).collect();
            parts.join(" | ")
        }
        Value::Object(_) => sanitize(&val.to_string()),
    }
}

pub fn get_field(obj: &Value, key: &str) -> String {
    match obj.get(key) {
        Some(val) => value_to_string(val),
        None => String::new(),
    }
}

// --- Header collection and row building ---

/// Scan all udiDis -> tradeNames arrays to discover which languages exist,
/// returned in a stable sorted order.
pub fn collect_trade_name_languages(values: &[Value]) -> Vec<String> {
    let mut langs = BTreeSet::new();

    for item in values {
        if let Some(udi_arr) = item.get("udiDis").and_then(|v| v.as_array()) {
            for udi in udi_arr {
                if let Some(tn_arr) = udi.get("tradeNames").and_then(|v| v.as_array()) {
                    for tn in tn_arr {
                        let lang = tn
                            .get("language")
                            .or_else(|| tn.get("lang"))
                            .and_then(|v| v.as_str())
                            .map(|s| s.trim().to_string())
                            .unwrap_or_else(|| "ANY".to_string());
                        langs.insert(lang);
                    }
                }
            }
        }
    }

    langs.into_iter().collect()
}

pub fn collect_headers(values: &[Value]) -> (Vec<String>, Vec<String>) {
    let mut seen = BTreeSet::new();
    let mut headers: Vec<String> = Vec::new();

    for item in values {
        if let Value::Object(map) = item {
            for key in map.keys() {
                if key == "udiDis" {
                    continue;
                }
                if seen.insert(key.clone()) {
                    headers.push(key.clone());
                }
            }
        }
    }

    let trade_name_langs = collect_trade_name_languages(values);

    // Append udiDiCode, then one column per language
    headers.push("udiDiCode".to_string());
    for lang in &trade_name_langs {
        headers.push(format!("tradeName_{}", lang));
    }

    (headers, trade_name_langs)
}

/// Extract per-language trade names from a single udiDis entry.
fn extract_trade_names_by_lang(udi: &Value) -> HashMap<String, String> {
    let mut map = HashMap::new();

    if let Some(tn_arr) = udi.get("tradeNames").and_then(|v| v.as_array()) {
        for tn in tn_arr {
            let lang = tn
                .get("language")
                .or_else(|| tn.get("lang"))
                .and_then(|v| v.as_str())
                .map(|s| s.trim().to_string())
                .unwrap_or_else(|| "ANY".to_string());

            let text = tn
                .get("textValue")
                .or_else(|| tn.get("value"))
                .or_else(|| tn.get("name"))
                .and_then(|v| v.as_str())
                .map(|s| sanitize(s.trim()))
                .unwrap_or_default();

            if !text.is_empty() {
                map.entry(lang)
                    .and_modify(|existing: &mut String| {
                        existing.push_str(" | ");
                        existing.push_str(&text);
                    })
                    .or_insert(text);
            }
        }
    }

    map
}

pub fn build_rows(
    values: &[Value],
    headers: &[String],
    trade_name_langs: &[String],
) -> Vec<Vec<String>> {
    let main_header_count = headers.len() - 1 - trade_name_langs.len();
    let mut rows = Vec::new();

    for item in values {
        if !item.is_object() {
            continue;
        }

        let main_fields: Vec<String> = headers[..main_header_count]
            .iter()
            .map(|key| get_field(item, key))
            .collect();

        let udi_entries: Vec<(String, HashMap<String, String>)> = item
            .get("udiDis")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|udi| {
                        let code = get_field(udi, "udiDiCode");
                        let tn_map = extract_trade_names_by_lang(udi);
                        (code, tn_map)
                    })
                    .collect()
            })
            .unwrap_or_else(|| vec![(String::new(), HashMap::new())]);

        for (code, tn_map) in &udi_entries {
            let mut row = main_fields.clone();
            row.push(code.clone());
            for lang in trade_name_langs {
                row.push(tn_map.get(lang).cloned().unwrap_or_default());
            }
            rows.push(row);
        }
    }

    rows
}

// --- Flat data processing (actors, mandates) ---

pub fn collect_flat_headers(values: &[Value]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut headers: Vec<String> = Vec::new();

    for item in values {
        if let Value::Object(map) = item {
            for key in map.keys() {
                if seen.insert(key.clone()) {
                    headers.push(key.clone());
                }
            }
        }
    }

    headers
}

pub fn build_flat_rows(values: &[Value], headers: &[String]) -> Vec<Vec<String>> {
    values
        .iter()
        .filter(|item| item.is_object())
        .map(|item| headers.iter().map(|key| get_field(item, key)).collect())
        .collect()
}

/// Flatten a mandate detail JSON into a stable set of key-value pairs.
/// Nested objects like `address` and `actorInfo` are flattened with prefix.
pub fn flatten_mandate_detail(detail: &Value) -> Vec<(String, String)> {
    let mut fields = Vec::new();

    if let Value::Object(map) = detail {
        for (key, val) in map {
            match val {
                Value::Object(inner) => {
                    for (inner_key, inner_val) in inner {
                        fields.push((
                            format!("{}_{}", key, inner_key),
                            value_to_string(inner_val),
                        ));
                    }
                }
                _ => {
                    fields.push((key.clone(), value_to_string(val)));
                }
            }
        }
    }

    fields
}

pub fn fetch_mandate_details(
    client: &reqwest::blocking::Client,
    mandate_ids: &[String],
) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    let total = mandate_ids.len();
    let mut details = Vec::with_capacity(total);

    for (i, id) in mandate_ids.iter().enumerate() {
        if (i + 1) % 50 == 0 || i + 1 == total {
            eprintln!("[mandate-details] Fetching {}/{} ...", i + 1, total);
        }

        let url = format!("https://swissdamed.ch/public/act/mandates/{}", id);
        let resp = client
            .get(&url)
            .header("Accept", "application/json, text/plain, */*")
            .send()?;

        if resp.status().is_success() {
            let body: Value = resp.json()?;
            details.push(body);
        } else {
            eprintln!(
                "[mandate-details] Warning: HTTP {} for mandate {}",
                resp.status(),
                id
            );
            details.push(Value::Null);
        }
    }

    eprintln!("[mandate-details] Fetched {} details.", details.len());
    Ok(details)
}
