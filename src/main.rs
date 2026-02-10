use chrono::Local;
use clap::Parser;
use csv::WriterBuilder;
use rusqlite::Connection;
use serde_json::Value;
use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

/// Download Swiss DAMED UDI data and convert to CSV or SQLite
#[derive(Parser, Debug)]
#[command(name = "swissdamed2sqlite", version, about)]
struct Args {
    /// Output as CSV file
    #[arg(long, group = "format")]
    csv: bool,

    /// Output as SQLite database
    #[arg(long, group = "format")]
    sqlite: bool,

    /// Use an existing JSON file instead of downloading
    #[arg(long, short = 'f')]
    file: Option<PathBuf>,

    /// Page size for API requests (default: 50)
    #[arg(long, default_value_t = 50)]
    page_size: u32,
}

fn date_stamp() -> String {
    Local::now().format("%d.%m.%Y").to_string()
}

fn output_filename(ext: &str) -> String {
    format!("swissdamed_{}.{}", date_stamp(), ext)
}

// --- Download ---

fn download_all_pages(page_size: u32) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    let client = reqwest::blocking::Client::builder()
        .cookie_store(true)
        .user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")
        .build()?;

    let mut all_values: Vec<Value> = Vec::new();
    let mut page: u32 = 0;

    loop {
        let url = format!(
            "https://swissdamed.ch/public/udi/basic-udis?page={}&size={}",
            page, page_size
        );
        eprintln!("Fetching page {} ...", page);

        let resp = client
            .post(&url)
            .header("Accept", "application/json, text/plain, */*")
            .header("Content-Type", "application/json")
            .body("{}")
            .send()?;

        if !resp.status().is_success() {
            return Err(format!("HTTP error: {} for page {}", resp.status(), page).into());
        }

        let body: Value = resp.json()?;

        let values = body
            .get("values")
            .and_then(|v| v.as_array())
            .ok_or("Response missing 'values' array")?;

        if values.is_empty() {
            break;
        }

        let count = values.len();
        all_values.extend(values.iter().cloned());
        eprintln!("  got {} items (total so far: {})", count, all_values.len());

        if (count as u32) < page_size {
            break;
        }

        page += 1;
    }

    eprintln!("Download complete: {} items total.", all_values.len());
    Ok(all_values)
}

// --- JSON file loading ---

fn load_json_file(path: &PathBuf) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(path)?;
    let parsed: Value = serde_json::from_str(&content)?;

    if let Some(arr) = parsed.get("values").and_then(|v| v.as_array()) {
        Ok(arr.clone())
    } else if let Some(arr) = parsed.as_array() {
        Ok(arr.clone())
    } else {
        Err("JSON must contain a 'values' array or be a top-level array".into())
    }
}

// --- Value conversion ---

fn sanitize(s: &str) -> String {
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
            if t.is_empty() {
                None
            } else {
                Some(t)
            }
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
            if d.is_empty() {
                None
            } else {
                Some(d)
            }
        }
    }
}

fn value_to_string(val: &Value) -> String {
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

fn get_field(obj: &Value, key: &str) -> String {
    match obj.get(key) {
        Some(val) => value_to_string(val),
        None => String::new(),
    }
}

// --- Header collection and row building ---

fn collect_headers(values: &[Value]) -> Vec<String> {
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

    headers.push("udiDiCode".to_string());
    headers.push("tradeNames".to_string());
    headers
}

fn build_rows(values: &[Value], headers: &[String]) -> Vec<Vec<String>> {
    let main_header_count = headers.len() - 2;
    let mut rows = Vec::new();

    for item in values {
        if !item.is_object() {
            continue;
        }

        let main_fields: Vec<String> = headers[..main_header_count]
            .iter()
            .map(|key| get_field(item, key))
            .collect();

        let udi_entries: Vec<(String, String)> = item
            .get("udiDis")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|udi| {
                        let code = get_field(udi, "udiDiCode");
                        let names = get_field(udi, "tradeNames");
                        (code, names)
                    })
                    .collect()
            })
            .unwrap_or_else(|| vec![(String::new(), String::new())]);

        for (code, names) in &udi_entries {
            let mut row = main_fields.clone();
            row.push(code.clone());
            row.push(names.clone());
            rows.push(row);
        }
    }

    rows
}

// --- Output writers ---

fn write_csv(
    headers: &[String],
    rows: &[Vec<String>],
    filename: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut wtr = WriterBuilder::new().from_writer(Vec::new());
    wtr.write_record(headers)?;
    for row in rows {
        wtr.write_record(row)?;
    }
    let data = wtr.into_inner()?;

    // Prepend UTF-8 BOM for Excel compatibility
    let mut output = Vec::with_capacity(3 + data.len());
    output.extend_from_slice(b"\xEF\xBB\xBF");
    output.extend_from_slice(&data);

    fs::write(filename, output)?;
    Ok(())
}

fn write_sqlite(
    headers: &[String],
    rows: &[Vec<String>],
    filename: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if std::path::Path::new(filename).exists() {
        fs::remove_file(filename)?;
    }

    let conn = Connection::open(filename)?;

    let col_defs: Vec<String> = headers
        .iter()
        .map(|h| format!("\"{}\" TEXT", h))
        .collect();
    let create_sql = format!("CREATE TABLE swissdamed ({})", col_defs.join(", "));
    conn.execute(&create_sql, [])?;

    let placeholders: Vec<&str> = vec!["?"; headers.len()];
    let insert_sql = format!(
        "INSERT INTO swissdamed ({}) VALUES ({})",
        headers
            .iter()
            .map(|h| format!("\"{}\"", h))
            .collect::<Vec<_>>()
            .join(", "),
        placeholders.join(", ")
    );

    let tx = conn.unchecked_transaction()?;
    {
        let mut stmt = tx.prepare(&insert_sql)?;
        for row in rows {
            let params: Vec<&dyn rusqlite::types::ToSql> =
                row.iter().map(|s| s as &dyn rusqlite::types::ToSql).collect();
            stmt.execute(params.as_slice())?;
        }
    }
    tx.commit()?;

    for col in &["udiDiCode", "tradeNames"] {
        if headers.contains(&col.to_string()) {
            let idx_sql = format!(
                "CREATE INDEX IF NOT EXISTS idx_{} ON swissdamed(\"{}\")",
                col, col
            );
            conn.execute(&idx_sql, [])?;
        }
    }

    Ok(())
}

// --- Main ---

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let (do_csv, do_sqlite) = if !args.csv && !args.sqlite {
        (true, true)
    } else {
        (args.csv, args.sqlite)
    };

    let values = if let Some(ref path) = args.file {
        eprintln!("Loading from file: {}", path.display());
        load_json_file(path)?
    } else {
        download_all_pages(args.page_size)?
    };

    if values.is_empty() {
        eprintln!("No data found.");
        return Ok(());
    }

    let headers = collect_headers(&values);
    let rows = build_rows(&values, &headers);

    eprintln!(
        "Processed {} items, generated {} rows with {} columns.",
        values.len(),
        rows.len(),
        headers.len()
    );

    if do_csv {
        let filename = output_filename("csv");
        write_csv(&headers, &rows, &filename)?;
        eprintln!("CSV written: {}", filename);
    }

    if do_sqlite {
        let filename = output_filename("db");
        write_sqlite(&headers, &rows, &filename)?;
        eprintln!("SQLite written: {}", filename);
    }

    Ok(())
}
