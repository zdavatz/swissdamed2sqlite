mod migel;

use chrono::Local;
use clap::Parser;
use csv::WriterBuilder;
use migel::{build_keyword_index, find_best_migel_match, parse_migel_items};
use rusqlite::Connection;
use serde_json::Value;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::process::Command;

/// Download Swiss DAMED UDI data and convert to CSV or SQLite
#[derive(Parser, Debug)]
#[command(name = "swissdamed2sqlite", version, about)]
struct Args {
    /// Output as CSV file
    #[arg(long)]
    csv: bool,

    /// Output as SQLite database
    #[arg(long)]
    sqlite: bool,

    /// Use an existing JSON file instead of downloading
    #[arg(long, short = 'f')]
    file: Option<PathBuf>,

    /// Page size for API requests (default: 50)
    #[arg(long, default_value_t = 50)]
    page_size: u32,

    /// Deploy SQLite DB to remote server via scp
    #[arg(long)]
    deploy: bool,

    /// Remote scp target (default: zdavatz@65.109.137.20:/var/www/pillbox.oddb.org/swissdamed.db)
    #[arg(long, default_value = "zdavatz@65.109.137.20:/var/www/pillbox.oddb.org/swissdamed.db")]
    scp: String,

    /// Diff two CSV files and output changes to diff/ folder
    #[arg(long, num_args = 2, value_names = ["OLD_CSV", "NEW_CSV"])]
    diff: Option<Vec<PathBuf>>,

    /// Match UDI entries against MiGel codes and output matched results
    #[arg(long)]
    migel: bool,
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

/// Scan all udiDis -> tradeNames arrays to discover which languages exist,
/// returned in a stable sorted order.
fn collect_trade_name_languages(values: &[Value]) -> Vec<String> {
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

fn collect_headers(values: &[Value]) -> (Vec<String>, Vec<String>) {
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
/// Returns a HashMap: language -> text.
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
                // If multiple entries for the same language, join with " | "
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

fn build_rows(values: &[Value], headers: &[String], trade_name_langs: &[String]) -> Vec<Vec<String>> {
    // Main fields = everything before udiDiCode
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

        // Collect udiDis entries with per-language trade names
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

    // Create index on udiDiCode
    if headers.contains(&"udiDiCode".to_string()) {
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_udiDiCode ON swissdamed(\"udiDiCode\")",
            [],
        )?;
    }

    // Create indexes on trade name columns
    for col in headers.iter().filter(|h| h.starts_with("tradeName_")) {
        let idx_sql = format!(
            "CREATE INDEX IF NOT EXISTS idx_{} ON swissdamed(\"{}\")",
            col, col
        );
        conn.execute(&idx_sql, [])?;
    }

    Ok(())
}

// --- Diff ---

fn extract_date_from_filename(path: &PathBuf) -> Option<String> {
    let stem = path.file_stem()?.to_str()?;
    // Expected format: swissdamed_dd.mm.yyyy
    let date = stem.rsplit('_').next()?;
    if date.len() == 10 && date.chars().filter(|c| *c == '.').count() == 2 {
        Some(date.to_string())
    } else {
        None
    }
}

fn read_csv_rows(path: &PathBuf) -> Result<(Vec<String>, Vec<Vec<String>>), Box<dyn std::error::Error>> {
    let data = fs::read(path)?;
    // Skip UTF-8 BOM if present
    let data = if data.starts_with(&[0xEF, 0xBB, 0xBF]) {
        &data[3..]
    } else {
        &data
    };
    let mut rdr = csv::ReaderBuilder::new().from_reader(data);
    let headers: Vec<String> = rdr.headers()?.iter().map(|s| s.to_string()).collect();
    let mut rows = Vec::new();
    for result in rdr.records() {
        let record = result?;
        rows.push(record.iter().map(|s| s.to_string()).collect());
    }
    Ok((headers, rows))
}

fn diff_csv_files(old_path: &PathBuf, new_path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let (old_headers, old_rows) = read_csv_rows(old_path)?;
    let (new_headers, new_rows) = read_csv_rows(new_path)?;

    if old_headers != new_headers {
        return Err("CSV files have different headers — cannot diff".into());
    }

    let key_col = "udiDiCode";
    let key_idx = old_headers.iter().position(|h| h == key_col)
        .ok_or_else(|| format!("Column '{}' not found in headers", key_col))?;

    // Build maps: udiDiCode -> Vec<row> (multiple rows can share same key)
    let mut old_map: HashMap<String, Vec<Vec<String>>> = HashMap::new();
    for row in &old_rows {
        old_map.entry(row[key_idx].clone()).or_default().push(row.clone());
    }
    let mut new_map: HashMap<String, Vec<Vec<String>>> = HashMap::new();
    for row in &new_rows {
        new_map.entry(row[key_idx].clone()).or_default().push(row.clone());
    }

    let old_keys: HashSet<String> = old_map.keys().cloned().collect();
    let new_keys: HashSet<String> = new_map.keys().cloned().collect();

    let mut diff_rows: Vec<(String, Vec<String>)> = Vec::new();

    // Added: keys only in new
    for key in &new_keys {
        if !old_keys.contains(key) {
            for row in &new_map[key] {
                diff_rows.push(("added".to_string(), row.clone()));
            }
        }
    }

    // Removed: keys only in old
    for key in &old_keys {
        if !new_keys.contains(key) {
            for row in &old_map[key] {
                diff_rows.push(("removed".to_string(), row.clone()));
            }
        }
    }

    // Changed: keys in both but rows differ
    for key in old_keys.intersection(&new_keys) {
        let old_set: HashSet<&Vec<String>> = old_map[key].iter().collect();
        let new_set: HashSet<&Vec<String>> = new_map[key].iter().collect();
        if old_set != new_set {
            for row in &old_map[key] {
                if !new_set.contains(row) {
                    diff_rows.push(("changed_old".to_string(), row.clone()));
                }
            }
            for row in &new_map[key] {
                if !old_set.contains(row) {
                    diff_rows.push(("changed_new".to_string(), row.clone()));
                }
            }
        }
    }

    if diff_rows.is_empty() {
        eprintln!("No differences found.");
        return Ok(());
    }

    // Build output filename from dates in input filenames
    let old_date = extract_date_from_filename(old_path)
        .unwrap_or_else(|| "unknown".to_string());
    let new_date = extract_date_from_filename(new_path)
        .unwrap_or_else(|| "unknown".to_string());
    let out_filename = format!("diff/diff_swissdamed_{}_{}.csv", old_date, new_date);

    fs::create_dir_all("diff")?;

    let mut out_headers = vec!["diff_status".to_string()];
    out_headers.extend(old_headers);

    let mut wtr = WriterBuilder::new().from_writer(Vec::new());
    wtr.write_record(&out_headers)?;
    for (status, row) in &diff_rows {
        let mut full_row = vec![status.clone()];
        full_row.extend(row.clone());
        wtr.write_record(&full_row)?;
    }
    let data = wtr.into_inner()?;

    let mut output = Vec::with_capacity(3 + data.len());
    output.extend_from_slice(b"\xEF\xBB\xBF");
    output.extend_from_slice(&data);

    fs::write(&out_filename, output)?;

    let added = diff_rows.iter().filter(|(s, _)| s == "added").count();
    let removed = diff_rows.iter().filter(|(s, _)| s == "removed").count();
    let changed = diff_rows.iter().filter(|(s, _)| s == "changed_new").count();
    eprintln!(
        "Diff written: {} ({} added, {} removed, {} changed)",
        out_filename, added, removed, changed,
    );

    Ok(())
}

// --- MiGel matching ---

fn run_migel(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    // 1. Get swissdamed data
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

    let (headers, trade_name_langs) = collect_headers(&values);
    let rows = build_rows(&values, &headers, &trade_name_langs);
    eprintln!(
        "Processed {} items, generated {} rows with {} columns.",
        values.len(),
        rows.len(),
        headers.len()
    );

    // 2. Download MiGel XLSX
    let migel_url = "https://www.bag.admin.ch/dam/de/sd-web/77j5rwUTzbkq/Mittel-%20und%20Gegenst%C3%A4ndeliste%20per%2001.01.2026%20in%20Excel-Format.xlsx";
    let migel_file = "migel.xlsx";

    eprintln!("Downloading MiGel XLSX...");
    let client = reqwest::blocking::Client::builder()
        .user_agent("swissdamed2sqlite/0.1")
        .build()?;
    let response = client.get(migel_url).send()?;
    if !response.status().is_success() {
        return Err(format!("Failed to download MiGel XLSX: HTTP {}", response.status()).into());
    }
    let bytes = response.bytes()?;
    fs::write(migel_file, &bytes)?;
    eprintln!("MiGel XLSX saved ({} bytes)", bytes.len());

    // 3. Parse MiGel items and build keyword index
    eprintln!("Parsing MiGel items...");
    let migel_items = parse_migel_items(migel_file)?;
    eprintln!("Found {} MiGel items with position numbers", migel_items.len());

    let keyword_index = build_keyword_index(&migel_items);
    eprintln!("Built keyword index with {} unique keywords", keyword_index.len());

    // 4. Find column indices for matching — collect ALL tradeName columns
    let trade_name_indices: Vec<(String, usize)> = headers
        .iter()
        .enumerate()
        .filter(|(_, h)| h.starts_with("tradeName_"))
        .map(|(i, h)| (h.clone(), i))
        .collect();
    let idx_brand = headers.iter().position(|h| h == "companyName");
    let idx_device = headers.iter().position(|h| h == "deviceName");
    let idx_model = headers.iter().position(|h| h == "modelName");

    // 5. Match each row against MiGel
    let mut migel_headers = headers.clone();
    migel_headers.push("migel_code".to_string());
    migel_headers.push("migel_bezeichnung".to_string());
    migel_headers.push("migel_limitation".to_string());

    let mut matched_rows: Vec<Vec<String>> = Vec::new();

    for row in &rows {
        // Combine all tradeName columns into DE/FR/IT buckets for matching.
        // ANY and EN text is added to all three language descriptions so that
        // products with only tradeName_ANY or tradeName_EN can still match.
        let mut desc_de = String::new();
        let mut desc_fr = String::new();
        let mut desc_it = String::new();

        for (col_name, idx) in &trade_name_indices {
            let val = row.get(*idx).cloned().unwrap_or_default();
            if val.is_empty() {
                continue;
            }
            match col_name.as_str() {
                "tradeName_DE" => desc_de = format!("{} {}", desc_de, val),
                "tradeName_FR" => desc_fr = format!("{} {}", desc_fr, val),
                "tradeName_IT" => desc_it = format!("{} {}", desc_it, val),
                _ => {
                    // ANY, EN, or other languages — add to all three
                    desc_de = format!("{} {}", desc_de, val);
                    desc_fr = format!("{} {}", desc_fr, val);
                    desc_it = format!("{} {}", desc_it, val);
                }
            }
        }

        // Also include deviceName and modelName for better matching
        let device = idx_device.and_then(|i| row.get(i)).cloned().unwrap_or_default();
        let model = idx_model.and_then(|i| row.get(i)).cloned().unwrap_or_default();
        if !device.is_empty() {
            desc_de = format!("{} {}", desc_de, device);
            desc_fr = format!("{} {}", desc_fr, device);
            desc_it = format!("{} {}", desc_it, device);
        }
        if !model.is_empty() {
            desc_de = format!("{} {}", desc_de, model);
            desc_fr = format!("{} {}", desc_fr, model);
            desc_it = format!("{} {}", desc_it, model);
        }

        let brand = idx_brand.and_then(|i| row.get(i)).cloned().unwrap_or_default();

        if let Some(migel) = find_best_migel_match(
            &desc_de, &desc_fr, &desc_it, &brand, &migel_items, &keyword_index,
        ) {
            let mut matched_row = row.clone();
            matched_row.push(migel.position_nr.clone());
            matched_row.push(migel.bezeichnung.clone());
            matched_row.push(migel.limitation.clone());
            matched_rows.push(matched_row);
        }
    }

    eprintln!(
        "MiGel matches: {} out of {} rows",
        matched_rows.len(),
        rows.len()
    );

    if matched_rows.is_empty() {
        eprintln!("No MiGel matches found.");
        return Ok(());
    }

    // 6. Write matched rows to SQLite
    let db_filename = format!("swissdamed_migel_{}.db", date_stamp());
    write_sqlite(&migel_headers, &matched_rows, &db_filename)?;
    eprintln!("SQLite written: {}", db_filename);

    Ok(())
}

// --- Main ---

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Handle --diff mode
    if let Some(ref diff_files) = args.diff {
        return diff_csv_files(&diff_files[0], &diff_files[1]);
    }

    // Handle --migel mode
    if args.migel {
        return run_migel(&args);
    }

    // --deploy implies --sqlite
    let (do_csv, do_sqlite) = if !args.csv && !args.sqlite {
        (true, true)
    } else if args.deploy && !args.sqlite {
        (args.csv, true)
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

    let (headers, trade_name_langs) = collect_headers(&values);
    let rows = build_rows(&values, &headers, &trade_name_langs);

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

        if args.deploy {
            eprintln!("Deploying {} to {} ...", filename, args.scp);
            let status = Command::new("scp")
                .arg(&filename)
                .arg(&args.scp)
                .status()?;

            if status.success() {
                eprintln!("Deploy successful.");
            } else {
                eprintln!("Deploy failed with exit code: {}", status);
                return Err("scp failed".into());
            }
        }
    }

    Ok(())
}
