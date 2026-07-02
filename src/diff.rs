use csv::WriterBuilder;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;

use crate::app_data_dir;

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

fn read_csv_rows(
    path: &PathBuf,
) -> Result<(Vec<String>, Vec<Vec<String>>), Box<dyn std::error::Error>> {
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

pub fn diff_csv_files(
    old_path: &PathBuf,
    new_path: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let (old_headers, old_rows) = read_csv_rows(old_path)?;
    let (new_headers, new_rows) = read_csv_rows(new_path)?;

    if old_headers != new_headers {
        return Err("CSV files have different headers — cannot diff".into());
    }

    let key_col = "udiDiCode";
    let key_idx = old_headers
        .iter()
        .position(|h| h == key_col)
        .ok_or_else(|| format!("Column '{}' not found in headers", key_col))?;

    // Build maps: udiDiCode -> Vec<row>
    let mut old_map: HashMap<String, Vec<Vec<String>>> = HashMap::new();
    for row in &old_rows {
        old_map
            .entry(row[key_idx].clone())
            .or_default()
            .push(row.clone());
    }
    let mut new_map: HashMap<String, Vec<Vec<String>>> = HashMap::new();
    for row in &new_rows {
        new_map
            .entry(row[key_idx].clone())
            .or_default()
            .push(row.clone());
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
    let old_date = extract_date_from_filename(old_path).unwrap_or_else(|| "unknown".to_string());
    let new_date = extract_date_from_filename(new_path).unwrap_or_else(|| "unknown".to_string());
    let diff_dir = app_data_dir().join("diff");
    fs::create_dir_all(&diff_dir)?;
    let out_filename = diff_dir
        .join(format!("diff_swissdamed_{}_{}.csv", old_date, new_date))
        .to_string_lossy()
        .to_string();

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
