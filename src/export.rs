use csv::WriterBuilder;
use rusqlite::Connection;
use std::fs;

use crate::app_data_dir;

fn date_stamp() -> String {
    chrono::Local::now().format("%d.%m.%Y").to_string()
}

pub fn output_csv(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let dir = app_data_dir().join("csv");
    fs::create_dir_all(&dir)?;
    Ok(dir
        .join(format!("{}_{}.csv", name, date_stamp()))
        .to_string_lossy()
        .to_string())
}

pub fn output_db(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let dir = app_data_dir().join("db");
    fs::create_dir_all(&dir)?;
    Ok(dir
        .join(format!("{}_{}.db", name, date_stamp()))
        .to_string_lossy()
        .to_string())
}

/// Turn the `DD.MM.YYYY` stamp of a dated output file into a `YYYYMMDD` sort
/// key (`udi_details_01.09.2026.db` + prefix `udi_details_` → `20260901`).
///
/// Dated filenames must never be ordered lexically — `..._01.09.2026.db` sorts
/// *before* `..._28.08.2026.db` — and mtime order only holds until a DB is
/// copied, restored or rsynced, at which point an older file wins. Every
/// "newest DB" lookup goes through this instead; see
/// [`crate::sigvaris_shop::find_latest_db`], where this bug was first fixed.
///
/// Returns `None` for a name that carries no well-formed stamp.
pub fn dated_db_key(name: &str, prefix: &str) -> Option<u32> {
    let stamp = name.strip_prefix(prefix)?.strip_suffix(".db")?;
    let parts: Vec<&str> = stamp.split('.').collect();
    let [d, m, y] = parts[..] else { return None };
    if d.len() != 2 || m.len() != 2 || y.len() != 4 {
        return None;
    }
    let (d, m, y): (u32, u32, u32) = (d.parse().ok()?, m.parse().ok()?, y.parse().ok()?);
    if !(1..=31).contains(&d) || !(1..=12).contains(&m) {
        return None;
    }
    Some(y * 10_000 + m * 100 + d)
}

/// Pick the newest `<prefix>DD.MM.YYYY.db` in `db_dir` by its filename date,
/// breaking ties on mtime. Files without a well-formed stamp are ignored.
pub fn find_latest_dated_db(dir: &std::path::Path, prefix: &str) -> Option<std::path::PathBuf> {
    let mut best: Option<(u32, std::time::SystemTime, std::path::PathBuf)> = None;
    for entry in fs::read_dir(dir).ok()?.flatten() {
        let path = entry.path();
        let name = path.file_name()?.to_string_lossy().into_owned();
        let key = match dated_db_key(&name, prefix) {
            Some(k) => k,
            None => continue,
        };
        let mtime = entry
            .metadata()
            .and_then(|m| m.modified())
            .unwrap_or(std::time::UNIX_EPOCH);
        let better = best
            .as_ref()
            .map_or(true, |(k, t, _)| key > *k || (key == *k && mtime > *t));
        if better {
            best = Some((key, mtime, path));
        }
    }
    best.map(|(_, _, p)| p)
}

/// Like [`output_db`] but without the date stamp — a stable filename that is
/// overwritten on each run (used for the MiGeL match DB so it no longer
/// accumulates one file per day).
pub fn output_db_fixed(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let dir = app_data_dir().join("db");
    fs::create_dir_all(&dir)?;
    Ok(dir
        .join(format!("{}.db", name))
        .to_string_lossy()
        .to_string())
}

/// Date-stamped output path under the `pdf/` subdir (e.g. the triage status sheet).
pub fn output_pdf(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let dir = app_data_dir().join("pdf");
    fs::create_dir_all(&dir)?;
    Ok(dir
        .join(format!("{}_{}.pdf", name, date_stamp()))
        .to_string_lossy()
        .to_string())
}

pub fn write_csv(
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

pub fn write_sqlite(
    headers: &[String],
    rows: &[Vec<String>],
    filename: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    write_sqlite_table(headers, rows, filename, "swissdamed")
}

pub fn write_sqlite_table(
    headers: &[String],
    rows: &[Vec<String>],
    filename: &str,
    table_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if std::path::Path::new(filename).exists() {
        fs::remove_file(filename)?;
    }

    let mut conn = Connection::open(filename)?;

    // Escape SQL identifiers: double any embedded quotes per SQL standard
    let quote_ident = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));

    let col_defs: Vec<String> = headers
        .iter()
        .map(|h| format!("{} TEXT", quote_ident(h)))
        .collect();
    let create_sql = format!(
        "CREATE TABLE {} ({})",
        quote_ident(table_name),
        col_defs.join(", ")
    );
    conn.execute(&create_sql, [])?;

    let placeholders: Vec<&str> = vec!["?"; headers.len()];
    let insert_sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        quote_ident(table_name),
        headers
            .iter()
            .map(|h| quote_ident(h))
            .collect::<Vec<_>>()
            .join(", "),
        placeholders.join(", ")
    );

    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare(&insert_sql)?;
        for row in rows {
            let params: Vec<&dyn rusqlite::types::ToSql> = row
                .iter()
                .map(|s| s as &dyn rusqlite::types::ToSql)
                .collect();
            stmt.execute(params.as_slice())?;
        }
    }
    tx.commit()?;

    // Create index on udiDiCode
    if headers.contains(&"udiDiCode".to_string()) {
        conn.execute(
            &format!(
                "CREATE INDEX IF NOT EXISTS idx_udiDiCode ON {}({})",
                quote_ident(table_name),
                quote_ident("udiDiCode")
            ),
            [],
        )?;
    }

    // Create indexes on trade name columns
    for col in headers.iter().filter(|h| h.starts_with("tradeName_")) {
        let idx_name = format!("idx_{}", col.replace('"', ""));
        let idx_sql = format!(
            "CREATE INDEX IF NOT EXISTS {} ON {}({})",
            quote_ident(&idx_name),
            quote_ident(table_name),
            quote_ident(col)
        );
        conn.execute(&idx_sql, [])?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dated_db_keys_order_by_date_not_lexically() {
        let k = |n: &str| dated_db_key(n, "udi_details_").unwrap();
        // The regression this guards: September sorts lexically BEFORE August.
        assert!("udi_details_01.09.2026.db" < "udi_details_28.08.2026.db");
        assert!(k("udi_details_01.09.2026.db") > k("udi_details_28.08.2026.db"));
        assert_eq!(k("udi_details_01.09.2026.db"), 20_260_901);
        assert!(k("udi_details_31.12.2025.db") < k("udi_details_01.01.2026.db"));
    }

    #[test]
    fn undated_and_malformed_names_have_no_key() {
        for n in [
            "swissdamed_migel.db",          // the fixed-name MiGeL DB
            "udi_details_2026-09-01.db",    // wrong separator
            "udi_details_1.9.2026.db",      // unpadded
            "udi_details_01.09.2026.sqlite" // wrong extension
        ] {
            assert_eq!(dated_db_key(n, "udi_details_"), None, "{n}");
            assert_eq!(dated_db_key(n, "swissdamed_migel_"), None, "{n}");
        }
        // A date-shaped stamp that is not a real date is rejected too.
        assert_eq!(dated_db_key("udi_details_00.13.2026.db", "udi_details_"), None);
    }

    #[test]
    fn find_latest_dated_db_ignores_mtime_order() {
        let dir = std::env::temp_dir().join(format!("sd_export_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        // Write the NEWER date first so it also carries the OLDER mtime — an
        // mtime-ordered lookup would pick 28.08 here.
        for n in ["udi_details_01.09.2026.db", "udi_details_28.08.2026.db"] {
            fs::write(dir.join(n), b"x").unwrap();
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        fs::write(dir.join("udi_details_partial.db"), b"x").unwrap();
        let got = find_latest_dated_db(&dir, "udi_details_").unwrap();
        assert_eq!(got.file_name().unwrap(), "udi_details_01.09.2026.db");
        let _ = fs::remove_dir_all(&dir);
    }
}
