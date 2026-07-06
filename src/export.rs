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
