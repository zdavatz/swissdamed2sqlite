//! `--gtin-bundle <file>`: everything swissdamed has for a list of GTINs, in one
//! table — no download, it reads the latest local DBs (06.11.2026).
//!
//! Sources (latest dated file in the db dir each):
//! - `swissdamed_<date>.db`: list row (`swissdamed`) and package levels
//!   (`swissdamed_packages`)
//! - `udi_details_<date>.db`: full per-device detail record (`rawJson`), fetched
//!   with `--details` (`--details-gtin-file` for exactly these GTINs)
//!
//! Output `gtin_bundle_<date>.db`, table `gtin_bundle`, one row per GTIN of the
//! file: gtin, in_swissdamed, list_json, detail_json, packages_json (JSON text,
//! NULL when swissdamed has none). Use case: the EUDAMED public endpoints were
//! blocked (429 since 05.11.2026), so the devices a nightly could not download
//! are collected from swissdamed as far as it has them.

use std::collections::HashMap;

use rusqlite::{types::ValueRef, Connection, OpenFlags};
use serde_json::{Map, Value};

fn pad14(s: &str) -> String {
    format!("{:0>14}", s.trim())
}

/// GTINs of the file: 8-14 digit lines, padded to 14, de-duplicated in order.
pub fn read_gtins(path: &std::path::Path) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for l in std::fs::read_to_string(path)?.lines() {
        let t = l.trim();
        if t.len() >= 8 && t.len() <= 14 && t.bytes().all(|b| b.is_ascii_digit()) {
            let g = pad14(t);
            if seen.insert(g.clone()) {
                out.push(g);
            }
        }
    }
    Ok(out)
}

fn open_ro(path: &std::path::Path) -> rusqlite::Result<Connection> {
    Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
}

/// All rows of `table` as JSON objects, grouped by padded `udiDiCode`.
fn rows_by_gtin(
    conn: &Connection,
    table: &str,
) -> Result<HashMap<String, Vec<Map<String, Value>>>, Box<dyn std::error::Error>> {
    let mut stmt = conn.prepare(&format!("SELECT * FROM \"{}\"", table))?;
    let cols: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
    let gi = cols
        .iter()
        .position(|c| c == "udiDiCode")
        .ok_or_else(|| format!("{table}: no udiDiCode column"))?;
    let mut out: HashMap<String, Vec<Map<String, Value>>> = HashMap::new();
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let mut obj = Map::new();
        for (i, c) in cols.iter().enumerate() {
            let v = match r.get_ref(i)? {
                ValueRef::Null => Value::Null,
                ValueRef::Integer(n) => Value::from(n),
                ValueRef::Real(f) => Value::from(f),
                ValueRef::Text(t) | ValueRef::Blob(t) => {
                    Value::String(String::from_utf8_lossy(t).into_owned())
                }
            };
            obj.insert(c.clone(), v);
        }
        let g = obj
            .get(&cols[gi])
            .and_then(|v| v.as_str())
            .map(pad14)
            .unwrap_or_default();
        if !g.trim_start_matches('0').is_empty() {
            out.entry(g).or_default().push(obj);
        }
    }
    Ok(out)
}

pub fn run(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let db_dir = crate::app_data_dir().join("db");
    let list_db = crate::export::find_latest_dated_db(&db_dir, "swissdamed_")
        .ok_or("no swissdamed_<date>.db — run --sqlite first")?;
    let gtins = read_gtins(path)?;
    eprintln!("[bundle] {} GTINs from {}", gtins.len(), path.display());
    eprintln!("[bundle] list + packages: {}", list_db.display());

    let lconn = open_ro(&list_db)?;
    let list = rows_by_gtin(&lconn, "swissdamed")?;
    let packages = rows_by_gtin(&lconn, "swissdamed_packages").unwrap_or_default();
    let details: HashMap<String, String> = match crate::details::find_latest_db(&db_dir) {
        Some(p) => {
            eprintln!("[bundle] details: {}", p.display());
            let c = open_ro(&p)?;
            let mut st = c.prepare("SELECT udiDiCode, rawJson FROM udi_details")?;
            let it = st.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
            let mut m = HashMap::new();
            for (g, raw) in it.flatten() {
                m.entry(pad14(&g)).or_insert(raw);
            }
            m
        }
        None => {
            eprintln!("[bundle] no udi_details_<date>.db — detail_json stays empty (run --details --details-gtin-file)");
            HashMap::new()
        }
    };

    let out_path = crate::export::output_db("gtin_bundle")?;
    let _ = std::fs::remove_file(&out_path);
    let mut out = Connection::open(&out_path)?;
    out.execute(
        "CREATE TABLE gtin_bundle (gtin TEXT PRIMARY KEY, in_swissdamed INTEGER, \
         list_json TEXT, detail_json TEXT, packages_json TEXT)",
        [],
    )?;
    let (mut found, mut with_detail, mut with_pkgs) = (0usize, 0usize, 0usize);
    let tx = out.transaction()?;
    {
        let mut ins = tx.prepare("INSERT INTO gtin_bundle VALUES (?1, ?2, ?3, ?4, ?5)")?;
        for g in &gtins {
            let l = list
                .get(g)
                .and_then(|v| v.first())
                .map(|o| Value::Object(o.clone()).to_string());
            let d = details.get(g).cloned();
            let p = packages
                .get(g)
                .map(|v| Value::Array(v.iter().cloned().map(Value::Object).collect()).to_string());
            let hit = l.is_some() || d.is_some();
            found += hit as usize;
            with_detail += d.is_some() as usize;
            with_pkgs += p.is_some() as usize;
            ins.execute(rusqlite::params![g, hit as i64, l, d, p])?;
        }
    }
    tx.commit()?;
    eprintln!(
        "[bundle] {} of {} GTINs in swissdamed ({} with detail record, {} with package levels) -> {}",
        found,
        gtins.len(),
        with_detail,
        with_pkgs,
        out_path
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::read_gtins;

    #[test]
    fn gtin_file_is_padded_and_deduplicated() {
        let p = std::env::temp_dir().join(format!("gtin_bundle_{}.txt", std::process::id()));
        std::fs::write(
            &p,
            "GTIN\n4034342117337\n04034342117337\n07612345000435\nabc\n",
        )
        .unwrap();
        assert_eq!(
            read_gtins(&p).unwrap(),
            vec!["04034342117337", "07612345000435"]
        );
        let _ = std::fs::remove_file(&p);
    }
}
