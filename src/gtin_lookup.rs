//! `--gtin <code>…` — look one or more GTINs (UDI-DIs) up in the local DBs and
//! print everything we hold on them: the full basic UDI-DI record and the full
//! UDI-DI detail record from the newest `udi_details_*.db` (the verbatim detail
//! response in `rawJson`, flattened to dotted paths), the triage verdict, and
//! the MiGeL match row from `swissdamed_migel.db` (list fields + code).
//!
//! Standalone: no download. A code that is not a base-unit UDI-DI is looked
//! for as a package UDI-DI inside the details' `rawJson` (slower full scan,
//! only on a miss), and the owning base unit is printed.

use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde_json::Value;
use std::error::Error;
use std::path::Path;

type R<T> = Result<T, Box<dyn Error>>;

/// Spellings to try for one input: as typed, digits only, and the GS1 digit
/// string padded to 14 (the registry stores GTIN-14, retail labels show
/// EAN-13). Non-numeric HIBCC / ICCBBA codes keep their typed form.
pub fn candidates(input: &str) -> Vec<String> {
    let typed = input.trim().to_string();
    let mut out = vec![typed.clone()];
    let digits: String = typed.chars().filter(|c| c.is_ascii_digit()).collect();
    if !digits.is_empty() && digits.len() == typed.len() {
        let core = digits.trim_start_matches('0');
        for w in [13usize, 14] {
            if core.len() <= w {
                out.push(format!("{core:0>w$}"));
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Flatten a JSON value into `(dotted.path, value)` leaves. Empty arrays and
/// objects are kept as `[]` / `{}` so "field present but empty" stays visible.
pub fn flatten(prefix: &str, v: &Value, out: &mut Vec<(String, String)>) {
    match v {
        Value::Object(m) if m.is_empty() => out.push((prefix.to_string(), "{}".into())),
        Value::Object(m) => {
            for (k, val) in m {
                let p = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
                flatten(&p, val, out);
            }
        }
        Value::Array(a) if a.is_empty() => out.push((prefix.to_string(), "[]".into())),
        Value::Array(a) => {
            for (i, val) in a.iter().enumerate() {
                flatten(&format!("{prefix}[{i}]"), val, out);
            }
        }
        Value::String(s) => out.push((prefix.to_string(), s.clone())),
        Value::Null => out.push((prefix.to_string(), "null".into())),
        other => out.push((prefix.to_string(), other.to_string())),
    }
}

fn open_ro(p: &Path) -> R<Connection> {
    Ok(Connection::open_with_flags(p, OpenFlags::SQLITE_OPEN_READ_ONLY)?)
}

fn placeholders(n: usize) -> String {
    vec!["?"; n].join(",")
}

struct DetailRow {
    udi: String,
    raw: String,
    intended: String,
    conf: String,
    reason: String,
}

fn detail_by_code(c: &Connection, codes: &[String]) -> R<Option<DetailRow>> {
    let sql = format!(
        "SELECT udiDiCode, rawJson, coalesce(intendedUser,''), coalesce(iuConfidence,''), coalesce(iuReason,'') \
         FROM udi_details WHERE udiDiCode IN ({}) LIMIT 1",
        placeholders(codes.len())
    );
    Ok(c.query_row(&sql, rusqlite::params_from_iter(codes), |r| {
        Ok(DetailRow { udi: r.get(0)?, raw: r.get(1)?, intended: r.get(2)?, conf: r.get(3)?, reason: r.get(4)? })
    })
    .optional()?)
}

/// Package-level fallback: the code appears as a `diCode` somewhere in a base
/// unit's detail JSON (its `udiDi.packages` tree). Full scan, so only on a miss.
fn detail_by_package(c: &Connection, codes: &[String]) -> R<Option<DetailRow>> {
    // One pass over the table for all spellings — each extra LIKE scan costs ~5 s on 660k rows.
    let likes = vec!["rawJson LIKE ?"; codes.len()].join(" OR ");
    let sql = format!(
        "SELECT udiDiCode, rawJson, coalesce(intendedUser,''), coalesce(iuConfidence,''), coalesce(iuReason,'') \
         FROM udi_details WHERE ({likes}) AND udiDiCode NOT IN ({}) LIMIT 1",
        placeholders(codes.len())
    );
    let mut params: Vec<String> = codes.iter().map(|c| format!("%\"diCode\":\"{c}\"%")).collect();
    params.extend(codes.iter().cloned());
    Ok(c.query_row(&sql, rusqlite::params_from_iter(params), |r| {
        Ok(DetailRow { udi: r.get(0)?, raw: r.get(1)?, intended: r.get(2)?, conf: r.get(3)?, reason: r.get(4)? })
    })
    .optional()?)
}

/// The MiGeL DB row as (column, value) pairs, all columns, empty ones skipped.
fn migel_row(c: &Connection, codes: &[String]) -> R<Option<Vec<(String, String)>>> {
    let sql = format!("SELECT * FROM swissdamed WHERE udiDiCode IN ({}) LIMIT 1", placeholders(codes.len()));
    let mut st = c.prepare(&sql)?;
    let names: Vec<String> = st.column_names().iter().map(|s| s.to_string()).collect();
    let row = st
        .query_row(rusqlite::params_from_iter(codes), |r| {
            let mut v = Vec::new();
            for (i, n) in names.iter().enumerate() {
                let s: Option<String> = r.get(i)?;
                if let Some(s) = s.filter(|s| !s.is_empty()) {
                    v.push((n.clone(), s));
                }
            }
            Ok(v)
        })
        .optional()?;
    Ok(row)
}

fn print_section(title: &str, rows: &[(String, String)]) {
    println!("  [{title}]");
    let w = rows.iter().map(|(k, _)| k.len()).max().unwrap_or(0).min(60);
    for (k, v) in rows {
        println!("    {k:<w$}  {}", v.replace(['\n', '\r'], " "));
    }
}

pub fn run(inputs: &[String], db_dir: &Path, json: bool) -> R<()> {
    let details_path = crate::details::find_latest_db(db_dir);
    let migel_path = Some(db_dir.join("swissdamed_migel.db")).filter(|p| p.exists());
    let details = details_path.as_deref().map(open_ro).transpose()?;
    let migel = migel_path.as_deref().map(open_ro).transpose()?;
    if !json {
        eprintln!(
            "[gtin] details DB: {}   MiGeL DB: {}",
            details_path.as_ref().map(|p| p.display().to_string()).unwrap_or_else(|| "none".into()),
            migel_path.as_ref().map(|p| p.display().to_string()).unwrap_or_else(|| "none".into()),
        );
    }

    let mut json_out = Vec::new();
    let mut misses = 0;
    for input in inputs {
        let codes = candidates(input);
        let mut via_package = false;
        let mut det = match &details {
            Some(c) => detail_by_code(c, &codes)?,
            None => None,
        };
        if det.is_none() {
            if let Some(c) = &details {
                det = detail_by_package(c, &codes)?;
                via_package = det.is_some();
            }
        }
        // The MiGeL row is keyed on the base unit, so a package hit looks up its owner.
        let mut mcodes = codes.clone();
        if let Some(d) = &det {
            mcodes.push(d.udi.clone());
        }
        let mig = match &migel {
            Some(c) => migel_row(c, &mcodes)?,
            None => None,
        };

        if det.is_none() && mig.is_none() {
            misses += 1;
        }

        if json {
            let raw: Value = det.as_ref().and_then(|d| serde_json::from_str(&d.raw).ok()).unwrap_or(Value::Null);
            let migel_obj: Value = mig
                .as_ref()
                .map(|v| Value::Object(v.iter().map(|(k, s)| (k.clone(), Value::String(s.clone()))).collect()))
                .unwrap_or(Value::Null);
            json_out.push(serde_json::json!({
                "input": input,
                "udiDiCode": det.as_ref().map(|d| d.udi.clone()),
                "matchedAsPackage": via_package,
                "triage": det.as_ref().map(|d| serde_json::json!({
                    "intendedUser": d.intended, "iuConfidence": d.conf, "iuReason": d.reason })),
                "migel": migel_obj,
                "details": raw,
            }));
            continue;
        }

        println!("\n=== {input} ===");
        let Some(d) = det else {
            match mig {
                Some(m) => {
                    println!("  (no row in the details DB — only the MiGeL list row)");
                    print_section("MiGeL match", &m);
                }
                None => println!("  not found in the details or MiGeL DB (tried {})", codes.join(", ")),
            }
            continue;
        };
        if via_package {
            println!("  package UDI-DI of base unit {}", d.udi);
        } else {
            println!("  UDI-DI {}", d.udi);
        }

        let mig_rows = mig.unwrap_or_default();
        let code = mig_rows.iter().find(|(k, _)| k == "migel_code").map(|(_, v)| v.as_str());
        let bez = mig_rows.iter().find(|(k, _)| k == "migel_bezeichnung").map(|(_, v)| v.as_str());
        match code {
            Some(c) => println!("  MiGeL: {c}  {}", bez.unwrap_or("")),
            None => println!("  MiGeL: not matched"),
        }
        println!("  Triage: {}/{}  {}", d.intended, d.conf, d.reason);

        let raw: Value = serde_json::from_str(&d.raw).unwrap_or(Value::Null);
        let obj = raw.as_object().cloned().unwrap_or_default();
        for (key, title) in [("basicUdi", "Basic UDI-DI"), ("udiDi", "UDI-DI detail"), ("ownerInfo", "Owner / mandate")] {
            if let Some(v) = obj.get(key) {
                let mut rows = Vec::new();
                flatten("", v, &mut rows);
                print_section(title, &rows);
            }
        }
        let mut rest = Vec::new();
        for (k, v) in &obj {
            if !["basicUdi", "udiDi", "ownerInfo"].contains(&k.as_str()) {
                flatten(k, v, &mut rest);
            }
        }
        if !rest.is_empty() {
            print_section("Other", &rest);
        }
        if !mig_rows.is_empty() {
            print_section("MiGeL list row", &mig_rows);
        }
    }

    if json {
        println!("{}", serde_json::to_string_pretty(&json_out)?);
    }
    if misses == inputs.len() {
        return Err("no GTIN found".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidates_cover_ean13_and_gtin14() {
        let c = candidates("5900516699673");
        assert!(c.contains(&"5900516699673".to_string()));
        assert!(c.contains(&"05900516699673".to_string()));
        let c = candidates("05900516699673");
        assert!(c.contains(&"5900516699673".to_string()));
    }

    #[test]
    fn candidates_keep_hibcc_as_typed() {
        assert_eq!(candidates("+H123ABC0"), vec!["+H123ABC0".to_string()]);
    }

    #[test]
    fn flatten_keeps_empty_containers_and_indexes_arrays() {
        let v: Value = serde_json::json!({"a": {"b": 1, "c": []}, "d": [{"e": "x"}], "f": null});
        let mut out = Vec::new();
        flatten("", &v, &mut out);
        assert!(out.contains(&("a.b".into(), "1".into())));
        assert!(out.contains(&("a.c".into(), "[]".into())));
        assert!(out.contains(&("d[0].e".into(), "x".into())));
        assert!(out.contains(&("f".into(), "null".into())));
    }
}
