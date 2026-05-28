//! Scrape SIGVARIS Shopify shop (shop.sigvaris.com) and derive MiGeL codes
//! per GTIN, persisting to a dated SQLite DB (`db/sigvaris_shop_DD.MM.YYYY.db`).
//!
//! The shop exposes a public `/products/{handle}.json` per product and
//! `/collections/{handle}/products.json` for enumeration. From each variant
//! we capture: GTIN (barcode), SKU, title, product_type, Klasse (option2),
//! size (option3). The MiGeL code is derived locally from product_type +
//! Klasse using the BAG Kapitel 17 rules (Anti-Thrombose / Stützstrumpf
//! Klasse 1 / Reisestrumpf / Sport are explicitly NOT MiGeL).

use crate::download::http_client;
use crate::export::output_db;
use reqwest::blocking::Client;
use rusqlite::{params, Connection};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

const BASE: &str = "https://shop.sigvaris.com/de-de";

/// Collection handles to enumerate. "all" is a sentinel Shopify collection
/// containing every published product; the rest catch products that may be
/// hidden from "all" (lymphatic / clinical / accessory ranges).
const COLLECTIONS: &[&str] = &[
    "all",
    // Anziehhilfen
    "anziehhilfen-kompressionsstruempfe",
    // Bandagen (Kapitel 05)
    "bandagen",
    "bandagen-ellbogen",
    "bandagen-hand",
    "bandagen-knie",
    "bandagen-nacken",
    "bandagen-ruecken",
    "bandagen-sprunggelenk",
    // Flachstrick (Kapitel 17.15)
    "flachstrick-armversorgungen",
    "flachstrick-beinversorgungen",
    "flachstrickversorgungen",
    // Wraps (Kapitel 17.06)
    "kompressions-wraps",
    "kompressions-wraps-armversorgungen",
    "kompressions-wraps-beinversorgung",
    // Kompressionsstrümpfe — Damen + Herren, by length
    "kompressionsstruempfe",
    "kompressionsstruempfe-damen",
    "kompressionsstruempfe-herren",
    "kompressions-kniestruempfe-damen",
    "kompressions-kniestruempfe-herren",
    "kompressions-schenkelstruempfe-damen",
    "kompressions-schenkelstruempfe-herren",
    "kompressions-strumpfhosen-damen",
    "kompressions-strumpfhosen-herren",
    "kompressions-schwangerschaftsstrumpfhosen",
    "strumpfhosen-schwangerschaft",
    // Non-MiGeL but still scraped (so we have the official "skip" classification)
    "reisestruempfe",
    "stuetzstruempfe",
    "stuetzstruempfe-damen",
    "stuetzstruempfe-herren",
    "zubehoer",
    "pflege-und-zusatzprodukte",
];

#[derive(Debug)]
struct Variant {
    gtin13: String,
    sku: Option<String>,
    title: String,
    product_type: String,
    klasse: Option<u8>,
    migel_code: Option<String>,
    migel_reason: String,
}

pub fn run() -> Result<(), Box<dyn std::error::Error>> {
    let client = http_client()?;

    // Snapshot the existing DB so we can compare against it as a sanity floor.
    let db_dir = crate::app_data_dir().join("db");
    let baseline_variants = find_latest_db(&db_dir)
        .and_then(|p| count_variants(&p).ok())
        .unwrap_or(0);
    if baseline_variants > 0 {
        eprintln!(
            "[sigvaris-shop] Baseline: existing DB has {} variants \
             (new scrape must reach at least 80% to be accepted)",
            baseline_variants
        );
    }

    // Warmup: visit the homepage so Cloudflare seats us with a cookie
    eprintln!("[sigvaris-shop] Warming up Cloudflare session ...");
    let _ = client
        .get(format!("{}/", BASE))
        .header("Accept", "text/html,application/xhtml+xml")
        .send()
        .map(|r| r.text());

    eprintln!("[sigvaris-shop] Discovering product handles ...");
    let handles = discover_handles(&client)?;
    eprintln!("[sigvaris-shop] Found {} distinct product handles", handles.len());

    // Fail-safe: never overwrite the existing DB with an empty result
    // (would happen if Cloudflare rate-limits the discovery endpoints).
    if handles.is_empty() {
        return Err(
            "Discovery returned 0 handles — likely Cloudflare rate-limit. \
             Aborting to preserve the existing SQLite DB. Retry in 10-30 minutes."
                .into(),
        );
    }

    eprintln!("[sigvaris-shop] Fetching product details ...");
    let mut variants: Vec<Variant> = Vec::with_capacity(handles.len() * 30);
    let mut errors = 0usize;
    for (i, handle) in handles.iter().enumerate() {
        match fetch_product_with_retry(&client, handle) {
            Ok(vs) => variants.extend(vs),
            Err(e) => {
                eprintln!("[sigvaris-shop]   error on {}: {}", handle, e);
                errors += 1;
            }
        }
        if (i + 1) % 25 == 0 {
            eprintln!(
                "[sigvaris-shop]   {} / {} products done ({} variants so far, {} errors)",
                i + 1,
                handles.len(),
                variants.len(),
                errors,
            );
        }
        // Polite throttle: ~1 req/sec average → 432 products ≈ 7 minutes
        thread::sleep(Duration::from_millis(1000));
    }
    eprintln!(
        "[sigvaris-shop] Fetched {} variants from {} products ({} errors)",
        variants.len(),
        handles.len(),
        errors
    );

    // Fail-safe: refuse to overwrite a known-good baseline DB with a partial
    // scrape. Cloudflare rate-limits sometimes block most discovery requests
    // but leave a handful of handles reachable, producing a tiny DB that
    // would silently destroy the bulk of our GTIN→MiGeL overrides.
    if baseline_variants > 0 {
        let min_acceptable = (baseline_variants as f64 * 0.8) as usize;
        if variants.len() < min_acceptable {
            return Err(format!(
                "Scrape produced {} variants but baseline has {} (threshold {} \
                 = 80%). Refusing to overwrite the existing DB. Retry in 1-2 \
                 hours when Cloudflare backs off.",
                variants.len(),
                baseline_variants,
                min_acceptable,
            )
            .into());
        }
    }

    let db_path = output_db("sigvaris_shop")?;
    write_db(&variants, &db_path)?;
    eprintln!("[sigvaris-shop] SQLite written: {}", db_path);

    // Summary
    let mapped = variants.iter().filter(|v| v.migel_code.is_some()).count();
    eprintln!(
        "[sigvaris-shop] Summary: {} variants total, {} mapped to MiGeL, {} skipped (non-MiGeL)",
        variants.len(),
        mapped,
        variants.len() - mapped
    );

    Ok(())
}

fn discover_handles(client: &Client) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut all: HashSet<String> = HashSet::new();
    for col in COLLECTIONS {
        for page in 1..20u32 {
            let url = format!("{}/collections/{}/products.json?limit=250&page={}", BASE, col, page);
            // retry on 403 (Cloudflare rate-limit)
            let mut resp_opt = None;
            for wait in [0u64, 10, 30] {
                if wait > 0 {
                    eprintln!("[sigvaris-shop]   discovery 403 on {} p{}; sleep {}s", col, page, wait);
                    thread::sleep(Duration::from_secs(wait));
                }
                let r = client.get(&url).header("Accept", "application/json").send()?;
                if r.status() == reqwest::StatusCode::FORBIDDEN {
                    continue;
                }
                resp_opt = Some(r);
                break;
            }
            let resp = match resp_opt {
                Some(r) => r,
                None => break, // give up this collection
            };
            if !resp.status().is_success() {
                break;
            }
            let body: Value = match resp.json() {
                Ok(j) => j,
                Err(_) => break,
            };
            let products = body.get("products").and_then(|v| v.as_array());
            let products = match products {
                Some(arr) if !arr.is_empty() => arr,
                _ => break,
            };
            for p in products {
                if let Some(h) = p.get("handle").and_then(|v| v.as_str()) {
                    all.insert(h.to_string());
                }
            }
            if products.len() < 250 {
                break;
            }
            thread::sleep(Duration::from_millis(800));
        }
        // Throttle between collections too
        thread::sleep(Duration::from_millis(500));
    }
    let mut v: Vec<String> = all.into_iter().collect();
    v.sort();
    Ok(v)
}

/// Retry on 403 (Cloudflare rate-limit) with exponential backoff.
fn fetch_product_with_retry(
    client: &Client,
    handle: &str,
) -> Result<Vec<Variant>, Box<dyn std::error::Error>> {
    let backoffs_secs: [u64; 3] = [10, 30, 60];
    let mut last_err: Option<Box<dyn std::error::Error>> = None;
    for (attempt, wait) in std::iter::once(0u64).chain(backoffs_secs.into_iter()).enumerate() {
        if wait > 0 {
            eprintln!(
                "[sigvaris-shop]   retry {}/{} for {} after {}s ...",
                attempt,
                backoffs_secs.len(),
                handle,
                wait
            );
            thread::sleep(Duration::from_secs(wait));
        }
        match fetch_product(client, handle) {
            Ok(v) => return Ok(v),
            Err(e) => {
                let msg = e.to_string();
                last_err = Some(e);
                if !msg.contains("403") {
                    break; // non-rate-limit errors: don't retry
                }
            }
        }
    }
    Err(last_err.unwrap_or_else(|| "unknown error".into()))
}

fn fetch_product(client: &Client, handle: &str) -> Result<Vec<Variant>, Box<dyn std::error::Error>> {
    let url = format!("{}/products/{}.json", BASE, handle);
    let resp = client
        .get(&url)
        .header("Accept", "application/json, text/plain, */*")
        .header("Accept-Language", "de-DE,de;q=0.9,en;q=0.8")
        .header("Referer", format!("{}/products/{}", BASE, handle))
        .send()?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()).into());
    }
    let body: Value = resp.json()?;
    let product = body.get("product").ok_or("no product field")?;
    let title = product
        .get("title")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let product_type = product
        .get("product_type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let empty_vec: Vec<Value> = Vec::new();
    let variants = product
        .get("variants")
        .and_then(|v| v.as_array())
        .unwrap_or(&empty_vec);

    let mut out = Vec::with_capacity(variants.len());
    for v in variants {
        let barcode = v
            .get("barcode")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if barcode.is_empty() {
            continue;
        }
        let sku = v
            .get("sku")
            .and_then(|x| x.as_str())
            .filter(|s| !s.is_empty())
            .map(String::from);
        let o2 = v.get("option2").and_then(|x| x.as_str()).unwrap_or("");
        let o3 = v.get("option3").and_then(|x| x.as_str()).unwrap_or("");
        let klasse = parse_klasse(o2).or_else(|| parse_klasse(o3));
        let (migel_code, migel_reason) = derive_migel(&title, &product_type, klasse);
        out.push(Variant {
            gtin13: barcode,
            sku,
            title: title.clone(),
            product_type: product_type.clone(),
            klasse,
            migel_code,
            migel_reason,
        });
    }
    Ok(out)
}

/// Parse "Klasse 1 (18-21 mmHg)" / "2332mmhg" / "Klasse 3" → 1/2/3/4
fn parse_klasse(s: &str) -> Option<u8> {
    if s.is_empty() {
        return None;
    }
    let lower = s.to_lowercase();
    let cleaned: String = lower
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '-' && *c != '(' && *c != ')' && *c != ',')
        .collect();
    if let Some(pos) = cleaned.find("klasse") {
        let rest = &cleaned[pos + 6..];
        if let Some(c) = rest.chars().next() {
            if let Some(d) = c.to_digit(10) {
                if (1..=4).contains(&d) {
                    return Some(d as u8);
                }
            }
        }
    }
    if cleaned.contains("1821") {
        return Some(1);
    }
    if cleaned.contains("2332") {
        return Some(2);
    }
    if cleaned.contains("3446") {
        return Some(3);
    }
    if cleaned.contains("49") {
        return Some(4);
    }
    None
}

/// Derive the MiGeL code from product title + type + Klasse.
/// Returns (Some(code), reason) on a MiGeL hit, (None, reason) on a deliberate skip.
fn derive_migel(title: &str, product_type: &str, klasse: Option<u8>) -> (Option<String>, String) {
    let s = format!("{} {}", title, product_type).to_lowercase();

    // Explicit non-MiGeL per BAG Kap. 17 intro
    if s.contains("stützstrümpfe") || s.contains("stuetzstruempfe") {
        return (None, "Stützstrumpf".into());
    }
    if s.contains("reisestrümpfe") || s.contains("reisestruempfe") || s.contains("traveno") {
        return (None, "Reisestrumpf".into());
    }
    if s.contains("laufsocken") || s.contains("sport > ") {
        return (None, "Sport".into());
    }
    if s.contains("thrombo") {
        return (None, "Anti-Thrombose".into());
    }

    // MiGeL 17.05 — Spezielle Kompressionsstrümpfe
    if s.contains("ulcer") {
        return (Some("17.05.01.00.1".into()), "Ulcus cruris System".into());
    }
    if s.contains("diabetic") || s.contains("diabetes") {
        return (Some("17.05.02.00.1".into()), "Diabetes-Kompressionsstrumpf".into());
    }

    // MiGeL 17.06 — Medizinisch adaptive Kompressionssysteme (Wraps)
    if s.contains("wrap") || s.contains("manuwrap") || s.contains("genuwrap") {
        if s.contains("arm") {
            return (Some("17.06.01.00.1".into()), "MAK Arm".into());
        }
        if s.contains("knie") || s.contains("genu") {
            return (Some("17.06.01.03.1".into()), "MAK Knie".into());
        }
        if s.contains("oberschenkel") {
            return (Some("17.06.01.04.1".into()), "MAK Oberschenkel".into());
        }
        if s.contains("fuss") {
            return (Some("17.06.01.01.1".into()), "MAK Fuss".into());
        }
        return (Some("17.06.01.02.1".into()), "MAK Wade (default)".into());
    }

    // MiGeL 17.12 — Anziehhilfen
    if ["doff", "donner", "magnide", "simslide", "gleitsocke", "gleithilfe"]
        .iter()
        .any(|k| s.contains(k))
    {
        if s.contains("rolly") || s.contains("cone") || s.contains("rahmen") {
            return (Some("17.12.01.01.1".into()), "Anziehhilfe Rahmengestell".into());
        }
        return (Some("17.12.01.00.1".into()), "Anziehhilfe Gleithilfe".into());
    }

    // MiGeL 17.15 — Flachstrick (Massanfertigung, flachgestrickt)
    if s.contains("flachstrick") {
        if s.contains("arm") {
            return (Some("17.15.03.00.1".into()), "Flachstrick Arm".into());
        }
        if s.contains("hand") {
            return (Some("17.15.02.00.1".into()), "Flachstrick Hand".into());
        }
        if s.contains("bein") || s.contains("leg") {
            return (Some("17.15.01.00.1".into()), "Flachstrick Bein".into());
        }
        if s.contains("rumpf") || s.contains("leib") {
            return (Some("17.15.04.00.1".into()), "Flachstrick Rumpf".into());
        }
        if s.contains("kopf") || s.contains("hals") {
            return (Some("17.15.05.00.1".into()), "Flachstrick Kopf/Hals".into());
        }
    }

    // Bandagen (MiGeL Kap. 05)
    if s.contains("bandagen >") || product_type.to_lowercase().starts_with("bandagen") {
        if s.contains("knie") || s.contains("genu") {
            return (Some("05.04.11.00.1".into()), "Bandage Knie".into());
        }
        if s.contains("sprunggelenk") || s.contains("malleo") {
            return (Some("05.02.11.00.1".into()), "Bandage Sprunggelenk".into());
        }
        if s.contains("hand") || s.contains("manu") {
            return (Some("05.07.11.00.1".into()), "Bandage Hand".into());
        }
        if s.contains("ellbogen") || s.contains("elbow") || s.contains("epi") {
            return (Some("05.08.11.00.1".into()), "Bandage Ellbogen".into());
        }
        if s.contains("nacken") || s.contains("cervi") {
            return (Some("22.12.01.00.1".into()), "Bandage Nacken (Cervikalstütze)".into());
        }
        if s.contains("rücken") || s.contains("lumbo") {
            return (Some("05.14.11.00.1".into()), "Bandage Rücken".into());
        }
        if s.contains("daumen") {
            return (Some("05.07.11.00.1".into()), "Bandage Daumen→Hand".into());
        }
        return (None, "Bandage unbekannt".into());
    }

    // MiGeL 17.02 / 17.03 — medizinische Kompressionsstrümpfe Klasse 2 / 3+4
    let klasse = match klasse {
        Some(k) if k >= 2 => k,
        _ => return (None, "Klasse 1 oder unbekannt".into()),
    };
    let chapter = if klasse == 2 { "17.02" } else { "17.03" };

    if s.contains("arm") && (s.contains("sleeve") || s.contains("armkompr")) {
        if chapter == "17.02" {
            return (Some("17.02.01.11.1".into()), "Armkompressionsstrumpf Kl.2 Serien".into());
        }
        return (Some("17.03.01.10.1".into()), "Armkompressionsstrumpf Kl.3/4 nach Mass".into());
    }
    if s.contains("strumpfhose") && (s.contains("maternity") || s.contains("schwanger")) {
        if chapter == "17.02" {
            return (Some("17.02.01.09.1".into()), "Maternity Strumpfhose Kl.2 Serien".into());
        }
        return (Some("17.03.01.07.1".into()), "Strumpfhose Kl.3/4 Serien (Maternity n/a)".into());
    }
    if s.contains("strumpfhose") {
        return (Some(format!("{}.01.07.1", chapter)), format!("Strumpfhose Kl.{} Serien", klasse));
    }
    if s.contains("halbschenkel") {
        return (Some(format!("{}.01.03.1", chapter)), format!("Halbschenkelstrumpf Kl.{} Serien", klasse));
    }
    if s.contains("schenkelstrumpf") || (s.contains("schenkel") && !s.contains("halb")) {
        return (Some(format!("{}.01.05.1", chapter)), format!("Schenkelstrumpf Kl.{} Serien", klasse));
    }
    if s.contains("kniestrumpf") || s.contains("kniest") || s.contains("calf") || s.contains("wadenstrumpf") {
        return (Some(format!("{}.01.01.1", chapter)), format!("Wadenstrumpf Kl.{} Serien", klasse));
    }

    (None, "Anatomie unklar".into())
}

/// GTIN → override decision: `Some(code)` means use this MiGeL code,
/// `None` means explicitly skip (SIGVARIS classifies it as Stützstrumpf /
/// Anti-Thrombose / Reisestrumpf / Klasse 1, i.e. not a MiGeL Pflichtleistung).
pub type Overrides = HashMap<String, Option<String>>;

/// Locate the most recent `db/sigvaris_shop_*.db` under the app data dir.
pub fn find_latest_db(db_dir: &Path) -> Option<PathBuf> {
    let mut newest: Option<(std::time::SystemTime, PathBuf)> = None;
    if let Ok(entries) = std::fs::read_dir(db_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let name = match path.file_name().and_then(|n| n.to_str()) {
                Some(n) => n,
                None => continue,
            };
            if !name.starts_with("sigvaris_shop_") || !name.ends_with(".db") {
                continue;
            }
            if let Ok(meta) = entry.metadata() {
                if let Ok(mtime) = meta.modified() {
                    let take = newest.as_ref().map_or(true, |(t, _)| mtime > *t);
                    if take {
                        newest = Some((mtime, path));
                    }
                }
            }
        }
    }
    newest.map(|(_, p)| p)
}

/// Count rows in the sigvaris_shop_variants table for the given DB. Used as
/// the baseline floor when deciding whether to accept a fresh scrape.
fn count_variants(db_path: &Path) -> Result<usize, Box<dyn std::error::Error>> {
    let conn = Connection::open(db_path)?;
    let n: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sigvaris_shop_variants",
        [],
        |r| r.get(0),
    )?;
    Ok(n.max(0) as usize)
}

/// Load all GTIN → override entries from the given sigvaris_shop DB.
/// Keys are indexed by both gtin14 (with leading zero, matches swissdamed)
/// and gtin13 (raw shop EAN-13) so callers can look up either format.
pub fn load_overrides(db_path: &Path) -> Result<Overrides, Box<dyn std::error::Error>> {
    let conn = Connection::open(db_path)?;
    let mut stmt = conn.prepare("SELECT gtin13, gtin14, migel_code FROM sigvaris_shop_variants")?;
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, Option<String>>(2)?,
        ))
    })?;
    let mut map: Overrides = HashMap::new();
    for row in rows {
        let (g13, g14, code) = row?;
        map.insert(g14, code.clone());
        map.insert(g13, code);
    }
    Ok(map)
}

fn write_db(variants: &[Variant], path: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Fresh DB: drop file if it exists, recreate
    let _ = std::fs::remove_file(path);
    let mut conn = Connection::open(path)?;
    conn.execute_batch(
        "CREATE TABLE sigvaris_shop_variants (
            gtin13 TEXT NOT NULL,
            gtin14 TEXT NOT NULL,
            sku TEXT,
            title TEXT NOT NULL,
            product_type TEXT,
            klasse INTEGER,
            migel_code TEXT,
            migel_reason TEXT
         );
         CREATE INDEX idx_sigvaris_gtin13 ON sigvaris_shop_variants(gtin13);
         CREATE INDEX idx_sigvaris_gtin14 ON sigvaris_shop_variants(gtin14);
         CREATE INDEX idx_sigvaris_migel ON sigvaris_shop_variants(migel_code);
         CREATE INDEX idx_sigvaris_klasse ON sigvaris_shop_variants(klasse);
         CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);",
    )?;

    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare(
            "INSERT INTO sigvaris_shop_variants \
             (gtin13, gtin14, sku, title, product_type, klasse, migel_code, migel_reason) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )?;
        for v in variants {
            let gtin14 = if v.gtin13.len() == 13 {
                format!("0{}", v.gtin13)
            } else {
                v.gtin13.clone()
            };
            stmt.execute(params![
                v.gtin13,
                gtin14,
                v.sku,
                v.title,
                v.product_type,
                v.klasse,
                v.migel_code,
                v.migel_reason,
            ])?;
        }
    }
    tx.execute(
        "INSERT INTO meta (key, value) VALUES ('source', ?1), ('scraped_at', ?2), ('variant_count', ?3)",
        params![
            "https://shop.sigvaris.com/de-de/",
            chrono::Local::now().format("%Y-%m-%d").to_string(),
            variants.len().to_string(),
        ],
    )?;
    tx.commit()?;

    Ok(())
}
