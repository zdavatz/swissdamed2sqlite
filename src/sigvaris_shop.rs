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

#[derive(Debug, Clone)]
struct Variant {
    handle: String,
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

    let db_dir = crate::app_data_dir().join("db");
    let partial_path = db_dir.join("sigvaris_shop_partial.db");

    // Snapshot the baseline DB (variant count + handle→variants cache used
    // when a fresh fetch fails or discovery is Cloudflare-blocked).
    let baseline_path = find_latest_db(&db_dir);
    let baseline_variants = baseline_path
        .as_ref()
        .and_then(|p| count_variants(p).ok())
        .unwrap_or(0);
    let baseline_cache: HashMap<String, Vec<Variant>> = baseline_path
        .as_ref()
        .map(|p| load_variants_by_handle(p).unwrap_or_default())
        .unwrap_or_default();
    if baseline_variants > 0 {
        eprintln!(
            "[sigvaris-shop] Baseline: {} variants in latest DB, {} known handles available \
             as resume fallback (new scrape must reach 80% to be accepted)",
            baseline_variants,
            baseline_cache.len(),
        );
    }

    // Resume from a previous partial scrape, if any. Already-processed handles
    // are skipped during the fetch loop; their variants stay in the partial DB.
    ensure_partial_db(&partial_path)?;
    let already_done: HashSet<String> = list_handles_in_db(&partial_path)?.into_iter().collect();
    if !already_done.is_empty() {
        let n = count_variants(&partial_path).unwrap_or(0);
        eprintln!(
            "[sigvaris-shop] Resuming from {}: {} handles, {} variants already cached",
            partial_path.display(),
            already_done.len(),
            n
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
    let discovered = discover_handles(&client).unwrap_or_else(|e| {
        eprintln!("[sigvaris-shop] Discovery error ({}); will rely on baseline handles", e);
        Vec::new()
    });
    eprintln!("[sigvaris-shop] Found {} distinct product handles via discovery", discovered.len());

    // Union of fresh discovery + baseline handles (latest finalized DB) +
    // already-done partial handles. This way, even when Cloudflare blocks
    // most discovery requests, we still iterate the known product universe
    // and either refresh successfully or fall back to cached data.
    let mut all_handles: HashSet<String> = discovered.iter().cloned().collect();
    for h in baseline_cache.keys() {
        all_handles.insert(h.clone());
    }
    for h in already_done.iter() {
        all_handles.insert(h.clone());
    }
    let mut handles: Vec<String> = all_handles.into_iter().collect();
    handles.sort();
    eprintln!(
        "[sigvaris-shop] Total master handle list: {} (discovery {} + baseline {} + partial {})",
        handles.len(),
        discovered.len(),
        baseline_cache.len(),
        already_done.len()
    );

    if handles.is_empty() {
        return Err(
            "No handles to process: discovery returned 0 and no baseline DB is \
             available. Aborting. Retry in 10-30 minutes."
                .into(),
        );
    }

    eprintln!("[sigvaris-shop] Fetching product details ...");
    let mut new_fetches = 0usize;
    let mut errors = 0usize;
    let mut fallbacks = 0usize;
    let total = handles.len();
    let to_process: Vec<&String> = handles.iter().filter(|h| !already_done.contains(*h)).collect();
    for (i, handle) in to_process.iter().enumerate() {
        let result = fetch_product_with_retry(&client, handle);
        let variants_for_handle: Vec<Variant> = match result {
            Ok(vs) => {
                new_fetches += 1;
                vs
            }
            Err(e) => {
                if let Some(cached) = baseline_cache.get(handle.as_str()) {
                    eprintln!(
                        "[sigvaris-shop]   error on {}: {} — using {} cached variants from baseline",
                        handle,
                        e,
                        cached.len()
                    );
                    fallbacks += 1;
                    cached.clone()
                } else {
                    eprintln!("[sigvaris-shop]   error on {}: {} (no baseline cache)", handle, e);
                    errors += 1;
                    Vec::new()
                }
            }
        };
        append_to_partial(&partial_path, handle, &variants_for_handle)?;
        if (i + 1) % 25 == 0 {
            let cur = count_variants(&partial_path).unwrap_or(0);
            eprintln!(
                "[sigvaris-shop]   {} / {} processed ({} fetched, {} fallback, {} errors, {} variants in partial)",
                i + 1,
                to_process.len(),
                new_fetches,
                fallbacks,
                errors,
                cur,
            );
        }
        thread::sleep(Duration::from_millis(1000));
    }
    let final_variants = count_variants(&partial_path).unwrap_or(0);
    eprintln!(
        "[sigvaris-shop] Done: {} variants in partial DB ({} master handles, {} new fetches, \
         {} cache fallbacks, {} errors)",
        final_variants, total, new_fetches, fallbacks, errors
    );

    // Fail-safe: refuse to finalize a partial DB significantly smaller than the
    // baseline. Cloudflare may have blocked the bulk of fetches AND we had no
    // baseline cache for those handles → finalizing would destroy overrides.
    if baseline_variants > 0 {
        let min_acceptable = (baseline_variants as f64 * 0.8) as usize;
        if final_variants < min_acceptable {
            return Err(format!(
                "Scrape produced {} variants but baseline has {} (threshold {} \
                 = 80%). Refusing to finalize. Partial DB preserved at {} — \
                 rerun --sigvaris-shop in 1-2 hours to resume.",
                final_variants,
                baseline_variants,
                min_acceptable,
                partial_path.display(),
            )
            .into());
        }
    }

    // Finalize: stamp meta then rename partial → dated DB
    {
        let conn = Connection::open(&partial_path)?;
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES \
             ('source', ?1), ('scraped_at', ?2), ('variant_count', ?3)",
            params![
                "https://shop.sigvaris.com/de-de/",
                chrono::Local::now().format("%Y-%m-%d").to_string(),
                final_variants.to_string(),
            ],
        )?;
    }
    let db_path = output_db("sigvaris_shop")?;
    std::fs::rename(&partial_path, &db_path)?;
    eprintln!("[sigvaris-shop] SQLite written: {}", db_path);

    // Summary
    let mapped: i64 = {
        let conn = Connection::open(&db_path)?;
        conn.query_row(
            "SELECT COUNT(*) FROM sigvaris_shop_variants WHERE migel_code IS NOT NULL",
            [],
            |r| r.get(0),
        )?
    };
    eprintln!(
        "[sigvaris-shop] Summary: {} variants total, {} mapped to MiGeL, {} skipped (non-MiGeL)",
        final_variants,
        mapped,
        final_variants as i64 - mapped
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
            handle: handle.to_string(),
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

/// Locate the most recent finalized `db/sigvaris_shop_DD.MM.YYYY.db` under
/// the app data dir. The in-progress partial DB (`sigvaris_shop_partial.db`)
/// is explicitly excluded — picking it as baseline would let the 80 %
/// threshold trivially compare today's partial against itself.
///
/// Date is parsed from the filename (so a manually restored older DB takes
/// priority correctly even if mtime is fresh); ties fall back to mtime.
pub fn find_latest_db(db_dir: &Path) -> Option<PathBuf> {
    let mut newest: Option<(String, std::time::SystemTime, PathBuf)> = None;
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
            // Exclude the partial / work-in-progress checkpoint
            if name == "sigvaris_shop_partial.db" {
                continue;
            }
            // Extract DD.MM.YYYY → sort key YYYYMMDD; non-conforming names sort last.
            let date_part = name
                .strip_prefix("sigvaris_shop_")
                .and_then(|s| s.strip_suffix(".db"))
                .unwrap_or("");
            let sort_key = match date_part.split('.').collect::<Vec<_>>().as_slice() {
                [d, m, y] if d.len() == 2 && m.len() == 2 && y.len() == 4 => {
                    format!("{}{}{}", y, m, d)
                }
                _ => continue, // skip non-dated files
            };
            let mtime = entry
                .metadata()
                .and_then(|m| m.modified())
                .unwrap_or(std::time::UNIX_EPOCH);
            let take = newest.as_ref().map_or(true, |(k, t, _)| {
                sort_key > *k || (sort_key == *k && mtime > *t)
            });
            if take {
                newest = Some((sort_key, mtime, path));
            }
        }
    }
    newest.map(|(_, _, p)| p)
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

fn ensure_partial_db(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(path)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sigvaris_shop_variants (
            handle TEXT NOT NULL DEFAULT '',
            gtin13 TEXT NOT NULL,
            gtin14 TEXT NOT NULL,
            sku TEXT,
            title TEXT NOT NULL,
            product_type TEXT,
            klasse INTEGER,
            migel_code TEXT,
            migel_reason TEXT
         );
         CREATE INDEX IF NOT EXISTS idx_sigvaris_gtin13 ON sigvaris_shop_variants(gtin13);
         CREATE INDEX IF NOT EXISTS idx_sigvaris_gtin14 ON sigvaris_shop_variants(gtin14);
         CREATE INDEX IF NOT EXISTS idx_sigvaris_migel ON sigvaris_shop_variants(migel_code);
         CREATE INDEX IF NOT EXISTS idx_sigvaris_klasse ON sigvaris_shop_variants(klasse);
         CREATE INDEX IF NOT EXISTS idx_sigvaris_handle ON sigvaris_shop_variants(handle);
         CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
         CREATE TABLE IF NOT EXISTS done_handles (handle TEXT PRIMARY KEY);",
    )?;
    Ok(())
}

fn append_to_partial(
    path: &Path,
    handle: &str,
    variants: &[Variant],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = Connection::open(path)?;
    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare(
            "INSERT INTO sigvaris_shop_variants \
             (handle, gtin13, gtin14, sku, title, product_type, klasse, migel_code, migel_reason) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        )?;
        for v in variants {
            let gtin14 = if v.gtin13.len() == 13 {
                format!("0{}", v.gtin13)
            } else {
                v.gtin13.clone()
            };
            stmt.execute(params![
                handle,
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
        "INSERT OR IGNORE INTO done_handles (handle) VALUES (?1)",
        params![handle],
    )?;
    tx.commit()?;
    Ok(())
}

fn list_handles_in_db(path: &Path) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let conn = Connection::open(path)?;
    let table_exists: bool = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='done_handles'",
            [],
            |_| Ok(true),
        )
        .unwrap_or(false);
    if !table_exists {
        return Ok(Vec::new());
    }
    let mut stmt = conn.prepare("SELECT handle FROM done_handles")?;
    let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

/// Load variants from a finalized DB grouped by `handle`. Returns an empty
/// map if the DB doesn't have a `handle` column (older schema) — callers can
/// still use `count_variants` and other handle-agnostic helpers.
fn load_variants_by_handle(
    path: &Path,
) -> Result<HashMap<String, Vec<Variant>>, Box<dyn std::error::Error>> {
    let conn = Connection::open(path)?;
    let has_handle: bool = conn
        .query_row(
            "SELECT 1 FROM pragma_table_info('sigvaris_shop_variants') WHERE name='handle'",
            [],
            |_| Ok(true),
        )
        .unwrap_or(false);
    if !has_handle {
        return Ok(HashMap::new());
    }
    let mut stmt = conn.prepare(
        "SELECT handle, gtin13, sku, title, product_type, klasse, migel_code, migel_reason \
         FROM sigvaris_shop_variants WHERE handle != ''",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok(Variant {
            handle: r.get(0)?,
            gtin13: r.get(1)?,
            sku: r.get(2)?,
            title: r.get(3)?,
            product_type: r.get(4)?,
            klasse: r.get(5)?,
            migel_code: r.get(6)?,
            migel_reason: r.get(7)?,
        })
    })?;
    let mut map: HashMap<String, Vec<Variant>> = HashMap::new();
    for r in rows {
        let v = r?;
        map.entry(v.handle.clone()).or_default().push(v);
    }
    Ok(map)
}

#[allow(dead_code)]
fn write_db(variants: &[Variant], path: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Fresh DB: drop file if it exists, recreate
    let _ = std::fs::remove_file(path);
    let mut conn = Connection::open(path)?;
    conn.execute_batch(
        "CREATE TABLE sigvaris_shop_variants (
            handle TEXT NOT NULL DEFAULT '',
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
         CREATE INDEX idx_sigvaris_handle ON sigvaris_shop_variants(handle);
         CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);",
    )?;

    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare(
            "INSERT INTO sigvaris_shop_variants \
             (handle, gtin13, gtin14, sku, title, product_type, klasse, migel_code, migel_reason) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        )?;
        for v in variants {
            let gtin14 = if v.gtin13.len() == 13 {
                format!("0{}", v.gtin13)
            } else {
                v.gtin13.clone()
            };
            stmt.execute(params![
                v.handle,
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
