//! `--details` mode: enrich UDI devices with their per-device detail record.
//!
//! The public list endpoint (`POST /public/udi/basic-udis`, used by the main
//! download) returns only a slim summary. The full regulated attribute set —
//! EMDN nomenclature (the structured "Zweckbestimmung"), the free-text
//! additional description, and the MDR/IVD yes/no characteristics — lives behind
//! a separate per-device endpoint:
//!
//! ```text
//! GET https://swissdamed.ch/public/udi/udi-dis/{udiDiId}/details
//! ```
//!
//! where `{udiDiId}` is the UUID `id` of a nested `udiDis[]` entry (NOT the
//! `udiDiCode`/GTIN). It is public but sits behind a load-balancer sticky-session
//! cookie (`sm-cookie-be`): the first request answers `302` + Set-Cookie and
//! redirects to the same URL, which then serves `200`. The shared
//! [`crate::download::http_client`] already enables a cookie store and follows
//! redirects, so a plain `GET` transparently handles that handshake.

use serde_json::Value;
use std::thread;
use std::time::Duration;

use crate::download::http_client;
use crate::export;

const LIST_URL: &str = "https://swissdamed.ch/public/udi/basic-udis";

/// Identity of one marketed device (a `udiDis[]` entry) taken from the list.
struct UdiRef {
    udi_di_id: String,
    udi_di_code: String,
    basic_udi_di_code: String,
    company_name: String,
    device_name: String,
    device_type: String,
    risk_class: String,
    market_status: String,
    /// `udiDis[].lastModifiedAt` — the change key for incremental updates.
    last_modified_at: String,
}

/// Flatten already-downloaded basic-UDI list values into one [`UdiRef`] per
/// `udiDis[]` entry. Used by the `--migel` daily hook, which reuses the UDI
/// download instead of paging the list a second time.
fn refs_from_values(values: &[Value]) -> Vec<UdiRef> {
    let mut refs = Vec::new();
    for rec in values {
        let basic_udi_di_code = s(rec, "basicUdiDiCode");
        let company_name = s(rec, "companyName");
        let device_name = s(rec, "deviceName");
        let device_type = s(rec, "deviceType");
        let risk_class = s(rec, "riskClass");
        if let Some(udis) = rec.get("udiDis").and_then(|v| v.as_array()) {
            for u in udis {
                let id = s(u, "id");
                if id.is_empty() {
                    continue;
                }
                refs.push(UdiRef {
                    udi_di_id: id,
                    udi_di_code: s(u, "udiDiCode"),
                    basic_udi_di_code: basic_udi_di_code.clone(),
                    company_name: company_name.clone(),
                    device_name: device_name.clone(),
                    device_type: device_type.clone(),
                    risk_class: risk_class.clone(),
                    market_status: s(u, "marketStatus"),
                    last_modified_at: s(u, "lastModifiedAt"),
                });
            }
        }
    }
    refs
}

fn s(v: &Value, key: &str) -> String {
    v.get(key).and_then(|x| x.as_str()).unwrap_or("").to_string()
}

/// A boolean field rendered as "true"/"false", or "" when absent.
fn b(v: &Value, key: &str) -> String {
    match v.get(key) {
        Some(Value::Bool(x)) => x.to_string(),
        _ => String::new(),
    }
}

/// Page through the basic-UDI list, flattening to one [`UdiRef`] per `udiDis[]`
/// entry, stopping once `limit` refs are collected (`limit == 0` → all).
fn collect_udi_refs(
    client: &reqwest::blocking::Client,
    page_size: u32,
    limit: u32,
) -> Result<Vec<UdiRef>, Box<dyn std::error::Error>> {
    let mut refs: Vec<UdiRef> = Vec::new();
    let mut page: u32 = 0;

    loop {
        let url = format!("{}?page={}&size={}", LIST_URL, page, page_size);
        eprintln!("[details] Listing basic-UDIs page {} ...", page);
        let resp = client
            .post(&url)
            .header("Accept", "application/json, text/plain, */*")
            .header("Content-Type", "application/json")
            .body("{}")
            .send()?;
        if !resp.status().is_success() {
            return Err(format!("HTTP {} listing page {}", resp.status(), page).into());
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

        for rec in values {
            let basic_udi_di_code = s(rec, "basicUdiDiCode");
            let company_name = s(rec, "companyName");
            let device_name = s(rec, "deviceName");
            let device_type = s(rec, "deviceType");
            let risk_class = s(rec, "riskClass");
            if let Some(udis) = rec.get("udiDis").and_then(|v| v.as_array()) {
                for u in udis {
                    let id = s(u, "id");
                    if id.is_empty() {
                        continue;
                    }
                    refs.push(UdiRef {
                        udi_di_id: id,
                        udi_di_code: s(u, "udiDiCode"),
                        basic_udi_di_code: basic_udi_di_code.clone(),
                        company_name: company_name.clone(),
                        device_name: device_name.clone(),
                        device_type: device_type.clone(),
                        risk_class: risk_class.clone(),
                        market_status: s(u, "marketStatus"),
                        last_modified_at: s(u, "lastModifiedAt"),
                    });
                    if limit != 0 && refs.len() as u32 >= limit {
                        eprintln!("[details] Collected {} udiDi refs (limit reached).", refs.len());
                        return Ok(refs);
                    }
                }
            }
        }

        eprintln!("[details]   {} refs so far", refs.len());
        if (count as u32) < page_size {
            break;
        }
        page += 1;
    }

    eprintln!("[details] Collected {} udiDi refs total.", refs.len());
    Ok(refs)
}

/// Columns written to the `udi_details` table (all TEXT, per project convention).
const HEADERS: &[&str] = &[
    // identity (from the list)
    "udiDiCode",
    "udiDiId",
    "basicUdiDiCode",
    "companyName",
    "deviceName",
    "deviceType",
    "riskClass",
    "marketStatus",
    "lastModifiedAt",
    // intended-purpose signal (from /details)
    "emdnCode",
    "emdnTerm",
    "additionalDescription",
    "eudamedBasicUdiUri",
    // MDR characteristics
    "implantable",
    "reusable",
    "active",
    "sterile",
    "sterilization",
    "measuringFunction",
    "administeringMedicine",
    "latex",
    "animalTissuesCells",
    "humanTissuesCells",
    // IVD characteristics
    "selfTesting",
    "nearPatientTesting",
    "professionalTesting",
    "companionDiagnostics",
    "reagent",
    "instrument",
    "kit",
    "microbialSubstances",
    // full raw detail response — catch-all so no field is ever lost
    "rawJson",
    // triage classification (public vs professional) — decision-support only
    "intendedUser",
    "iuConfidence",
    "iuReason",
];

/// Join a `nomenclatureCodes` array into (codes, terms) with ";" separators.
fn emdn(udi_di: &Value) -> (String, String) {
    let mut codes = Vec::new();
    let mut terms = Vec::new();
    if let Some(arr) = udi_di.get("nomenclatureCodes").and_then(|v| v.as_array()) {
        for nc in arr {
            let c = s(nc, "code");
            let t = s(nc, "term");
            if !c.is_empty() {
                codes.push(c);
            }
            if !t.is_empty() {
                terms.push(t);
            }
        }
    }
    (codes.join("; "), terms.join("; "))
}

/// Pick the additional description text, preferring the EN entry.
fn additional_description(udi_di: &Value) -> String {
    let arr = match udi_di.get("additionalDescription").and_then(|v| v.as_array()) {
        Some(a) => a,
        None => return String::new(),
    };
    if let Some(en) = arr
        .iter()
        .find(|e| e.get("language").and_then(|l| l.as_str()) == Some("EN"))
    {
        return s(en, "textValue");
    }
    arr.first().map(|e| s(e, "textValue")).unwrap_or_default()
}

/// Build one output row from a list ref + its fetched detail record.
fn build_row(r: &UdiRef, detail: &Value) -> Vec<String> {
    let (iu, iu_conf, iu_reason) = crate::triage::classify(detail);
    let basic = detail.get("basicUdi").cloned().unwrap_or(Value::Null);
    let udi_di = detail.get("udiDi").cloned().unwrap_or(Value::Null);
    let (emdn_code, emdn_term) = emdn(&udi_di);
    let eudamed = basic
        .get("certificate")
        .map(|c| s(c, "eudamedBasicUdiUri"))
        .unwrap_or_default();
    // Prefer the detail's marketStatus, fall back to the list's.
    let market_status = {
        let d = s(&udi_di, "marketStatus");
        if d.is_empty() {
            r.market_status.clone()
        } else {
            d
        }
    };

    vec![
        r.udi_di_code.clone(),
        r.udi_di_id.clone(),
        r.basic_udi_di_code.clone(),
        r.company_name.clone(),
        r.device_name.clone(),
        r.device_type.clone(),
        r.risk_class.clone(),
        market_status,
        {
            let d = s(&udi_di, "lastModifiedAt");
            if d.is_empty() {
                r.last_modified_at.clone()
            } else {
                d
            }
        },
        emdn_code,
        emdn_term,
        additional_description(&udi_di),
        eudamed,
        b(&basic, "implantable"),
        b(&basic, "reusable"),
        b(&basic, "active"),
        b(&udi_di, "sterile"),
        b(&udi_di, "sterilization"),
        b(&basic, "measuringFunction"),
        b(&basic, "administeringMedicine"),
        b(&udi_di, "latex"),
        b(&basic, "animalTissuesCells"),
        b(&basic, "humanTissuesCells"),
        b(&basic, "selfTesting"),
        b(&basic, "nearPatientTesting"),
        b(&basic, "professionalTesting"),
        b(&basic, "companionDiagnostics"),
        b(&basic, "reagent"),
        b(&basic, "instrument"),
        b(&basic, "kit"),
        b(&basic, "microbialSubstances"),
        serde_json::to_string(detail).unwrap_or_default(),
        iu,
        iu_conf,
        iu_reason,
    ]
}

/// Fetch and parse one detail record, retrying transient failures. Returns the
/// built output row, or `None` if all attempts fail.
///
/// The `client` is per-worker-thread (see `map_init` below): its cookie store
/// keeps this thread's `sm-cookie-be` backend affinity and reuses the connection
/// across the many devices that thread handles.
fn fetch_one(client: &reqwest::blocking::Client, r: &UdiRef) -> Option<Vec<String>> {
    let url = format!(
        "https://swissdamed.ch/public/udi/udi-dis/{}/details",
        r.udi_di_id
    );
    for attempt in 1..=3 {
        let outcome = client
            .get(&url)
            .header("Accept", "application/json, text/plain, */*")
            .send();
        match outcome {
            Ok(resp) if resp.status().is_success() => match resp.json::<Value>() {
                Ok(detail) => return Some(build_row(r, &detail)),
                Err(_) if attempt < 3 => {}
                Err(e) => {
                    eprintln!("[details]   parse error for {}: {}", r.udi_di_id, e);
                    return None;
                }
            },
            Ok(resp) if attempt == 3 => {
                eprintln!("[details]   HTTP {} for {}", resp.status(), r.udi_di_id);
                return None;
            }
            Err(e) if attempt == 3 => {
                eprintln!("[details]   request error for {}: {}", r.udi_di_id, e);
                return None;
            }
            _ => {}
        }
        thread::sleep(Duration::from_millis(300 * attempt as u64));
    }
    None
}

/// Fetch `refs` in parallel via a rayon pool. Each worker gets its own reqwest
/// client (own cookie store / `sm-cookie-be` backend affinity), reused across
/// the devices it processes. Failed fetches are dropped (logged in `fetch_one`).
fn fetch_rows(refs: &[&UdiRef], threads: usize) -> Vec<Vec<String>> {
    use rayon::prelude::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let total = refs.len();
    let done = AtomicUsize::new(0);
    let ok_count = AtomicUsize::new(0);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads.max(1))
        .build()
        .ok();
    let work = || {
        refs.par_iter()
            .map_init(
                || http_client().expect("failed to build worker http client"),
                |worker_client, r| {
                    let row = fetch_one(worker_client, r);
                    if row.is_some() {
                        ok_count.fetch_add(1, Ordering::Relaxed);
                    }
                    let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                    if n % 500 == 0 || n == total {
                        eprintln!(
                            "[details]   {}/{} fetched (ok={})",
                            n,
                            total,
                            ok_count.load(Ordering::Relaxed)
                        );
                    }
                    row
                },
            )
            .flatten()
            .collect::<Vec<Vec<String>>>()
    };
    match &pool {
        Some(p) => p.install(work),
        None => work(),
    }
}

/// Entry point for `--details` (full/limited rebuild — overwrites the DB).
pub fn run(args: &crate::Args) -> Result<(), Box<dyn std::error::Error>> {
    let client = http_client()?;
    let refs = collect_udi_refs(&client, args.page_size, args.details_limit)?;
    let total = refs.len();
    if total == 0 {
        return Err("No udiDi references collected".into());
    }
    let threads = args.details_threads.max(1) as usize;
    eprintln!(
        "[details] Fetching {} detail records with {} threads ...",
        total, threads
    );

    let refptrs: Vec<&UdiRef> = refs.iter().collect();
    let rows = fetch_rows(&refptrs, threads);
    let ok = rows.len();
    let failed = total - ok;

    let headers: Vec<String> = HEADERS.iter().map(|h| h.to_string()).collect();
    let db_path = export::output_db("udi_details")?;
    export::write_sqlite_table(&headers, &rows, &db_path, "udi_details")?;
    eprintln!("[details] SQLite written: {} ({} rows)", db_path, rows.len());

    if args.csv {
        let csv_path = export::output_csv("udi_details")?;
        export::write_csv(&headers, &rows, &csv_path)?;
        eprintln!("[details] CSV written: {}", csv_path);
    }

    eprintln!(
        "[details] Done. {} detail records ({} ok, {} failed) of {} requested.",
        rows.len(),
        ok,
        failed,
        total
    );
    Ok(())
}

/// Find the newest `udi_details_*.db` in the db dir (by mtime).
pub fn find_latest_db(db_dir: &std::path::Path) -> Option<std::path::PathBuf> {
    let mut best: Option<(std::time::SystemTime, std::path::PathBuf)> = None;
    for entry in std::fs::read_dir(db_dir).ok()?.flatten() {
        let p = entry.path();
        let name = p.file_name()?.to_string_lossy().to_string();
        if name.starts_with("udi_details_") && name.ends_with(".db") {
            if let Some(mt) = entry.metadata().ok().and_then(|m| m.modified().ok()) {
                if best.as_ref().map(|(t, _)| mt > *t).unwrap_or(true) {
                    best = Some((mt, p));
                }
            }
        }
    }
    best.map(|(_, p)| p)
}

/// Ensure the `udi_details` table exists and has every column in `HEADERS`
/// (migrates older DBs via `ALTER TABLE ADD COLUMN`).
fn ensure_schema(conn: &rusqlite::Connection) -> Result<(), Box<dyn std::error::Error>> {
    let quote = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    let table_exists: bool = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='udi_details'",
            [],
            |_| Ok(true),
        )
        .unwrap_or(false);
    if !table_exists {
        let cols: Vec<String> = HEADERS.iter().map(|h| format!("{} TEXT", quote(h))).collect();
        conn.execute(
            &format!("CREATE TABLE udi_details ({})", cols.join(", ")),
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_udiDiCode ON udi_details(\"udiDiCode\")",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_udiDiId ON udi_details(\"udiDiId\")",
            [],
        )?;
        return Ok(());
    }
    let existing: std::collections::HashSet<String> = conn
        .prepare("PRAGMA table_info(udi_details)")?
        .query_map([], |r| r.get::<_, String>(1))?
        .filter_map(|r| r.ok())
        .collect();
    for h in HEADERS {
        if !existing.contains(*h) {
            conn.execute(
                &format!("ALTER TABLE udi_details ADD COLUMN {} TEXT", quote(h)),
                [],
            )?;
        }
    }
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_udiDiId ON udi_details(\"udiDiId\")",
        [],
    )?;
    Ok(())
}

/// Incrementally update the `udi_details` DB from an already-downloaded basic-UDI
/// list. Fetches details only for NEW or CHANGED udiDis (change key:
/// `lastModifiedAt`), replaces changed rows, and removes delisted ones. Carries
/// the latest existing DB forward into today's date-stamped file. Reuses the
/// caller's UDI download (no extra list request).
pub fn update_details(
    values: &[Value],
    threads: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::collections::{HashMap, HashSet};

    let db_dir = crate::app_data_dir().join("db");
    std::fs::create_dir_all(&db_dir)?;
    let today_path = export::output_db("udi_details")?;

    // Carry the latest existing DB forward into today's file (unless it IS today's).
    if let Some(src) = find_latest_db(&db_dir) {
        if src.to_string_lossy() != today_path {
            std::fs::copy(&src, &today_path)?;
            eprintln!(
                "[details] Carried {} -> {} for incremental update",
                src.display(),
                today_path
            );
        }
    }

    let refs = refs_from_values(values);
    eprintln!("[details] {} udiDi refs in current list", refs.len());

    let mut conn = rusqlite::Connection::open(&today_path)?;
    ensure_schema(&conn)?;

    // One-time backfill: fill lastModifiedAt for legacy rows from rawJson so
    // change-detection works for the whole corpus, not just newly-added rows.
    let backfilled = conn.execute(
        "UPDATE udi_details SET lastModifiedAt = json_extract(rawJson, '$.udiDi.lastModifiedAt') \
         WHERE (lastModifiedAt IS NULL OR lastModifiedAt = '') AND rawJson IS NOT NULL",
        [],
    )?;
    if backfilled > 0 {
        eprintln!("[details] backfilled lastModifiedAt for {} legacy rows", backfilled);
    }

    // Existing state: udiDiId -> lastModifiedAt.
    let existing: HashMap<String, String> = {
        let mut stmt = conn.prepare("SELECT udiDiId, IFNULL(lastModifiedAt,'') FROM udi_details")?;
        let it = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
        it.filter_map(|r| r.ok()).collect()
    };
    let current_ids: HashSet<&str> = refs.iter().map(|r| r.udi_di_id.as_str()).collect();

    // New = unknown id; Changed = known id whose stored lastModifiedAt differs
    // (only when we actually have a stored timestamp).
    let to_fetch: Vec<&UdiRef> = refs
        .iter()
        .filter(|r| match existing.get(&r.udi_di_id) {
            None => true,
            Some(lmt) => !lmt.is_empty() && *lmt != r.last_modified_at,
        })
        .collect();
    let delisted: Vec<String> = existing
        .keys()
        .filter(|id| !current_ids.contains(id.as_str()))
        .cloned()
        .collect();

    eprintln!(
        "[details] incremental: {} to fetch (new/changed), {} delisted, {} unchanged",
        to_fetch.len(),
        delisted.len(),
        existing.len() - (to_fetch.len().min(existing.len()))
    );

    // Fetch the delta.
    let rows = if to_fetch.is_empty() {
        Vec::new()
    } else {
        fetch_rows(&to_fetch, threads.max(1) as usize)
    };
    // udiDiId is column index 1 in HEADERS order.
    let fetched_ids: HashSet<String> = rows.iter().map(|r| r[1].clone()).collect();

    // Apply: delete rows we can replace (successfully fetched) + delisted, then insert.
    let quote = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    let cols_sql = HEADERS.iter().map(|h| quote(h)).collect::<Vec<_>>().join(", ");
    let ph = vec!["?"; HEADERS.len()].join(", ");
    let insert_sql = format!("INSERT INTO udi_details ({}) VALUES ({})", cols_sql, ph);

    let tx = conn.transaction()?;
    {
        let mut del = tx.prepare("DELETE FROM udi_details WHERE udiDiId = ?")?;
        for id in fetched_ids.iter().chain(delisted.iter()) {
            del.execute([id])?;
        }
        let mut ins = tx.prepare(&insert_sql)?;
        for row in &rows {
            let params: Vec<&dyn rusqlite::types::ToSql> =
                row.iter().map(|s| s as &dyn rusqlite::types::ToSql).collect();
            ins.execute(params.as_slice())?;
        }
    }
    tx.commit()?;

    let final_count: i64 =
        conn.query_row("SELECT COUNT(*) FROM udi_details", [], |r| r.get(0))?;
    eprintln!(
        "[details] update done: +{} inserted, -{} delisted → {} rows in {}",
        rows.len(),
        delisted.len(),
        final_count,
        today_path
    );
    Ok(())
}

/// Standalone `--details-update`: download the list and run an incremental update.
pub fn run_update(args: &crate::Args) -> Result<(), Box<dyn std::error::Error>> {
    let values = if let Some(ref path) = args.file {
        crate::download::load_json_file(path)?
    } else {
        crate::download::download_all_pages(args.page_size)?
    };
    update_details(&values, args.details_threads)
}
