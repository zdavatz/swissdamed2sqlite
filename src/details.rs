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

/// Entry point for `--details`.
pub fn run(args: &crate::Args) -> Result<(), Box<dyn std::error::Error>> {
    use rayon::prelude::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    // Parallel fetch: each rayon worker gets its own reqwest client (own cookie
    // store / backend affinity), reused across the devices it processes.
    let done = AtomicUsize::new(0);
    let ok_count = AtomicUsize::new(0);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
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
    let rows: Vec<Vec<String>> = match &pool {
        Some(p) => p.install(work),
        None => work(),
    };
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
