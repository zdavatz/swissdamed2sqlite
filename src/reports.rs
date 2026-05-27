use rayon::prelude::*;
use serde_json::Value;
use std::collections::{BTreeSet, HashMap, HashSet};

use crate::data::*;
use crate::download::*;
use crate::error_report::{is_valid_srn, write_srn_error_report, InvalidSrn};
use crate::export::*;
use crate::gdrive::{gdrive_upload_csv, send_email_with_attachment};
use crate::migel::{build_search_index, find_best_migel_match, parse_migel_items, MigelItem};
use crate::Args;

// --- Shared helpers ---

/// Group actors by companyUid and return UIDs that have only AR/IM roles.
/// If `require_ar` is true, the UID must have at least one AR role.
fn find_ch_rep_uids(actor_values: &[Value], require_ar: bool) -> HashSet<String> {
    let mut uid_roles: HashMap<String, HashSet<String>> = HashMap::new();
    for v in actor_values {
        let uid = v
            .get("companyUid")
            .and_then(|u| u.as_str())
            .unwrap_or("");
        let role = v
            .get("actorType")
            .and_then(|t| t.as_str())
            .unwrap_or("");
        if !uid.is_empty() && !role.is_empty() {
            uid_roles
                .entry(uid.to_string())
                .or_default()
                .insert(role.to_string());
        }
    }

    uid_roles
        .into_iter()
        .filter(|(_, roles)| {
            let only_ar_im = !roles.is_empty() && roles.iter().all(|r| r == "AR" || r == "IM");
            if require_ar {
                only_ar_im && roles.contains("AR")
            } else {
                only_ar_im
            }
        })
        .map(|(uid, _)| uid)
        .collect()
}

/// Common output pattern: write CSV (optionally upload to GDrive / email) and SQLite.
fn output_results(
    headers: &[String],
    rows: &[Vec<String>],
    name: &str,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error>> {
    let (do_csv, do_sqlite) = if !args.csv && !args.sqlite {
        (true, true)
    } else {
        (args.csv, args.sqlite)
    };

    if do_csv {
        let filename = output_csv(name)?;
        write_csv(headers, rows, &filename)?;
        eprintln!("CSV written: {}", filename);
        if args.gdrive {
            gdrive_upload_csv(args, &filename)?;
        }
        if let Some(ref to) = args.mailto {
            send_email_with_attachment(args, &filename, to)?;
        }
    }

    if do_sqlite {
        let filename = output_db(name)?;
        write_sqlite_table(headers, rows, &filename, name)?;
        eprintln!("SQLite written: {}", filename);
    }

    Ok(())
}

// --- MiGel matching ---

pub fn run_migel(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
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
        return Err(
            format!("Failed to download MiGel XLSX: HTTP {}", response.status()).into(),
        );
    }
    let bytes = response.bytes()?;
    std::fs::write(migel_file, &bytes)?;
    eprintln!("MiGel XLSX saved ({} bytes)", bytes.len());

    // 3. Parse MiGel items and build keyword index
    eprintln!("Parsing MiGel items...");
    let migel_items = parse_migel_items(migel_file)?;
    eprintln!("Found {} MiGel items with position numbers", migel_items.len());

    let search_index = build_search_index(&migel_items)?;
    eprintln!("Built Aho-Corasick search index");

    // 4. Find column indices for matching
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

    let excluded_companies: &[&str] = &[
        "Varian Medical Systems Inc",
        "Varian Medical Systems Inc.",
        "Sunstar Europe SA",
    ];
    let idx_company = headers.iter().position(|h| h == "companyName");
    let idx_gtin = headers.iter().position(|h| h == "udiDiCode");

    // Optional GTIN→MiGeL override map from the latest sigvaris_shop_*.db.
    // Lookup keys are both gtin14 (matches swissdamed) and gtin13. A value of
    // `None` means SIGVARIS classifies the GTIN as non-MiGeL (Stützstrumpf,
    // Anti-Thrombose, Reisestrumpf, Klasse 1) and we must skip the row.
    let db_dir = crate::app_data_dir().join("db");
    let overrides: crate::sigvaris_shop::Overrides =
        match crate::sigvaris_shop::find_latest_db(&db_dir) {
            Some(p) => match crate::sigvaris_shop::load_overrides(&p) {
                Ok(m) => {
                    eprintln!(
                        "Loaded {} GTIN overrides from {}",
                        m.len(),
                        p.display()
                    );
                    m
                }
                Err(e) => {
                    eprintln!("Warning: failed to load sigvaris_shop overrides: {}", e);
                    HashMap::new()
                }
            },
            None => HashMap::new(),
        };
    // Index MiGel items by position_nr for O(1) override lookup
    let migel_by_pos: HashMap<&str, &MigelItem> =
        migel_items.iter().map(|m| (m.position_nr.as_str(), m)).collect();

    let override_hits = std::sync::atomic::AtomicUsize::new(0);
    let override_skips = std::sync::atomic::AtomicUsize::new(0);

    let matched_rows: Vec<Vec<String>> = rows
        .par_iter()
        .filter_map(|row| {
            if let Some(ci) = idx_company {
                if let Some(company) = row.get(ci) {
                    if excluded_companies.contains(&company.as_str()) {
                        return None;
                    }
                }
            }

            // 1. Override lookup by GTIN — takes precedence over heuristic matcher
            if let Some(gi) = idx_gtin {
                if let Some(gtin) = row.get(gi) {
                    if let Some(decision) = overrides.get(gtin) {
                        match decision {
                            None => {
                                // Explicit skip (e.g. SIGVARIS Stützstrumpf / Anti-Thrombose)
                                override_skips
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                return None;
                            }
                            Some(code) => {
                                if let Some(item) = migel_by_pos.get(code.as_str()) {
                                    override_hits
                                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    let mut matched_row = row.clone();
                                    matched_row.push(item.position_nr.clone());
                                    matched_row.push(item.bezeichnung.clone());
                                    matched_row.push(item.limitation.clone());
                                    return Some(matched_row);
                                }
                                // Override code not in MiGeL XLSX (stale?) — fall through to heuristic
                            }
                        }
                    }
                }
            }

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
                        desc_de = format!("{} {}", desc_de, val);
                        desc_fr = format!("{} {}", desc_fr, val);
                        desc_it = format!("{} {}", desc_it, val);
                    }
                }
            }

            let device = idx_device
                .and_then(|i| row.get(i))
                .cloned()
                .unwrap_or_default();
            let model = idx_model
                .and_then(|i| row.get(i))
                .cloned()
                .unwrap_or_default();
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

            let brand = idx_brand
                .and_then(|i| row.get(i))
                .cloned()
                .unwrap_or_default();

            find_best_migel_match(
                &desc_de,
                &desc_fr,
                &desc_it,
                &brand,
                &migel_items,
                &search_index,
            )
            .map(|migel| {
                let mut matched_row = row.clone();
                matched_row.push(migel.position_nr.clone());
                matched_row.push(migel.bezeichnung.clone());
                matched_row.push(migel.limitation.clone());
                matched_row
            })
        })
        .collect();

    let oh = override_hits.load(std::sync::atomic::Ordering::Relaxed);
    let os = override_skips.load(std::sync::atomic::Ordering::Relaxed);
    if !overrides.is_empty() {
        eprintln!(
            "GTIN overrides applied: {} matched ({} explicit-skip)",
            oh, os
        );
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
    let db_filename = output_db("swissdamed_migel")?;
    write_sqlite(&migel_headers, &matched_rows, &db_filename)?;
    eprintln!("SQLite written: {}", db_filename);

    // Stash the total UDI row count + override stats in the migel DB so the
    // stats renderer can compute coverage even when no full UDI DB is on disk.
    {
        let conn = rusqlite::Connection::open(&db_filename)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)",
            [],
        )?;
        let total_str = rows.len().to_string();
        let oh_str = oh.to_string();
        let os_str = os.to_string();
        for (k, v) in [
            ("total_products", total_str.as_str()),
            ("override_matched", oh_str.as_str()),
            ("override_skipped", os_str.as_str()),
        ] {
            conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES (?1, ?2)",
                rusqlite::params![k, v],
            )?;
        }
    }

    // 7. Generate stats PNG (Rust, via plotters)
    let db_dir = crate::app_data_dir().join("db");
    let (_, full_db) = crate::migel_stats::find_latest_dbs(&db_dir);
    if let Err(e) = crate::migel_stats::generate(
        std::path::Path::new(&db_filename),
        full_db.as_deref(),
    ) {
        eprintln!("Could not generate stats PNG: {}", e);
    }

    Ok(())
}

// --- CH-REP only (companies with only AR/IM roles, no MF/PR) ---

pub fn run_ch_rep(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let actor_values = download_all_pages_from(
        "https://swissdamed.ch/public/act/actors",
        "actors",
        50,
    )?;

    let ch_rep_uids = find_ch_rep_uids(&actor_values, false);

    eprintln!(
        "Found {} CH-REP only companies (AR/IM only, no MF/PR) out of all actors.",
        ch_rep_uids.len()
    );

    let filtered: Vec<&Value> = actor_values
        .iter()
        .filter(|v| {
            v.get("companyUid")
                .and_then(|u| u.as_str())
                .map(|uid| ch_rep_uids.contains(uid))
                .unwrap_or(false)
        })
        .collect();

    let filtered_owned: Vec<Value> = filtered.into_iter().cloned().collect();
    let headers = collect_flat_headers(&filtered_owned);
    let rows = build_flat_rows(&filtered_owned, &headers);

    eprintln!(
        "CH-REP output: {} rows with {} columns.",
        rows.len(),
        headers.len()
    );

    output_results(&headers, &rows, "ch_rep", args)
}

// --- CH-REP mandate count ranking ---

pub fn run_ch_rep_mandates(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    // 1. Download actors
    let actor_values = download_all_pages_from(
        "https://swissdamed.ch/public/act/actors",
        "actors",
        50,
    )?;

    // 2. Identify CH-REP UIDs
    let ch_rep_uids = find_ch_rep_uids(&actor_values, args.ar_only);

    let mode_label = if args.ar_only { "AR-only" } else { "AR/IM" };
    eprintln!(
        "Found {} CH-REP companies ({}).",
        ch_rep_uids.len(),
        mode_label
    );

    // 3. Build actor_id -> companyUid lookup (for CH-REP actors only)
    let mut actor_id_to_uid: HashMap<String, String> = HashMap::new();
    let mut uid_to_info: HashMap<String, (String, String, String)> = HashMap::new();
    for v in &actor_values {
        let uid = v
            .get("companyUid")
            .and_then(|u| u.as_str())
            .unwrap_or("");
        if !ch_rep_uids.contains(uid) {
            continue;
        }
        let role = v
            .get("actorType")
            .and_then(|t| t.as_str())
            .unwrap_or("");
        if args.ar_only && role != "AR" {
            continue;
        }
        if let Some(actor_id) = v.get("id").and_then(|id| id.as_str()) {
            actor_id_to_uid.insert(actor_id.to_string(), uid.to_string());
        }
        uid_to_info.entry(uid.to_string()).or_insert_with(|| {
            let name = v
                .get("companyName")
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_string();
            let city = v
                .get("city")
                .and_then(|c| c.as_str())
                .unwrap_or("")
                .to_string();
            let country = v
                .get("country")
                .and_then(|c| c.as_str())
                .unwrap_or("")
                .to_string();
            (name, city, country)
        });
    }

    // 4. Download mandates and count per CH-REP company
    let mandate_values = download_all_pages_from(
        "https://swissdamed.ch/public/act/mandates",
        "mandates",
        50,
    )?;

    let mut uid_mandate_count: HashMap<String, u32> = HashMap::new();
    for m in &mandate_values {
        let actor_id = m
            .get("actorId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if let Some(uid) = actor_id_to_uid.get(actor_id) {
            *uid_mandate_count.entry(uid.clone()).or_insert(0) += 1;
        }
    }

    // 5. Build rows sorted by mandate count descending
    let mut ranked: Vec<(String, String, String, String, u32)> = uid_to_info
        .iter()
        .map(|(uid, (name, city, country))| {
            let count = uid_mandate_count.get(uid).copied().unwrap_or(0);
            (name.clone(), uid.clone(), city.clone(), country.clone(), count)
        })
        .collect();
    ranked.sort_by(|a, b| b.4.cmp(&a.4).then(a.0.cmp(&b.0)));

    let headers = vec![
        "rank".to_string(),
        "companyName".to_string(),
        "companyUid".to_string(),
        "city".to_string(),
        "country".to_string(),
        "mandate_count".to_string(),
    ];

    let rows: Vec<Vec<String>> = ranked
        .iter()
        .enumerate()
        .map(|(i, (name, uid, city, country, count))| {
            vec![
                (i + 1).to_string(),
                name.clone(),
                uid.clone(),
                city.clone(),
                country.clone(),
                count.to_string(),
            ]
        })
        .collect();

    eprintln!(
        "CH-REP mandate ranking: {} companies, {} total mandates.",
        rows.len(),
        ranked.iter().map(|r| r.4).sum::<u32>()
    );

    // Print top 20 to stderr
    eprintln!("\nTop 20 CH-REP by mandate count:");
    eprintln!("{:<4} {:<50} {:>6}", "Rank", "Company", "Mandates");
    eprintln!("{}", "-".repeat(62));
    for (i, (name, _, _, _, count)) in ranked.iter().take(20).enumerate() {
        eprintln!("{:<4} {:<50} {:>6}", i + 1, name, count);
    }

    let name = if args.ar_only {
        "ch_rep_mandates_ar_only"
    } else {
        "ch_rep_mandates"
    };
    output_results(&headers, &rows, name, args)
}

// --- Company ranking by product count ---

pub fn run_company_ranking(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
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

    let company_idx = headers.iter().position(|h| h == "companyName");
    let code_idx = headers.iter().position(|h| h == "udiDiCode");

    if company_idx.is_none() || code_idx.is_none() {
        return Err("Missing companyName or udiDiCode column".into());
    }
    let company_idx = company_idx.unwrap();
    let code_idx = code_idx.unwrap();

    let mut company_codes: HashMap<String, HashSet<String>> = HashMap::new();
    for row in &rows {
        let company = row.get(company_idx).map(|s| s.as_str()).unwrap_or("");
        let code = row.get(code_idx).map(|s| s.as_str()).unwrap_or("");
        if !company.is_empty() && !code.is_empty() {
            company_codes
                .entry(company.to_string())
                .or_default()
                .insert(code.to_string());
        }
    }

    let mut ranked: Vec<(String, usize)> = company_codes
        .into_iter()
        .map(|(name, codes)| (name, codes.len()))
        .collect();
    ranked.sort_by(|a, b| b.1.cmp(&a.1));

    let out_headers = vec![
        "rank".to_string(),
        "companyName".to_string(),
        "produkte".to_string(),
    ];
    let out_rows: Vec<Vec<String>> = ranked
        .iter()
        .enumerate()
        .map(|(i, (name, count))| vec![(i + 1).to_string(), name.clone(), count.to_string()])
        .collect();

    let total: usize = ranked.iter().map(|(_, c)| c).sum();
    eprintln!(
        "Company ranking: {} companies, {} total products.",
        ranked.len(),
        total
    );

    eprintln!("\nTop 20 companies by product count:");
    eprintln!("{:<6} {:<55} {:>8}", "Rank", "Company", "Products");
    eprintln!("{}", "-".repeat(71));
    for (i, (name, count)) in ranked.iter().take(20).enumerate() {
        eprintln!("{:<6} {:<55} {:>8}", i + 1, name, count);
    }

    let filename = output_csv("company_ranking")?;
    write_csv(&out_headers, &out_rows, &filename)?;
    eprintln!("CSV written: {}", filename);

    if args.gdrive {
        gdrive_upload_csv(args, &filename)?;
    }
    if let Some(ref to) = args.mailto {
        send_email_with_attachment(args, &filename, to)?;
    }

    Ok(())
}

// --- Unique SRNs export ---

pub fn run_unique_srns(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let actors =
        download_all_pages_from("https://swissdamed.ch/public/act/actors", "actors", 50)?;
    let actor_headers = collect_flat_headers(&actors);
    let actor_rows = build_flat_rows(&actors, &actor_headers);

    let id_idx = actor_headers.iter().position(|h| h == "id");
    let chrn_idx = actor_headers.iter().position(|h| h == "chrn");
    let name_idx = actor_headers.iter().position(|h| h == "companyName");
    let uid_idx = actor_headers.iter().position(|h| h == "companyUid");
    let type_idx = actor_headers.iter().position(|h| h == "actorType");

    let mut actor_map: HashMap<String, (String, String, String)> = HashMap::new();
    for row in &actor_rows {
        let actor_type = type_idx
            .and_then(|i| row.get(i))
            .map(|s| s.as_str())
            .unwrap_or("");
        if actor_type != "AR" {
            continue;
        }
        let id = id_idx
            .and_then(|i| row.get(i))
            .cloned()
            .unwrap_or_default();
        let chrn = chrn_idx
            .and_then(|i| row.get(i))
            .cloned()
            .unwrap_or_default();
        let name = name_idx
            .and_then(|i| row.get(i))
            .cloned()
            .unwrap_or_default();
        let uid = uid_idx
            .and_then(|i| row.get(i))
            .cloned()
            .unwrap_or_default();
        if !id.is_empty() {
            actor_map.insert(id, (chrn, name, uid));
        }
    }

    let mandates =
        download_all_pages_from("https://swissdamed.ch/public/act/mandates", "mandates", 50)?;
    let mandate_headers = collect_flat_headers(&mandates);
    let mandate_rows = build_flat_rows(&mandates, &mandate_headers);

    let m_id_idx = mandate_headers.iter().position(|h| h == "id");
    let m_actor_idx = mandate_headers.iter().position(|h| h == "actorId");

    let ar_mandate_ids: Vec<(String, String)> = mandate_rows
        .iter()
        .filter_map(|row| {
            let mid = m_id_idx.and_then(|i| row.get(i))?.clone();
            let aid = m_actor_idx.and_then(|i| row.get(i))?.clone();
            if actor_map.contains_key(&aid) {
                Some((mid, aid))
            } else {
                None
            }
        })
        .collect();

    eprintln!(
        "Fetching details for {} AR mandates...",
        ar_mandate_ids.len()
    );

    let client = http_client()?;

    let mut srn_map: HashMap<String, (String, String, String, String, String, String)> =
        HashMap::new();
    let mut invalid_srns: Vec<InvalidSrn> = Vec::new();

    for (i, (mid, aid)) in ar_mandate_ids.iter().enumerate() {
        if (i + 1) % 50 == 0 || i + 1 == ar_mandate_ids.len() {
            eprintln!(
                "[mandate-details] Fetching {}/{} ...",
                i + 1,
                ar_mandate_ids.len()
            );
        }
        let url = format!("https://swissdamed.ch/public/act/mandates/{}", mid);
        let resp = client
            .get(&url)
            .header("Accept", "application/json")
            .send();
        if let Ok(resp) = resp {
            if let Ok(detail) = resp.json::<Value>() {
                let srn = detail
                    .get("srn")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .trim()
                    .to_string();
                if srn.is_empty() {
                    continue;
                }
                let mfr_name = detail
                    .get("companyName")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let mtype = detail
                    .get("mandateType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let country = detail
                    .get("address")
                    .and_then(|a| a.get("country"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let (actor_chrn, actor_name, actor_uid) =
                    actor_map.get(aid).cloned().unwrap_or_default();
                if !is_valid_srn(&srn) {
                    invalid_srns.push(InvalidSrn {
                        srn,
                        manufacturer: mfr_name,
                        mandate_type: mtype,
                        mandate_holder_chrn: actor_chrn,
                        mandate_holder_name: actor_name,
                        mandate_holder_uid: actor_uid,
                    });
                    continue;
                }
                srn_map.entry(srn).or_insert((
                    mfr_name,
                    mtype,
                    country,
                    actor_chrn,
                    actor_name,
                    actor_uid,
                ));
            }
        }
    }

    let mut sorted: Vec<_> = srn_map.into_iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));

    eprintln!(
        "Unique SRNs: {} ({} invalid filtered)",
        sorted.len(),
        invalid_srns.len()
    );

    write_srn_error_report(&invalid_srns)?;

    let out_headers = vec![
        "srn".to_string(),
        "manufacturer".to_string(),
        "mandateType".to_string(),
        "manufacturer_country".to_string(),
        "mandate_holder_chrn".to_string(),
        "mandate_holder_name".to_string(),
        "mandate_holder_uid".to_string(),
    ];
    let out_rows: Vec<Vec<String>> = sorted
        .iter()
        .map(|(srn, (mfr, mtype, country, chrn, name, uid))| {
            vec![
                srn.clone(),
                mfr.clone(),
                mtype.clone(),
                country.clone(),
                chrn.clone(),
                name.clone(),
                uid.clone(),
            ]
        })
        .collect();

    let filename = output_csv("unique_srns")?;
    write_csv(&out_headers, &out_rows, &filename)?;
    eprintln!("CSV written: {}", filename);

    if args.gdrive {
        gdrive_upload_csv(args, &filename)?;
    }
    if let Some(ref to) = args.mailto {
        send_email_with_attachment(args, &filename, to)?;
    }

    Ok(())
}

// --- Lookup CHRN → SRNs ---

pub fn run_lookup_chrn(chrn: &str, args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let actor_values = download_all_pages_from(
        "https://swissdamed.ch/public/act/actors",
        "actors",
        50,
    )?;

    let matching_actors: Vec<&Value> = actor_values
        .iter()
        .filter(|v| v.get("chrn").and_then(|n| n.as_str()) == Some(chrn))
        .collect();

    if matching_actors.is_empty() {
        eprintln!("No actor found with actorNumber = {}", chrn);
        return Ok(());
    }

    eprintln!(
        "Found {} actor record(s) for {}.",
        matching_actors.len(),
        chrn
    );

    let actor_ids: HashSet<String> = matching_actors
        .iter()
        .filter_map(|v| {
            v.get("id")
                .and_then(|id| id.as_str())
                .map(|s| s.to_string())
        })
        .collect();

    let mandate_values = download_all_pages_from(
        "https://swissdamed.ch/public/act/mandates",
        "mandates",
        50,
    )?;

    let matching_mandate_ids: Vec<(String, String)> = mandate_values
        .iter()
        .filter_map(|m| {
            let actor_id = m.get("actorId").and_then(|v| v.as_str())?;
            if !actor_ids.contains(actor_id) {
                return None;
            }
            let mandate_id = m.get("id").and_then(|v| v.as_str())?;
            Some((actor_id.to_string(), mandate_id.to_string()))
        })
        .collect();

    if matching_mandate_ids.is_empty() {
        eprintln!("No mandates found for {}.", chrn);
        return Ok(());
    }

    eprintln!(
        "Fetching details for {} mandates of {} ...",
        matching_mandate_ids.len(),
        chrn
    );

    let client = http_client()?;

    let ids_only: Vec<String> = matching_mandate_ids
        .iter()
        .map(|(_, mid)| mid.clone())
        .collect();
    let details = fetch_mandate_details(&client, &ids_only)?;

    let actor_headers = collect_flat_headers(&actor_values);

    let mut detail_key_set = BTreeSet::new();
    let mut detail_key_order: Vec<String> = Vec::new();
    for detail in &details {
        for (key, _) in flatten_mandate_detail(detail) {
            if detail_key_set.insert(key.clone()) {
                detail_key_order.push(key);
            }
        }
    }

    let mut joined_headers: Vec<String> = actor_headers
        .iter()
        .map(|h| format!("actor_{}", h))
        .collect();
    for key in &detail_key_order {
        if key == "actorId" || key.starts_with("actorInfo_") {
            continue;
        }
        joined_headers.push(format!("mandate_{}", key));
    }

    // Build joined rows (skip entries where detail fetch returned null)
    let actor_map: HashMap<String, &Value> = matching_actors
        .iter()
        .filter_map(|v| {
            v.get("id")
                .and_then(|id| id.as_str())
                .map(|id| (id.to_string(), *v))
        })
        .collect();

    let mut rows: Vec<Vec<String>> = Vec::new();
    for (i, (actor_id, _)) in matching_mandate_ids.iter().enumerate() {
        let actor = match actor_map.get(actor_id) {
            Some(a) => a,
            None => continue,
        };
        let detail = match details.get(i) {
            Some(d) if !d.is_null() => d,
            _ => continue,
        };
        let detail_fields: HashMap<String, String> =
            flatten_mandate_detail(detail).into_iter().collect();

        let mut row: Vec<String> = actor_headers
            .iter()
            .map(|key| get_field(actor, key))
            .collect();
        for key in &detail_key_order {
            if key == "actorId" || key.starts_with("actorInfo_") {
                continue;
            }
            row.push(detail_fields.get(key).cloned().unwrap_or_default());
        }
        rows.push(row);
    }

    eprintln!(
        "Joined {} mandate rows ({} columns).",
        rows.len(),
        joined_headers.len()
    );

    // Write CSV with timestamp
    let timestamp = chrono::Local::now().format("%Hh%M.%d.%m.%Y").to_string();
    let csv_dir = crate::app_data_dir().join("csv");
    std::fs::create_dir_all(&csv_dir)?;
    let csv_path = csv_dir
        .join(format!("{}_{}.csv", chrn, timestamp))
        .to_string_lossy()
        .to_string();

    write_csv(&joined_headers, &rows, &csv_path)?;
    eprintln!("CSV written: {}", csv_path);

    if args.gdrive {
        gdrive_upload_csv(args, &csv_path)?;
    }
    if let Some(ref to) = args.mailto {
        send_email_with_attachment(args, &csv_path, to)?;
    }

    Ok(())
}

// --- AR mandates (join AR actors with their mandates + detail) ---

pub fn run_ar_mandates(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    // 1. Download actors
    let actor_values = download_all_pages_from(
        "https://swissdamed.ch/public/act/actors",
        "actors",
        50,
    )?;

    // 2. Download mandates
    let mandate_values = download_all_pages_from(
        "https://swissdamed.ch/public/act/mandates",
        "mandates",
        50,
    )?;

    // 3. Filter AR actors and build a lookup by id
    let ar_actors: Vec<&Value> = actor_values
        .iter()
        .filter(|v| v.get("actorType").and_then(|t| t.as_str()) == Some("AR"))
        .collect();

    eprintln!(
        "Found {} AR actors out of {} total actors.",
        ar_actors.len(),
        actor_values.len()
    );

    let actor_map: HashMap<String, &Value> = ar_actors
        .iter()
        .filter_map(|v| {
            v.get("id")
                .and_then(|id| id.as_str())
                .map(|id| (id.to_string(), *v))
        })
        .collect();

    // 4. Collect AR mandate IDs for detail fetching
    let ar_mandate_ids: Vec<(String, String)> = mandate_values
        .iter()
        .filter_map(|m| {
            let actor_id = m.get("actorId").and_then(|v| v.as_str())?;
            if !actor_map.contains_key(actor_id) {
                return None;
            }
            let mandate_id = m.get("id").and_then(|v| v.as_str())?;
            Some((actor_id.to_string(), mandate_id.to_string()))
        })
        .collect();

    eprintln!(
        "Fetching details for {} AR mandates...",
        ar_mandate_ids.len()
    );

    // 5. Fetch mandate details
    let client = http_client()?;

    let ids_only: Vec<String> = ar_mandate_ids.iter().map(|(_, mid)| mid.clone()).collect();
    let details = fetch_mandate_details(&client, &ids_only)?;

    // 6. Build headers from actor fields + flattened detail fields
    let actor_headers = collect_flat_headers(&actor_values);

    let mut detail_key_set = BTreeSet::new();
    let mut detail_key_order: Vec<String> = Vec::new();
    for detail in &details {
        for (key, _) in flatten_mandate_detail(detail) {
            if detail_key_set.insert(key.clone()) {
                detail_key_order.push(key);
            }
        }
    }

    let mut joined_headers: Vec<String> = actor_headers
        .iter()
        .map(|h| format!("actor_{}", h))
        .collect();
    for key in &detail_key_order {
        if key == "actorId" || key.starts_with("actorInfo_") {
            continue;
        }
        joined_headers.push(format!("mandate_{}", key));
    }

    // 7. Build joined rows (skip entries where detail fetch returned null)
    let mut rows: Vec<Vec<String>> = Vec::new();
    for (i, (actor_id, _)) in ar_mandate_ids.iter().enumerate() {
        let actor = match actor_map.get(actor_id) {
            Some(a) => a,
            None => continue,
        };
        let detail = match details.get(i) {
            Some(d) if !d.is_null() => d,
            _ => continue,
        };
        let detail_fields: HashMap<String, String> =
            flatten_mandate_detail(detail).into_iter().collect();

        let mut row: Vec<String> = actor_headers
            .iter()
            .map(|key| get_field(actor, key))
            .collect();
        for key in &detail_key_order {
            if key == "actorId" || key.starts_with("actorInfo_") {
                continue;
            }
            row.push(detail_fields.get(key).cloned().unwrap_or_default());
        }
        rows.push(row);
    }

    eprintln!(
        "Joined {} mandate rows for AR actors ({} columns).",
        rows.len(),
        joined_headers.len()
    );

    output_results(&joined_headers, &rows, "ar_mandates", args)
}

// --- Generic download and export (actors, mandates) ---

pub fn download_and_export(
    base_url: &str,
    name: &str,
    page_size: u32,
    do_csv: bool,
    do_sqlite: bool,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error>> {
    let values = download_all_pages_from(base_url, name, page_size)?;

    if values.is_empty() {
        eprintln!("[{}] No data found.", name);
        return Ok(());
    }

    let headers = collect_flat_headers(&values);
    let rows = build_flat_rows(&values, &headers);

    eprintln!(
        "[{}] Processed {} items, {} rows with {} columns.",
        name,
        values.len(),
        rows.len(),
        headers.len()
    );

    if do_csv {
        let filename = output_csv(name)?;
        write_csv(&headers, &rows, &filename)?;
        eprintln!("[{}] CSV written: {}", name, filename);
        if args.gdrive {
            gdrive_upload_csv(args, &filename)?;
        }
        if let Some(ref to) = args.mailto {
            send_email_with_attachment(args, &filename, to)?;
        }
    }

    if do_sqlite {
        let filename = output_db(name)?;
        write_sqlite_table(&headers, &rows, &filename, name)?;
        eprintln!("[{}] SQLite written: {}", name, filename);
    }

    Ok(())
}
