//! `--gtin-report` — join a partner's GTIN list against every registry we hold
//! and write the result as a single spreadsheet.
//!
//! A distributor sends a list of GTINs and asks: which of these do you know, and
//! what can you tell us about them? That question was first answered with a
//! throwaway script; it lives here so the next round is one command.
//!
//! Sources, in the order a value is taken from them (first non-empty wins):
//!   * **swissdamed** `udi_details_*.db` — CH registration, EMDN purpose, the
//!     derived intended-user triage, plus depth from the stored `rawJson`
//!   * **MiGeL** `swissdamed_migel.db` — reimbursement position, if matched
//!   * **EUDAMED** `devices_listing` + `device_details_flat` — EU registration,
//!     CND nomenclature, manufacturer/AR with SRN, MDR characteristics
//!   * **Firstbase** (GS1 CH) and **Trustbox** (GS1 GDSN) — brand, GPC, weights
//!
//! swissdamed is preferred over EUDAMED for shared fields because it is the
//! Swiss-market record; the others fill gaps the registries do not cover.
//!
//! The output deliberately leads with a **hits-only sheet**. Only a small share
//! of a retail catalogue is a registered medical device, so the full list opens
//! on mostly-empty rows and reads as "nothing was done" — the separate sheet and
//! the colour-coded headers exist to prevent exactly that misreading.

use calamine::{open_workbook, Data, Reader, Xlsx};
use rusqlite::Connection;
use rust_xlsxwriter::{Color, Format, Workbook};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::path::{Path, PathBuf};

type R<T> = Result<T, Box<dyn Error>>;

/// Canonical GTIN key: digits only, leading zeros stripped, so the EAN-13 and
/// GTIN-14 spellings of one article compare equal. Without this the retail
/// `7640127798065` never meets the registry's `07640127798065`.
pub fn norm_gtin(s: &str) -> Option<String> {
    let digits: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
    let trimmed = digits.trim_start_matches('0');
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// First non-empty value, used to express source precedence declaratively.
fn first(vals: &[&str]) -> String {
    vals.iter()
        .find(|v| !v.trim().is_empty())
        .map(|v| v.trim().to_string())
        .unwrap_or_default()
}

/// Collapse newlines so a cell cannot break the sheet layout.
fn clean(s: &str) -> String {
    s.replace(['\n', '\r'], " ").trim().to_string()
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        s.chars().take(max).collect()
    }
}

/// Where to read each source from. Any of the optional ones may be absent —
/// a missing source narrows the report, it does not fail it.
pub struct Sources {
    pub swissdamed_db: Option<PathBuf>,
    pub migel_db: Option<PathBuf>,
    pub eudamed_db: Option<PathBuf>,
    pub firstbase_csv: Option<PathBuf>,
    pub trustbox_xlsx: Option<PathBuf>,
}

#[derive(Default, Clone)]
struct Sd {
    code: String,
    name: String,
    dtype: String,
    risk: String,
    emdn: String,
    emdn_term: String,
    desc: String,
    company: String,
    iu: String,
    iu_conf: String,
    iu_reason: String,
    ar: String,
    ar_chrn: String,
    market: String,
    model: String,
    reference: String,
    implantable: String,
    reusable: String,
    sterile: String,
    latex: String,
    measuring: String,
    self_testing: String,
    prof_testing: String,
    warnings: String,
    storage: String,
    sizes: String,
}

#[derive(Default, Clone)]
struct Eu {
    name: String,
    manufacturer: String,
    manufacturer_country: String,
    manufacturer_srn: String,
    risk: String,
    legislation: String,
    status: String,
    uuid: String,
    basic_udi: String,
    cnd_codes: String,
    cnd_terms: String,
    ar_name: String,
    ar_srn: String,
    sterile: String,
    latex: String,
    single_use: String,
    reusable: String,
    implantable: String,
    measuring: String,
    self_testing: String,
    prof_testing: String,
    storage: String,
    warnings: String,
    sizes: String,
    cert_count: String,
    cert_numbers: String,
    market_countries: String,
    market_country_count: String,
    placed_on_market: String,
    base_quantity: String,
    description: String,
    reference: String,
}

#[derive(Default, Clone)]
struct Fb {
    desc: String,
    brand: String,
    qty: String,
    unit: String,
    gpc: String,
    provider: String,
    market: String,
}

#[derive(Default, Clone)]
struct Tb {
    gpc: String,
    desc: String,
    brand: String,
    market: String,
    gross: String,
    net: String,
}

/// The 48 output columns. Index 0..2 are the partner's own; everything after is
/// ours, which is what the header colouring communicates.
const PARTNER_COLS: usize = 2;
const HEAD: &[&str] = &[
    "GTIN",
    "ProductId Partner",
    "Gefunden in (ywesee-Quellen)",
    "Anzahl Quellen",
    "Produkttitel",
    "Marke",
    "Hersteller",
    "Hersteller-Land",
    "Hersteller SRN",
    "Medizinprodukt (Ja/Nein)",
    "Medizinprodukt Klasse",
    "Regulierung (MDR/IVDR)",
    "Status",
    "Produktkategorie Code (EMDN/CND/GPC)",
    "Produktkategorie Text",
    "Zweckbestimmung (EMDN Term)",
    "Beschreibung",
    "Medizinische Fachanwendung (abgeleitet)",
    "Konfidenz",
    "Begründung",
    "CH-REP / Bevollmächtigter",
    "CH-REP CHRN/SRN",
    "Steril",
    "Latex",
    "Einmalgebrauch",
    "Wiederverwendbar",
    "Implantierbar",
    "Messfunktion",
    "Selbsttestung (IVD)",
    "Professionelle Testung (IVD)",
    "Lagerbedingungen",
    "Kritische Warnungen",
    "Klinische Grössen",
    "Zertifikate (Anzahl)",
    "Zertifikatsnummern",
    "Zielmarkt",
    "Verfügbar in Ländern",
    "Anzahl Länder",
    "Menge / Nettoinhalt",
    "Bruttogewicht",
    "MiGeL Produkt (Ja/Nein)",
    "MiGeL Code",
    "MiGeL Bezeichnung",
    "MiGeL Limitationstext",
    "swissdamed udiDiCode",
    "EUDAMED UUID",
    "EUDAMED Basic-UDI",
    "Modell / Referenz",
];

/// Everything gathered for one GTIN, plus which sources it came from.
struct Merged {
    gtin: String,
    product_id: String,
    sources: Vec<&'static str>,
    row: Vec<String>,
}

pub fn run(list_xlsx: &Path, out: &Path, src: &Sources) -> R<()> {
    // --- the partner's list, order preserved ---
    let (entries, sheet_name) = read_gtin_list(list_xlsx)?;
    let want: HashSet<String> = entries.iter().filter_map(|(_, _, k)| k.clone()).collect();
    eprintln!(
        "[gtin-report] {} rows from {} (sheet {sheet_name:?})",
        entries.len(),
        list_xlsx.display()
    );

    let sd = match &src.swissdamed_db {
        Some(p) => load_swissdamed(p, &want)?,
        None => HashMap::new(),
    };
    eprintln!("[gtin-report] swissdamed: {}", sd.len());

    let mig = match &src.migel_db {
        Some(p) => load_migel(p, &want)?,
        None => HashMap::new(),
    };
    eprintln!("[gtin-report] migel: {}", mig.len());

    let eu = match &src.eudamed_db {
        Some(p) => load_eudamed(p, &want)?,
        None => HashMap::new(),
    };
    eprintln!("[gtin-report] eudamed: {}", eu.len());

    let fb = match &src.firstbase_csv {
        Some(p) => load_firstbase(p, &want)?,
        None => HashMap::new(),
    };
    eprintln!("[gtin-report] firstbase: {}", fb.len());

    let tb = match &src.trustbox_xlsx {
        Some(p) => load_trustbox(p, &want)?,
        None => HashMap::new(),
    };
    eprintln!("[gtin-report] trustbox: {}", tb.len());

    let merged: Vec<Merged> = entries
        .iter()
        .map(|(gtin, pid, key)| {
            let k = key.clone().unwrap_or_default();
            build_row(
                gtin,
                pid,
                sd.get(&k),
                eu.get(&k),
                fb.get(&k),
                tb.get(&k),
                mig.get(&k),
            )
        })
        .collect();

    let hits = merged.iter().filter(|m| !m.sources.is_empty()).count();
    eprintln!(
        "[gtin-report] {hits} of {} GTINs found ({:.2}%)",
        merged.len(),
        100.0 * hits as f64 / merged.len().max(1) as f64
    );

    write_workbook(out, &merged, hits, src)?;
    eprintln!("[gtin-report] wrote {}", out.display());
    Ok(())
}

/// Read the partner list: first column GTIN, optional second column their own id.
fn read_gtin_list(path: &Path) -> R<(Vec<(String, String, Option<String>)>, String)> {
    let mut wb: Xlsx<_> = open_workbook(path)?;
    let sheet = wb
        .sheet_names()
        .first()
        .cloned()
        .ok_or("the workbook has no sheets")?;
    let range = wb.worksheet_range(&sheet)?;
    let mut out = Vec::new();
    for (i, row) in range.rows().enumerate() {
        if i == 0 {
            continue; // header
        }
        let gtin = cell(row, 0);
        if gtin.is_empty() {
            continue;
        }
        let key = norm_gtin(&gtin);
        out.push((gtin, cell(row, 1), key));
    }
    Ok((out, sheet))
}

/// Read a calamine cell as a trimmed string, rendering floats without the
/// spurious `.0` Excel hands back for integer-valued numeric cells — a GTIN
/// read as `7640127798065.0` would never match.
fn cell(row: &[Data], idx: usize) -> String {
    match row.get(idx) {
        Some(Data::String(s)) => s.trim().to_string(),
        Some(Data::Float(f)) => {
            if f.fract() == 0.0 {
                format!("{}", *f as i64)
            } else {
                f.to_string()
            }
        }
        Some(Data::Int(i)) => i.to_string(),
        Some(Data::Bool(b)) => b.to_string(),
        Some(Data::DateTime(d)) => d.to_string(),
        _ => String::new(),
    }
}

fn load_swissdamed(db: &Path, want: &HashSet<String>) -> R<HashMap<String, Sd>> {
    let conn = Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let mut st = conn.prepare(
        "SELECT udiDiCode, deviceName, deviceType, riskClass, emdnCode, emdnTerm,
                additionalDescription, companyName, intendedUser, iuConfidence, iuReason, rawJson
         FROM udi_details WHERE udiDiCode IS NOT NULL",
    )?;
    let mut out = HashMap::new();
    let mut rows = st.query([])?;
    while let Some(r) = rows.next()? {
        let code: String = r.get(0)?;
        let Some(k) = norm_gtin(&code) else { continue };
        if !want.contains(&k) || out.contains_key(&k) {
            continue;
        }
        let raw: Option<String> = r.get(11)?;
        let j: Value = raw
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or(Value::Null);
        let udi = j.get("udiDi");
        let ar = j
            .get("ownerInfo")
            .and_then(|o| o.get("authorizedRepresentative"));
        out.insert(
            k,
            Sd {
                code,
                name: clean(&opt(r.get::<_, Option<String>>(1)?)),
                dtype: opt(r.get(2)?),
                risk: opt(r.get(3)?),
                emdn: opt(r.get(4)?),
                emdn_term: clean(&opt(r.get::<_, Option<String>>(5)?)),
                desc: clean(&opt(r.get::<_, Option<String>>(6)?)),
                company: clean(&opt(r.get::<_, Option<String>>(7)?)),
                iu: opt(r.get(8)?),
                iu_conf: opt(r.get(9)?),
                iu_reason: truncate(&clean(&opt(r.get::<_, Option<String>>(10)?)), 200),
                ar: json_str(ar, "companyName"),
                ar_chrn: json_str(ar, "chrn"),
                market: json_str(udi, "marketStatus"),
                model: json_str(udi, "modelName"),
                reference: json_str(udi, "referenceNumber"),
                implantable: String::new(),
                reusable: String::new(),
                sterile: String::new(),
                latex: String::new(),
                measuring: String::new(),
                self_testing: String::new(),
                prof_testing: String::new(),
                warnings: truncate(&json_join(udi, "criticalWarnings", "warningValue"), 300),
                storage: truncate(&json_join(udi, "storageHandlingConditions", "typeValue"), 300),
                sizes: truncate(&json_join(udi, "clinicalSizes", "value"), 300),
            },
        );
    }
    // The yes/no characteristic columns live in their own columns, fetched
    // separately to keep the first query readable.
    let mut st2 = conn.prepare(
        "SELECT udiDiCode, implantable, reusable, sterile, latex, measuringFunction,
                selfTesting, professionalTesting
         FROM udi_details WHERE udiDiCode IS NOT NULL",
    )?;
    let mut rows2 = st2.query([])?;
    while let Some(r) = rows2.next()? {
        let code: String = r.get(0)?;
        let Some(k) = norm_gtin(&code) else { continue };
        if let Some(e) = out.get_mut(&k) {
            if e.implantable.is_empty() {
                e.implantable = opt(r.get(1)?);
                e.reusable = opt(r.get(2)?);
                e.sterile = opt(r.get(3)?);
                e.latex = opt(r.get(4)?);
                e.measuring = opt(r.get(5)?);
                e.self_testing = opt(r.get(6)?);
                e.prof_testing = opt(r.get(7)?);
            }
        }
    }
    Ok(out)
}

fn opt(v: Option<String>) -> String {
    v.unwrap_or_default()
}

fn json_str(obj: Option<&Value>, key: &str) -> String {
    obj.and_then(|o| o.get(key))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

/// Join one field out of a JSON array (`criticalWarnings[].warningValue`, …).
fn json_join(obj: Option<&Value>, array_key: &str, field: &str) -> String {
    obj.and_then(|o| o.get(array_key))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|e| {
                    e.get(field).map(|v| match v {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                })
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
                .join("; ")
        })
        .unwrap_or_default()
}

fn load_migel(db: &Path, want: &HashSet<String>) -> R<HashMap<String, (String, String, String)>> {
    let conn = Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let mut st = conn.prepare(
        "SELECT udiDiCode, migel_code, migel_bezeichnung, migel_limitation
         FROM swissdamed WHERE migel_code IS NOT NULL AND migel_code <> ''",
    )?;
    let mut out = HashMap::new();
    let mut rows = st.query([])?;
    while let Some(r) = rows.next()? {
        let code: String = r.get(0)?;
        let Some(k) = norm_gtin(&code) else { continue };
        if want.contains(&k) {
            out.entry(k).or_insert((
                opt(r.get(1)?),
                clean(&opt(r.get::<_, Option<String>>(2)?)),
                clean(&opt(r.get::<_, Option<String>>(3)?)),
            ));
        }
    }
    Ok(out)
}

fn load_eudamed(db: &Path, want: &HashSet<String>) -> R<HashMap<String, Eu>> {
    let conn = Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    conn.busy_timeout(std::time::Duration::from_secs(120))?;
    let mut out: HashMap<String, Eu> = HashMap::new();

    let mut st = conn.prepare(
        "SELECT primaryDi, tradeName, deviceName, manufacturerName, riskClass,
                deviceStatusType, applicableLegislation, authorisedRepresentativeName, uuid, basicUdi
         FROM devices_listing WHERE primaryDi IS NOT NULL",
    )?;
    let mut rows = st.query([])?;
    while let Some(r) = rows.next()? {
        let di: String = r.get(0)?;
        if !di.chars().all(|c| c.is_ascii_digit()) {
            continue; // HIBCC / EUDAMED-internal DIs are not GTINs
        }
        let Some(k) = norm_gtin(&di) else { continue };
        if !want.contains(&k) || out.contains_key(&k) {
            continue;
        }
        out.insert(
            k,
            Eu {
                name: clean(&first(&[
                    &opt(r.get::<_, Option<String>>(1)?),
                    &opt(r.get::<_, Option<String>>(2)?),
                ])),
                manufacturer: clean(&opt(r.get::<_, Option<String>>(3)?)),
                risk: opt(r.get(4)?),
                status: opt(r.get(5)?),
                legislation: opt(r.get(6)?),
                ar_name: clean(&opt(r.get::<_, Option<String>>(7)?)),
                uuid: opt(r.get(8)?),
                basic_udi: opt(r.get(9)?),
                ..Default::default()
            },
        );
    }

    // The flat detail table may not exist if only `mirror --crawl` has been run.
    let has_flat: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='device_details_flat'",
            [],
            |r| r.get::<_, i64>(0),
        )
        .unwrap_or(0)
        > 0;
    if !has_flat {
        eprintln!("[gtin-report] EUDAMED detail table absent — run `mirror --details --flatten` for the full attribute set");
        return Ok(out);
    }

    let mut st = conn.prepare(
        "SELECT primaryDi, cndCodes, cndTerms, manufacturerName, manufacturerCountry,
                manufacturerSrn, arName, arSrn, riskClass, legislation, deviceStatus,
                sterile, latex, singleUse, reusable, implantable, measuringFunction,
                selfTesting, professionalTesting, storageConditions, criticalWarnings,
                clinicalSizes, certificateCount, certificateNumbers, marketCountries,
                marketCountryCount, placedOnTheMarketCountry, baseQuantity,
                additionalDescription, reference, tradeName, uuid, basicUdiCode
         FROM device_details_flat WHERE primaryDi IS NOT NULL",
    )?;
    let mut rows = st.query([])?;
    while let Some(r) = rows.next()? {
        let di: String = r.get(0)?;
        let Some(k) = norm_gtin(&di) else { continue };
        if !want.contains(&k) {
            continue;
        }
        let e = out.entry(k).or_default();
        let g = |i: usize| -> String { r.get::<_, Option<String>>(i).ok().flatten().unwrap_or_default() };
        e.cnd_codes = g(1);
        e.cnd_terms = g(2);
        e.manufacturer = first(&[&e.manufacturer, &g(3)]);
        e.manufacturer_country = g(4);
        e.manufacturer_srn = g(5);
        e.ar_name = first(&[&e.ar_name, &g(6)]);
        e.ar_srn = g(7);
        e.risk = first(&[&e.risk, &g(8)]);
        e.legislation = first(&[&e.legislation, &g(9)]);
        e.status = first(&[&g(10), &e.status]);
        e.sterile = g(11);
        e.latex = g(12);
        e.single_use = g(13);
        e.reusable = g(14);
        e.implantable = g(15);
        e.measuring = g(16);
        e.self_testing = g(17);
        e.prof_testing = g(18);
        e.storage = g(19);
        e.warnings = g(20);
        e.sizes = g(21);
        e.cert_count = g(22);
        e.cert_numbers = g(23);
        e.market_countries = g(24);
        e.market_country_count = g(25);
        e.placed_on_market = g(26);
        e.base_quantity = g(27);
        e.description = g(28);
        e.reference = g(29);
        e.name = first(&[&e.name, &g(30)]);
        e.uuid = first(&[&e.uuid, &g(31)]);
        e.basic_udi = first(&[&e.basic_udi, &g(32)]);
    }
    Ok(out)
}

fn load_firstbase(csv_path: &Path, want: &HashSet<String>) -> R<HashMap<String, Fb>> {
    let mut rdr = csv::ReaderBuilder::new().flexible(true).from_path(csv_path)?;
    let headers = rdr.headers()?.clone();
    let idx = |name: &str| headers.iter().position(|h| h.trim_start_matches('\u{feff}') == name);
    let (i_gtin, i_desc, i_brand, i_qty, i_unit, i_gpc, i_prov, i_market) = (
        idx("Gtin").ok_or("firstbase.csv has no Gtin column")?,
        idx("TradeItemDescription_DE"),
        idx("BrandName"),
        idx("NetContent_Value"),
        idx("NetContent_MeasurementUnitCode"),
        idx("GpcCategoryCode"),
        idx("InformationProviderPartyName"),
        idx("TargetMarketCountryCode"),
    );
    let mut out = HashMap::new();
    for rec in rdr.records() {
        let rec = match rec {
            Ok(r) => r,
            Err(_) => continue, // a malformed line must not abort a 190k-row file
        };
        let Some(k) = rec.get(i_gtin).and_then(norm_gtin) else { continue };
        if !want.contains(&k) || out.contains_key(&k) {
            continue;
        }
        let f = |i: Option<usize>| i.and_then(|i| rec.get(i)).unwrap_or("").trim().to_string();
        out.insert(
            k,
            Fb {
                desc: f(i_desc),
                brand: f(i_brand),
                qty: f(i_qty),
                unit: f(i_unit),
                gpc: f(i_gpc),
                provider: f(i_prov),
                market: f(i_market),
            },
        );
    }
    Ok(out)
}

fn load_trustbox(xlsx: &Path, want: &HashSet<String>) -> R<HashMap<String, Tb>> {
    let mut wb: Xlsx<_> = open_workbook(xlsx)?;
    let sheet = wb
        .sheet_names()
        .first()
        .cloned()
        .ok_or("trustbox workbook has no sheets")?;
    let range = wb.worksheet_range(&sheet)?;
    let mut out = HashMap::new();
    // Two header rows: a human label row and a machine field-name row.
    for (i, row) in range.rows().enumerate() {
        if i < 2 {
            continue;
        }
        let Some(k) = norm_gtin(&cell(row, 1)) else { continue };
        if !want.contains(&k) || out.contains_key(&k) {
            continue;
        }
        out.insert(
            k,
            Tb {
                gpc: cell(row, 4),
                desc: first(&[&cell(row, 6), &cell(row, 5)]),
                brand: cell(row, 10),
                market: cell(row, 3),
                gross: cell(row, 12),
                net: cell(row, 13),
            },
        );
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn build_row(
    gtin: &str,
    product_id: &str,
    sd: Option<&Sd>,
    eu: Option<&Eu>,
    fb: Option<&Fb>,
    tb: Option<&Tb>,
    mig: Option<&(String, String, String)>,
) -> Merged {
    let mut sources = Vec::new();
    if sd.is_some() {
        sources.push("swissdamed");
    }
    if eu.is_some() {
        sources.push("EUDAMED");
    }
    if fb.is_some() {
        sources.push("Firstbase");
    }
    if tb.is_some() {
        sources.push("Trustbox");
    }

    let s = |f: fn(&Sd) -> &String| sd.map(f).cloned().unwrap_or_default();
    let e = |f: fn(&Eu) -> &String| eu.map(f).cloned().unwrap_or_default();

    // Registered as a medical device in either registry; found only in a GS1
    // catalogue means the opposite, and found nowhere stays blank rather than
    // asserting something we did not check.
    let is_md = if sd.is_some() || eu.is_some() {
        "Ja".to_string()
    } else if fb.is_some() || tb.is_some() {
        "Nein (nicht als MD registriert)".to_string()
    } else {
        String::new()
    };

    let qty = match fb {
        Some(f) if !f.qty.is_empty() => format!("{} {}", f.qty, f.unit).trim().to_string(),
        _ => first(&[
            &tb.map(|t| t.net.clone()).unwrap_or_default(),
            &e(|x| &x.base_quantity),
        ]),
    };

    let row = vec![
        gtin.to_string(),
        product_id.to_string(),
        sources.join(", "),
        if sources.is_empty() {
            String::new()
        } else {
            sources.len().to_string()
        },
        first(&[
            &s(|x| &x.name),
            &e(|x| &x.name),
            &fb.map(|f| f.desc.clone()).unwrap_or_default(),
            &tb.map(|t| t.desc.clone()).unwrap_or_default(),
        ]),
        first(&[
            &fb.map(|f| f.brand.clone()).unwrap_or_default(),
            &tb.map(|t| t.brand.clone()).unwrap_or_default(),
        ]),
        first(&[
            &s(|x| &x.company),
            &e(|x| &x.manufacturer),
            &fb.map(|f| f.provider.clone()).unwrap_or_default(),
        ]),
        e(|x| &x.manufacturer_country),
        e(|x| &x.manufacturer_srn),
        is_md,
        first(&[&s(|x| &x.risk), &e(|x| &x.risk)]),
        first(&[&s(|x| &x.dtype), &e(|x| &x.legislation)]),
        first(&[&e(|x| &x.status), &s(|x| &x.market)]),
        first(&[
            &s(|x| &x.emdn),
            &e(|x| &x.cnd_codes),
            &fb.map(|f| f.gpc.clone()).unwrap_or_default(),
            &tb.map(|t| t.gpc.clone()).unwrap_or_default(),
        ]),
        first(&[&s(|x| &x.emdn_term), &e(|x| &x.cnd_terms)]),
        s(|x| &x.emdn_term),
        first(&[&s(|x| &x.desc), &e(|x| &x.description)]),
        s(|x| &x.iu),
        s(|x| &x.iu_conf),
        s(|x| &x.iu_reason),
        first(&[&s(|x| &x.ar), &e(|x| &x.ar_name)]),
        first(&[&s(|x| &x.ar_chrn), &e(|x| &x.ar_srn)]),
        first(&[&s(|x| &x.sterile), &e(|x| &x.sterile)]),
        first(&[&s(|x| &x.latex), &e(|x| &x.latex)]),
        e(|x| &x.single_use),
        first(&[&s(|x| &x.reusable), &e(|x| &x.reusable)]),
        first(&[&s(|x| &x.implantable), &e(|x| &x.implantable)]),
        first(&[&s(|x| &x.measuring), &e(|x| &x.measuring)]),
        first(&[&s(|x| &x.self_testing), &e(|x| &x.self_testing)]),
        first(&[&s(|x| &x.prof_testing), &e(|x| &x.prof_testing)]),
        first(&[&s(|x| &x.storage), &e(|x| &x.storage)]),
        first(&[&s(|x| &x.warnings), &e(|x| &x.warnings)]),
        first(&[&s(|x| &x.sizes), &e(|x| &x.sizes)]),
        e(|x| &x.cert_count),
        e(|x| &x.cert_numbers),
        first(&[
            &fb.map(|f| f.market.clone()).unwrap_or_default(),
            &tb.map(|t| t.market.clone()).unwrap_or_default(),
            &e(|x| &x.placed_on_market),
        ]),
        e(|x| &x.market_countries),
        e(|x| &x.market_country_count),
        qty,
        tb.map(|t| t.gross.clone()).unwrap_or_default(),
        if mig.is_some() {
            "Ja".to_string()
        } else if sd.is_some() || eu.is_some() {
            "Nein".to_string()
        } else {
            String::new()
        },
        mig.map(|m| m.0.clone()).unwrap_or_default(),
        mig.map(|m| m.1.clone()).unwrap_or_default(),
        mig.map(|m| m.2.clone()).unwrap_or_default(),
        s(|x| &x.code),
        e(|x| &x.uuid),
        e(|x| &x.basic_udi),
        first(&[&s(|x| &x.model), &s(|x| &x.reference), &e(|x| &x.reference)]),
    ];
    debug_assert_eq!(row.len(), HEAD.len());

    Merged {
        gtin: gtin.to_string(),
        product_id: product_id.to_string(),
        sources,
        row,
    }
}

fn write_workbook(out: &Path, merged: &[Merged], hits: usize, src: &Sources) -> R<()> {
    let mut wb = Workbook::new();

    // Partner columns grey, ours green: the single clearest signal that the file
    // was added to rather than merely returned.
    let partner_head = Format::new()
        .set_bold()
        .set_background_color(Color::RGB(0xE8EAED))
        .set_text_wrap();
    let ours_head = Format::new()
        .set_bold()
        .set_background_color(Color::RGB(0xC8E6C9))
        .set_text_wrap();

    let mut write_sheet = |wb: &mut Workbook, name: &str, only_hits: bool| -> R<()> {
        let ws = wb.add_worksheet().set_name(name)?;
        for (c, h) in HEAD.iter().enumerate() {
            let fmt = if c < PARTNER_COLS { &partner_head } else { &ours_head };
            ws.write_string_with_format(0, c as u16, *h, fmt)?;
        }
        ws.set_freeze_panes(1, PARTNER_COLS as u16)?;
        let mut r = 1u32;
        for m in merged {
            if only_hits && m.sources.is_empty() {
                continue;
            }
            for (c, v) in m.row.iter().enumerate() {
                if !v.is_empty() {
                    ws.write_string(r, c as u16, v)?;
                }
            }
            r += 1;
        }
        Ok(())
    };

    write_sheet(&mut wb, &format!("Treffer ({hits})"), true)?;

    // Orientation sheet: without it the full list opens on empty rows and reads
    // as "nothing was delivered".
    {
        let ws = wb.add_worksheet().set_name("Info ywesee")?;
        ws.set_column_width(0, 118)?;
        let title = Format::new().set_bold().set_font_size(14);
        let bold = Format::new().set_bold();
        let mut r = 0u32;
        ws.write_string_with_format(r, 0, "GTIN-Auswertung ywesee", &title)?;
        r += 2;
        for (line, is_head) in info_lines(merged.len(), hits, src) {
            if is_head {
                ws.write_string_with_format(r, 0, &line, &bold)?;
            } else if !line.is_empty() {
                ws.write_string(r, 0, &line)?;
            }
            r += 1;
        }
    }

    write_sheet(&mut wb, "GTIN Liste", false)?;
    wb.save(out)?;
    Ok(())
}

/// `(line, is_heading)` pairs for the orientation sheet.
fn info_lines(total: usize, hits: usize, src: &Sources) -> Vec<(String, bool)> {
    let mut v: Vec<(String, bool)> = Vec::new();
    let mut head = |s: &str, out: &mut Vec<(String, bool)>| out.push((s.to_string(), true));
    let mut line = |s: String, out: &mut Vec<(String, bool)>| out.push((s, false));

    head("Was wurde ergänzt?", &mut v);
    line(
        "Dies ist eure Originaldatei, von uns um zusätzliche Spalten erweitert. Eure beiden Spalten".into(),
        &mut v,
    );
    line("(GTIN, ProductId) sind unverändert. Alle Spalten ab Spalte C stammen von ywesee.".into(), &mut v);
    line("In den Kopfzeilen sind eure Spalten GRAU und unsere Ergänzungen GRÜN hinterlegt.".into(), &mut v);
    line(String::new(), &mut v);

    head("Tabs in dieser Datei", &mut v);
    line(format!("  1. «Treffer ({hits})» — nur die GTINs, zu denen wir Daten haben. Hier anfangen."), &mut v);
    line("  2. «Info ywesee» — diese Übersicht".into(), &mut v);
    line(format!("  3. «GTIN Liste» — alle {total} GTINs, inklusive der ohne Treffer"), &mut v);
    line(String::new(), &mut v);

    head("Wichtig zum Tab «GTIN Liste»", &mut v);
    line(format!(
        "{hits} von {total} GTINs ({:.1} %) sind in unseren Quellen enthalten.",
        100.0 * hits as f64 / total.max(1) as f64
    ), &mut v);
    line("Die übrigen Zeilen sind in unseren Spalten leer — das ist kein Fehler, sondern bedeutet:".into(), &mut v);
    line("in keiner unserer Quellen gefunden. Deshalb der separate Tab «Treffer».".into(), &mut v);
    line(String::new(), &mut v);

    head("Verwendete Quellen", &mut v);
    for (label, path) in [
        ("swissdamed", &src.swissdamed_db),
        ("MiGeL", &src.migel_db),
        ("EUDAMED", &src.eudamed_db),
        ("Firstbase", &src.firstbase_csv),
        ("Trustbox", &src.trustbox_xlsx),
    ] {
        match path {
            Some(p) => line(
                format!("  {label}: {}", p.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default()),
                &mut v,
            ),
            None => line(format!("  {label}: nicht verwendet"), &mut v),
        }
    }
    line(String::new(), &mut v);

    head("Hinweis zur Spalte «Medizinische Fachanwendung»", &mut v);
    line("Das ist eine abgeleitete Einschätzung mit Konfidenzangabe, keine regulatorische".into(), &mut v);
    line("Festlegung. Massgeblich bleibt die Gebrauchsanweisung des Herstellers.".into(), &mut v);
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gtin_normalization_matches_ean13_and_gtin14() {
        // The same article spelled both ways must collapse to one key, or a
        // retail GTIN never meets its registry entry.
        assert_eq!(norm_gtin("7640127798065"), norm_gtin("07640127798065"));
        assert_eq!(norm_gtin("7640-127-798065").unwrap(), "7640127798065");
        // Placeholder rows in GS1 dumps must not become a key.
        assert_eq!(norm_gtin("00000000000000"), None);
        assert_eq!(norm_gtin(""), None);
    }

    #[test]
    fn numeric_cells_do_not_pick_up_a_decimal_point() {
        // Excel hands integer-valued cells back as floats; a GTIN rendered as
        // "7640127798065.0" would silently never match.
        let row = vec![Data::Float(7640127798065.0), Data::Float(1.5)];
        assert_eq!(cell(&row, 0), "7640127798065");
        assert_eq!(cell(&row, 1), "1.5");
        assert_eq!(cell(&row, 9), "");
    }

    #[test]
    fn source_precedence_prefers_swissdamed_then_eudamed() {
        assert_eq!(first(&["", "  ", "eu", "fb"]), "eu");
        assert_eq!(first(&["sd", "eu"]), "sd");
        assert_eq!(first(&["", ""]), "");
    }

    #[test]
    fn a_row_reports_its_sources_and_medical_device_status() {
        let sd = Sd { code: "0764".into(), name: "Bandage".into(), risk: "CLASS_I".into(), ..Default::default() };
        let m = build_row("764", "p1", Some(&sd), None, None, None, None);
        assert_eq!(m.sources, vec!["swissdamed"]);
        assert_eq!(m.row[HEAD.iter().position(|h| *h == "Medizinprodukt (Ja/Nein)").unwrap()], "Ja");
        assert_eq!(m.row[HEAD.iter().position(|h| *h == "Produkttitel").unwrap()], "Bandage");
        // Found in neither registry nor a catalogue: we assert nothing.
        let empty = build_row("999", "p2", None, None, None, None, None);
        assert!(empty.sources.is_empty());
        assert_eq!(empty.row[HEAD.iter().position(|h| *h == "Medizinprodukt (Ja/Nein)").unwrap()], "");
        // A GS1-only hit is positively NOT a registered device.
        let fb = Fb { brand: "Acme".into(), ..Default::default() };
        let gs1 = build_row("888", "p3", None, None, Some(&fb), None, None);
        assert_eq!(
            gs1.row[HEAD.iter().position(|h| *h == "Medizinprodukt (Ja/Nein)").unwrap()],
            "Nein (nicht als MD registriert)"
        );
    }

    #[test]
    fn every_row_matches_the_header_width() {
        // A drifting column count would silently shift every value one cell over.
        let m = build_row("1", "", None, None, None, None, None);
        assert_eq!(m.row.len(), HEAD.len());
        assert_eq!(m.gtin, "1");
        assert!(m.product_id.is_empty());
    }

    #[test]
    fn json_arrays_are_joined_by_the_requested_field() {
        let v: Value = serde_json::json!({
            "criticalWarnings": [{"warningValue": "CW018"}, {"warningValue": "CW262"}],
            "clinicalSizes": []
        });
        assert_eq!(json_join(Some(&v), "criticalWarnings", "warningValue"), "CW018; CW262");
        assert_eq!(json_join(Some(&v), "clinicalSizes", "value"), "");
        assert_eq!(json_join(None, "criticalWarnings", "warningValue"), "");
    }
}
