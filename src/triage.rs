//! Triage classifier: "Medizinprodukt für das Publikum" (public / layperson) vs
//! "für Fachanwender" (professional user).
//!
//! swissdamed/EUDAMED expose NO structured intended-user field for MDR/MDD (95%
//! of the corpus) — nor for IVD (verified: the EUDAMED Basic UDI-DI `medicalPurpose`
//! field is populated for System/Procedure Packs only, empty for normal devices).
//! The only structured signal is the IVD `selfTesting` / `nearPatientTesting` /
//! `professionalTesting` triad — and per regulatory review (Maik, 21.08.2026) even
//! that describes the *testing modality / regulatory risk*, not a supply-chain
//! intended-user designation. A true consumer-vs-professional flag
//! (`isTradeItemAConsumerUnit` / professional-use) is a GS1 GDSN supply-chain
//! attribute, which EUDAMED does not carry for MD or IVD. This classifier is
//! therefore **decision-support / triage, not a compliance determination**: it
//! is deliberately asymmetric — it only ever labels `public` from a structured
//! signal (IVD self-testing), MiGeL membership (KLV Art. 20 lay-use), or an
//! explicit manufacturer lay-use statement, because a false `public` (selling a
//! professional-only device) is a compliance breach, whereas a false
//! `professional` is only a lost sale. Everything without a reliable signal is
//! left as `review` and MUST be verified against the manufacturer's
//! IFU/Zweckbestimmung and the Swiss MepV Abgabe rules before listing.
//!
//! Output columns (added to the `udi_details` table):
//!   intendedUser  ∈ {professional, public, review}
//!   iuConfidence  ∈ {high, medium, low}
//!   iuReason      short human-readable justification

use rusqlite::Connection;
use serde_json::Value;
use std::path::PathBuf;

/// Explicit professional-only phrases (manufacturer statements). Distinctive
/// enough to avoid substring false positives.
const PROF_PHRASES: &[&str] = &[
    "for professional use",
    "professional use only",
    "healthcare professional",
    "health care professional",
    "for use by a physician",
    "by a physician",
    "medical professional",
    "prescription only",
    "for use by trained",
    "for use by qualified",
    "hospital use only",
    "for clinical use by",
    "to be used by a doctor",
];

/// EMDN top-level categories that are procedurally professional (surgical /
/// interventional / sterilisation). Used only in the *exclusion* direction
/// (→ professional), never to assert public, and at low confidence — a false
/// professional is a lost sale, not a compliance breach. Regulatory review of
/// this list is recommended before operational use.
///   L surgical instruments · K minimally-invasive/electrosurgery ·
///   C cardiocirculatory (electrophysiology) · G GI endoscopy · H sutures ·
///   J cardiac leads/programmers · S sterilisation equipment ·
///   B hemotransfusion/hematology (blood bags, apheresis) ·
///   D disinfectants/detergents for reprocessing medical devices ·
///   P implantable prosthetics & osteosynthesis (surgical implants — most also
///     caught by the high-risk gate; P sweeps prosthetic accessories/instruments).
///   Z healthcare equipment & accessories (OR/ICU/imaging hardware: scialytic
///     lamps, operating tables, electrosurgery, endoscopy stacks, linear
///     accelerators, multi-parameter monitors) — added 27.08.2026.
///   A administration, withdrawal & collection (clinical infusion hardware:
///     extension lines, flow regulators, stopcocks, infusion/irrigation kits,
///     biopsy and venipuncture needles, surgical drainage, suction containers)
///     — added 28.08.2026.
/// B/D/P/L/K/S are verified single-sided in the corpus (0% MiGeL lay-match).
/// Deliberately EXCLUDES F (dialysis — home peritoneal dialysis is patient-run).
///
/// **Z and A are NOT single-sided** and are the reason
/// `EMDN_PROFESSIONAL_EXEMPT_CODES` exists. EMDN files home respiratory therapy,
/// insulin pumps and glucose meters under Z as "equipment" alongside the
/// operating theatre (999 of 18,425 Z rows hold a MiGeL match), and files the
/// entire ostomy / urine-bag / pen-needle business under A next to hospital IV
/// hardware (652 of 4,003 A rows hold a MiGeL match). Those matched rows are
/// already safe (TIER 4 outranks TIER 6), but their *unmatched* siblings — the
/// same CPAP or the same colostomy bag from a maker the matcher misses — would be
/// asserted professional here without the exemption list below.
const EMDN_PROFESSIONAL_CATS: &[char] =
    &['L', 'K', 'C', 'G', 'H', 'J', 'S', 'B', 'D', 'P', 'Z', 'A'];

/// Leaf codes inside an otherwise-professional EMDN category that are proven
/// home/lay-capable, so TIER 6 must NOT fire on them. An exempt row is simply
/// handed on to TIER 7 and judged by the ordinary MepV presumption there — TIER 6
/// itself never emits `public`, so this list can only ever *withhold* a
/// professional assertion, never manufacture a public one. (Where it lands then
/// depends on the category: an exempt Z row reaches `review`, an exempt A row
/// reaches `public/low`, because A is also in `EMDN_CONSUMER_LEAN_CATS`.)
///
/// Derived from evidence, not judgement: every prefix here is a leaf where the
/// corpus actually holds a MiGeL match (27.08.2026), i.e. the position is
/// reimbursable for self-application under KLV Art. 20. Sibling leaves without
/// such evidence (clinical spirometers, body plethysmographs, dermatoscopes,
/// blood flow meters) are deliberately NOT exempt, which is why this is a list of
/// specific leaves and not the parent prefixes `Z1215` / `Z120401`.
///
/// The A entries additionally cover home enteral nutrition, which carries no
/// MiGeL evidence (tube feed is reimbursed as Spezialnahrung, not as a MiGeL
/// position) but is unambiguously patient-run — a documented maintainer call,
/// safe because it only withholds a professional assertion.
const EMDN_PROFESSIONAL_EXEMPT_CODES: &[&str] = &[
    // Respiratory therapy at home — the largest genuine cluster in Z.
    "Z12030102",    // continuous positive pressure equipment (CPAP)
    "Z12030103",    // pulmonary ventilators for non-hospital use
    "Z12030104",    // portable pulmonary ventilators
    "Z1203010502",  // adult pulmonary ventilators
    "Z120309",      // medical gas pipeline systems + accessories (O2 regulators)
    "Z12159002",    // aerosol equipment
    "Z12159004",    // oxygen concentrators
    "Z12159099",    // various pneumology / respiratory physiopathology
    "Z12150102",    // peak flow spirometers (NOT Z12150101 clinical spirometers)
    "Z12040210",    // ultrasonic nebulisers
    // Diabetes self-management.
    "Z12040115",    // blood sugar monitoring systems (invasive + non-invasive)
    "Z12040216",    // portable microinfusors (insulin pumps) + consumables
    // Point-of-care vitals a patient owns.
    "Z1203020408",  // pulse oximeters
    "Z1203020501",  // non-invasive oscillometric blood pressure gauges
    // Misc verified home positions.
    "Z12080303",    // breast pumps
    "Z12019003",    // wound treatment equipment (NPWT is issued for home use too)
    // --- Category A: the lay business filed next to hospital IV hardware. ---
    // Abdominal ostomy, whole branch (bags, plates, peristomal skin care) —
    // core MiGeL ch. 29.01. Only 28 of these rows carry a MiGeL match today, so
    // without the exemption the other ~250 would be asserted professional.
    "A10",
    // Urinary drainage — MiGeL ch. 15.14 / 15.15.
    "A06030301",    // urine collection bags (350 MiGeL matches)
    "A06030399",    // urine collection systems - other
    "A0680",        // drainage/collection accessories (leg-bag straps, holders)
    // Diabetes self-injection — MiGeL 03.07.09.
    "A0101010201",  // hypodermic pen needles, with safety systems
    "A0101010202",  // hypodermic pen needles, w/o safety systems
    // Disposable syringes a patient buys — MiGeL 03.07.10.10.
    "A020102",      // infusion and irrigation syringes (all cone/piece variants)
    // Home enteral nutrition (patient- or carer-run; no MiGeL position exists).
    "A030403",      // enteral nutrition kits, incl. via pump
    "A03010302",    // enteral feeding pump controllers
    "A080101",      // enteral feeding bags and containers
];

/// EMDN categories that lean public but are NOT auto-classified (orthoses are
/// often professionally fitted/prescribed; TENS spans both). Left as `review`
/// with a lean note so a human starts from the right prior.
///   Y technical aids for disabled persons (orthoses) · N neuro (TENS).
const EMDN_PUBLIC_LEAN_CATS: &[char] = &['Y', 'N'];

/// Maintainer-curated EMDN **leaf-code prefixes** for devices verified to be
/// consumer products that fall OUTSIDE MiGeL — freely sellable to the public with
/// no MepV Abgabe restriction, but not a reimbursement item (so KLV Art. 20 does
/// not reach them). This is a documented human determination, NOT a structured or
/// legal-list signal → classified `public/low`. Use specific leaf codes only
/// (never a bare category letter), so a false `public` stays near-impossible.
///   V0807 anti-decubitus mattresses (active + non-active, Class I/IIa) ·
///   V08030102 non-active anti-decubitus cushions ·
///   V0811 heel/elbow/knee anti-decubitus protection aids.
const PUBLIC_EMDN_CODES: &[&str] = &["V0807", "V08030102", "V0811"];

/// EMDN top-level categories whose devices are of a consumer-usable *type*
/// (dressings, aids for the disabled, incontinence protection, ostomy/self-care,
/// TENS). Combined with a low risk class and no professional signal, these feed
/// the MepV "sellable-to-consumer unless restricted" presumption (TIER 7). This
/// is a **shop-sellability** presumption (may it be sold to the public), NOT an
/// intended-user determination — kept separate from the professional-equipment
/// categories (Q/W/R/V, and Z since 27.08.2026) which do not.
///   M dressings · Y aids for disabled (orthoses) · T incontinence protection ·
///   A administration/collection (ostomy, self-cath) · N neuro (TENS).
/// Note A is deliberately in BOTH this list and `EMDN_PROFESSIONAL_CATS`: TIER 6
/// removes the clinical bulk of A, and what survives via
/// `EMDN_PROFESSIONAL_EXEMPT_CODES` is exactly the ostomy / urine-bag / needle
/// business this presumption is meant to catch.
const EMDN_CONSUMER_LEAN_CATS: &[char] = &['M', 'Y', 'T', 'A', 'N'];

/// Explicit lay/consumer-use phrases.
const LAY_PHRASES: &[&str] = &[
    "for home use",
    "home use by the patient",
    "self-application",
    "self application",
    "for lay users",
    "for lay persons",
    "for laypersons",
    "over the counter",
    "intended for consumers",
    "for use by the patient at home",
];

fn sstr(o: &Value, k: &str) -> String {
    o.get(k).and_then(|v| v.as_str()).unwrap_or("").to_string()
}

fn is_true(o: &Value, k: &str) -> bool {
    matches!(o.get(k), Some(Value::Bool(true)))
}

/// Lowercased blob of the free-text fields that might carry a user statement:
/// deviceName, additionalDescription, and critical-warning comment texts.
fn text_blob(basic: &Value, udi: &Value) -> String {
    let mut parts: Vec<String> = vec![sstr(basic, "deviceName")];
    if let Some(arr) = udi.get("additionalDescription").and_then(|v| v.as_array()) {
        for e in arr {
            parts.push(sstr(e, "textValue"));
        }
    }
    if let Some(cws) = udi.get("criticalWarnings").and_then(|v| v.as_array()) {
        for cw in cws {
            if let Some(cs) = cw.get("comments").and_then(|v| v.as_array()) {
                for c in cs {
                    parts.push(sstr(c, "textValue"));
                }
            }
        }
    }
    parts.join(" \u{1f}").to_lowercase()
}

fn emdn_first(udi: &Value) -> (String, String) {
    udi.get("nomenclatureCodes")
        .and_then(|v| v.as_array())
        .and_then(|a| a.first())
        .map(|nc| (sstr(nc, "code"), sstr(nc, "term")))
        .unwrap_or_default()
}

/// Classify one detail record. Returns (intendedUser, confidence, reason).
///
/// `migel_code` is this device's matched MiGeL position (`None`/empty when the
/// MiGeL matcher found none). A genuine MiGeL match is a legally-anchored lay-use
/// signal: KLV Art. 20 restricts MiGeL to items for *Selbstanwendung* (use by the
/// insured person or a non-professionally-involved helper).
///
/// Rule order matters — first match wins, most-reliable first. The `public`
/// branch is reached only via structured IVD self-testing, MiGeL membership, or
/// an explicit lay statement — never inferred from risk class / EMDN alone, which
/// the data proves do NOT identify public devices.
pub fn classify(detail: &Value, migel_code: Option<&str>) -> (String, String, String) {
    let basic = detail.get("basicUdi").cloned().unwrap_or(Value::Null);
    let udi = detail.get("udiDi").cloned().unwrap_or(Value::Null);
    let dtype = sstr(detail, "deviceType");
    let risk = sstr(&basic, "riskClass");
    let is_ivd = dtype.starts_with("IVD");
    let migel = migel_code.unwrap_or("");
    let has_migel = !migel.is_empty();
    let (ecode, eterm) = emdn_first(&udi);
    let ecat = ecode.chars().next().unwrap_or(' ');

    let prof = |c: &str, r: String| ("professional".to_string(), c.to_string(), r);
    let public = |c: &str, r: String| ("public".to_string(), c.to_string(), r);
    let review = |c: &str, r: String| ("review".to_string(), c.to_string(), r);

    // TIER 1 — structured, high-confidence professional (never sold to public).
    // A MiGeL match on a high-risk device is a genuine conflict (e.g. Omnipod 5:
    // CLASS_III yet a legitimate MiGeL home device) — don't pick a side, route to
    // manual review.
    let high_risk_why = if dtype == "AIMDD" {
        Some("active implantable device".to_string())
    } else if is_true(&basic, "implantable") {
        Some("implantable".to_string())
    } else if risk == "CLASS_III" || risk == "CLASS_D" {
        Some(format!("high-risk {}", risk))
    } else {
        None
    };
    if let Some(why) = high_risk_why {
        if has_migel {
            return review(
                "low",
                format!("MiGeL-listed {} but {} — verify IFU/Abgabe", migel, why),
            );
        }
        return prof("high", why);
    }

    // TIER 2 — structured, high-confidence public (IVD self-testing).
    if is_true(&basic, "selfTesting") {
        return public("high", "IVD selfTesting flag".into());
    }
    if risk == "IVD_DEVICES_SELF_TESTING" {
        return public("high", "IVD self-testing risk class".into());
    }

    // TIER 3 — structured IVD professional signal (medium). A structured
    // professional-testing field outranks the MiGeL lay-use lean below.
    if is_ivd && is_true(&basic, "professionalTesting") {
        return prof("medium", "IVD professionalTesting flag".into());
    }
    if is_ivd && is_true(&basic, "nearPatientTesting") {
        return prof("medium", "IVD nearPatientTesting flag".into());
    }

    // TIER 4 — MiGeL membership → lay-use lean (medium). MiGeL (KLV Art. 20) lists
    // only items for Selbstanwendung: a legally-anchored lay-use signal that
    // outweighs the noisy free-text phrases below. Still a lean, not proof —
    // verify against the IFU before listing.
    if has_migel {
        return public(
            "medium",
            format!("MiGeL-listed {} → KLV Art. 20 Selbstanwendung (lay-use)", migel),
        );
    }

    // TIER 4b — maintainer-curated consumer EMDN leaf codes: verified consumer
    // products that fall OUTSIDE MiGeL (e.g. anti-decubitus mattresses — Class I,
    // freely sellable, no MepV Abgabe restriction, but not a reimbursement item so
    // KLV Art. 20 never reaches them). A documented human determination, not a
    // structured/legal-list signal → public/low.
    if PUBLIC_EMDN_CODES.iter().any(|p| ecode.starts_with(p)) {
        return public(
            "low",
            format!(
                "maintainer-verified consumer product, no MepV Abgabe restriction (EMDN {})",
                ecode
            ),
        );
    }

    // TIER 5 — explicit manufacturer text statements (medium).
    let blob = text_blob(&basic, &udi);
    for p in PROF_PHRASES {
        if blob.contains(p) {
            return prof("medium", format!("text: \"{}\"", p));
        }
    }
    for p in LAY_PHRASES {
        if blob.contains(p) {
            return public("medium", format!("text: \"{}\"", p));
        }
    }

    // TIER 6 — EMDN clearly-professional category (surgical/interventional).
    // Exclusion direction only, low confidence.
    if EMDN_PROFESSIONAL_CATS.contains(&ecat)
        && !EMDN_PROFESSIONAL_EXEMPT_CODES
            .iter()
            .any(|p| ecode.starts_with(p))
    {
        return prof("low", format!("EMDN professional category {} ({})", ecat, eterm));
    }

    // TIER 7 — presumptive consumer (MepV shop-sellability default). A device that
    // survived EVERY professional gate above (not implant/high-risk, not IVD-prof,
    // not surgical/interventional EMDN, no prof text) and is low-risk MDR/MDD may
    // be sold to the public under MepV — no Abgabe restriction found. This answers
    // "may it be sold to a consumer", NOT "who is it intended for": public/low, a
    // restriction-not-found presumption, verify IFU before listing.
    //   - CLASS_I: broad — the lowest MDR risk; the professional Class I devices
    //     (reusable surgical instruments) are already excluded via EMDN L/K above.
    //   - CLASS_IIA: only for consumer-type EMDN categories (dressings/aids/…),
    //     since IIa carries more genuinely professional devices; the rest stay
    //     `review`.
    if matches!(dtype.as_str(), "MDR" | "MDD") {
        if risk == "CLASS_I" {
            return public(
                "low",
                format!(
                    "presumptively consumer: Class I low-risk, no professional restriction — sellable under MepV, verify IFU (EMDN: {})",
                    if eterm.is_empty() { "n/a" } else { &eterm }
                ),
            );
        }
        if risk == "CLASS_IIA" && EMDN_CONSUMER_LEAN_CATS.contains(&ecat) {
            return public(
                "low",
                format!(
                    "presumptively consumer: Class IIa + consumer-type EMDN ({}), no professional restriction — sellable under MepV, verify IFU",
                    eterm
                ),
            );
        }
    }

    // Default — no reliable signal: manual IFU/Abgabe check required. Surface an
    // EMDN public lean where applicable so a reviewer starts from the right prior.
    let reason = if ecode.is_empty() {
        "no reliable user signal — verify IFU + MepV Abgabe".to_string()
    } else if EMDN_PUBLIC_LEAN_CATS.contains(&ecat) {
        format!(
            "no reliable signal — verify IFU/Abgabe (public-leaning EMDN: {})",
            eterm
        )
    } else {
        format!("no reliable signal — verify IFU/Abgabe (EMDN: {})", eterm)
    };
    ("review".to_string(), "low".to_string(), reason)
}

/// The three triage column names, in output order.
pub const COLUMNS: &[&str] = &["intendedUser", "iuConfidence", "iuReason"];

/// Load `udiDiCode → migel_code` for every MiGeL-matched device from the MiGeL DB
/// (fixed `swissdamed_migel.db`, else the newest legacy `swissdamed_migel_*.db`).
/// Best-effort: returns an empty map if the DB/table is absent or unreadable, so
/// the classifier degrades gracefully to its structured/text signals only.
pub fn load_migel_matches(db_dir: &std::path::Path) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    let path = {
        let fixed = db_dir.join("swissdamed_migel.db");
        if fixed.exists() {
            Some(fixed)
        } else {
            let mut best: Option<(std::time::SystemTime, PathBuf)> = None;
            if let Ok(rd) = std::fs::read_dir(db_dir) {
                for e in rd.flatten() {
                    let p = e.path();
                    let n = p
                        .file_name()
                        .map(|s| s.to_string_lossy().to_string())
                        .unwrap_or_default();
                    if n.starts_with("swissdamed_migel_") && n.ends_with(".db") {
                        if let Some(mt) = e.metadata().ok().and_then(|m| m.modified().ok()) {
                            if best.as_ref().map(|(t, _)| mt > *t).unwrap_or(true) {
                                best = Some((mt, p));
                            }
                        }
                    }
                }
            }
            best.map(|(_, p)| p)
        }
    };
    let path = match path {
        Some(p) => p,
        None => return map,
    };
    let conn = match Connection::open(&path) {
        Ok(c) => c,
        Err(_) => return map,
    };
    let mut stmt = match conn.prepare(
        "SELECT udiDiCode, migel_code FROM swissdamed \
         WHERE migel_code IS NOT NULL AND migel_code != ''",
    ) {
        Ok(s) => s,
        Err(_) => return map,
    };
    if let Ok(it) = stmt.query_map([], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
    }) {
        for (code, migel) in it.flatten() {
            if !code.is_empty() {
                map.insert(code, migel);
            }
        }
    }
    map
}

/// Find the newest `udi_details_*.db` in the app-data db dir (by mtime).
fn find_latest_details_db(db_dir: &std::path::Path) -> Option<PathBuf> {
    let mut best: Option<(std::time::SystemTime, PathBuf)> = None;
    for entry in std::fs::read_dir(db_dir).ok()?.flatten() {
        let p = entry.path();
        let name = p.file_name()?.to_string_lossy().to_string();
        if name.starts_with("udi_details_") && name.ends_with(".db") {
            let mt = entry.metadata().ok().and_then(|m| m.modified().ok());
            if let Some(mt) = mt {
                if best.as_ref().map(|(t, _)| mt > *t).unwrap_or(true) {
                    best = Some((mt, p));
                }
            }
        }
    }
    best.map(|(_, p)| p)
}

/// Standalone `--triage`: classify the latest `udi_details` DB in place,
/// adding/refreshing the three triage columns. No download.
pub fn run() -> Result<(), Box<dyn std::error::Error>> {
    let db_dir = crate::app_data_dir().join("db");
    let db_path = find_latest_details_db(&db_dir).ok_or_else(|| {
        format!(
            "No udi_details_*.db found in {} — run --details first",
            db_dir.display()
        )
    })?;
    eprintln!("[triage] Classifying {}", db_path.display());

    let migel = load_migel_matches(&db_dir);
    eprintln!("[triage] {} MiGeL-matched udiDiCodes loaded (KLV Art. 20 lay-use signal)", migel.len());

    let mut conn = Connection::open(&db_path)?;

    // Add columns if absent (idempotent — a fresh --details run already has them).
    let existing: std::collections::HashSet<String> = conn
        .prepare("PRAGMA table_info(udi_details)")?
        .query_map([], |r| r.get::<_, String>(1))?
        .filter_map(|r| r.ok())
        .collect();
    for col in COLUMNS {
        if !existing.contains(*col) {
            conn.execute(
                &format!("ALTER TABLE udi_details ADD COLUMN \"{}\" TEXT", col),
                [],
            )?;
        }
    }

    // Load (rowid, udiDiCode, rawJson), classify, batch-update.
    let rows: Vec<(i64, String, String)> = {
        let mut stmt = conn.prepare("SELECT rowid, udiDiCode, rawJson FROM udi_details")?;
        let it = stmt.query_map([], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?))
        })?;
        it.filter_map(|r| r.ok()).collect()
    };
    let total = rows.len();
    eprintln!("[triage] {} rows to classify ...", total);

    let mut counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let tx = conn.transaction()?;
    {
        let mut up = tx.prepare(
            "UPDATE udi_details SET intendedUser=?, iuConfidence=?, iuReason=? WHERE rowid=?",
        )?;
        for (rowid, udi_di_code, raw) in &rows {
            let (user, conf, reason) = match serde_json::from_str::<Value>(raw) {
                Ok(v) => classify(&v, migel.get(udi_di_code).map(|s| s.as_str())),
                Err(_) => (
                    "review".to_string(),
                    "low".to_string(),
                    "unparseable rawJson".to_string(),
                ),
            };
            *counts.entry(format!("{}/{}", user, conf)).or_insert(0) += 1;
            up.execute(rusqlite::params![user, conf, reason, rowid])?;
        }
    }
    tx.commit()?;

    eprintln!("[triage] Done. Distribution (intendedUser/confidence):");
    let mut keys: Vec<_> = counts.keys().cloned().collect();
    keys.sort();
    for k in keys {
        eprintln!("  {:<22} {}", k, counts[&k]);
    }
    Ok(())
}
