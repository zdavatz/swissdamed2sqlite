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
/// Verified single-sided in the corpus (0% MiGeL lay-match for B/D/P/L/K/S).
/// Deliberately EXCLUDES F (dialysis — home peritoneal dialysis is patient-run).
const EMDN_PROFESSIONAL_CATS: &[char] = &['L', 'K', 'C', 'G', 'H', 'J', 'S', 'B', 'D', 'P'];

/// EMDN categories that lean public but are NOT auto-classified (orthoses are
/// often professionally fitted/prescribed; TENS spans both). Left as `review`
/// with a lean note so a human starts from the right prior.
///   Y technical aids for disabled persons (orthoses) · N neuro (TENS).
const EMDN_PUBLIC_LEAN_CATS: &[char] = &['Y', 'N'];

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
    let (ecode, eterm) = emdn_first(&udi);
    let ecat = ecode.chars().next().unwrap_or(' ');
    if EMDN_PROFESSIONAL_CATS.contains(&ecat) {
        return prof("low", format!("EMDN professional category {} ({})", ecat, eterm));
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
