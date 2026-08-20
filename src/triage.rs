//! Triage classifier: "Medizinprodukt für das Publikum" (public / layperson) vs
//! "für Fachanwender" (professional user).
//!
//! swissdamed/EUDAMED expose NO structured intended-user field for MDR/MDD (95%
//! of the corpus); the only structured signal is the IVD `selfTesting` /
//! `nearPatientTesting` / `professionalTesting` triad. This classifier is
//! therefore **decision-support / triage, not a compliance determination**: it
//! is deliberately asymmetric — it only ever labels `public` from a structured
//! signal (or an explicit manufacturer lay-use statement), because a false
//! `public` (selling a professional-only device) is a compliance breach, whereas
//! a false `professional` is only a lost sale. Everything without a reliable
//! signal is left as `review` and MUST be verified against the manufacturer's
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
///   J cardiac leads/programmers · S sterilisation equipment.
/// Deliberately EXCLUDES F (dialysis — home peritoneal dialysis is patient-run).
const EMDN_PROFESSIONAL_CATS: &[char] = &['L', 'K', 'C', 'G', 'H', 'J', 'S'];

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
/// Rule order matters — first match wins, most-reliable first. The `public`
/// branch is reached only via structured IVD self-testing or an explicit lay
/// statement (never inferred from risk class / EMDN, which the data proves do
/// NOT identify public devices).
pub fn classify(detail: &Value) -> (String, String, String) {
    let basic = detail.get("basicUdi").cloned().unwrap_or(Value::Null);
    let udi = detail.get("udiDi").cloned().unwrap_or(Value::Null);
    let dtype = sstr(detail, "deviceType");
    let risk = sstr(&basic, "riskClass");
    let is_ivd = dtype.starts_with("IVD");

    let prof = |c: &str, r: String| ("professional".to_string(), c.to_string(), r);
    let public = |c: &str, r: String| ("public".to_string(), c.to_string(), r);

    // TIER 1 — structured, high-confidence professional (never sold to public).
    if dtype == "AIMDD" {
        return prof("high", "active implantable device".into());
    }
    if is_true(&basic, "implantable") {
        return prof("high", "implantable".into());
    }
    if risk == "CLASS_III" || risk == "CLASS_D" {
        return prof("high", format!("high-risk {}", risk));
    }

    // TIER 2 — structured, high-confidence public (IVD self-testing).
    if is_true(&basic, "selfTesting") {
        return public("high", "IVD selfTesting flag".into());
    }
    if risk == "IVD_DEVICES_SELF_TESTING" {
        return public("high", "IVD self-testing risk class".into());
    }

    // TIER 3 — structured IVD professional signal (medium).
    if is_ivd && is_true(&basic, "professionalTesting") {
        return prof("medium", "IVD professionalTesting flag".into());
    }
    if is_ivd && is_true(&basic, "nearPatientTesting") {
        return prof("medium", "IVD nearPatientTesting flag".into());
    }

    // TIER 4 — explicit manufacturer text statements (medium).
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

    // TIER 5 — EMDN clearly-professional category (surgical/interventional).
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

    // Load (rowid, rawJson), classify, batch-update.
    let rows: Vec<(i64, String)> = {
        let mut stmt = conn.prepare("SELECT rowid, rawJson FROM udi_details")?;
        let it = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?;
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
        for (rowid, raw) in &rows {
            let (user, conf, reason) = match serde_json::from_str::<Value>(raw) {
                Ok(v) => classify(&v),
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
